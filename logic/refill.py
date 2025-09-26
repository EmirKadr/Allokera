# logic/refill.py
from __future__ import annotations
from typing import Dict, List, Tuple
import pandas as pd
import numpy as np

from ..config.constants import REFILL_BUFFER_STATUSES
from ..io.schemas import ORDER_SCHEMA, BUFFER_SCHEMA, NOT_PUTAWAY_SCHEMA
from ..io.file_readers import normalize_saldo, normalize_not_putaway
from ..utils.common import find_col, smart_to_datetime, to_num


def annotate_refill(refill_df: pd.DataFrame, df_metrics: pd.DataFrame) -> pd.DataFrame:
    if refill_df is None or refill_df.empty or df_metrics is None or df_metrics.empty:
        return refill_df
    cols = ["Artikel", "ADV_90", "ABC_klass", "DagarSedanSenast", "UnikaPlockdagar_90", "NollraderPerPlockdag_90"]
    cols = [c for c in cols if c in df_metrics.columns or c == "Artikel"]
    out = refill_df.merge(df_metrics[cols], on="Artikel", how="left")
    return out

def _reclassify_skrymmande(result_df: pd.DataFrame, saldo_norm: pd.DataFrame | None) -> pd.DataFrame:
    if result_df is None or result_df.empty or saldo_norm is None or saldo_norm.empty:
        return result_df
    res = result_df.copy()
    art_col = find_col(res, ORDER_SCHEMA["artikel"], required=True)
    pp_map: Dict[str, str] = {}
    for _, r in saldo_norm.iterrows():
        a = str(r.get("Artikel", "")).strip()
        p = str(r.get("Plockplats", "") or "").strip()
        if a and p and a not in pp_map:
            pp_map[a] = p
    if not pp_map:
        return res
    ktyp_series = res.get("Källtyp", pd.Series("", index=res.index)).astype(str)
    kalla_blank = res.get("Källa", pd.Series("", index=res.index))
    mask_blank = kalla_blank.isna() | (kalla_blank.astype(str).str.strip() == "")
    mask_ktyp = ktyp_series.isin(["HUVUDPLOCK", "AUTOSTORE"])
    mask = mask_blank & mask_ktyp
    if not mask.any():
        return res
    arts = res.loc[mask, art_col].astype(str).str.strip()
    pp = arts.map(lambda a: pp_map.get(a, ""))
    pp_up = pp.str.upper()
    cond = pp_up.str.startswith("SK") | pp_up.str.contains("BRAND", na=False)
    idx = res.loc[mask].index[cond.fillna(False)]
    if len(idx) > 0:
        res.loc[idx, "Källtyp"] = "SKRYMMANDE"
        if "Zon (beräknad)" not in res.columns:
            res["Zon (beräknad)"] = ""
        res.loc[idx, "Zon (beräknad)"] = "S"
    return res

def calculate_refill(allocated_df: pd.DataFrame,
                     buffer_raw: pd.DataFrame,
                     saldo_df: pd.DataFrame | None = None,
                     not_putaway_df: pd.DataFrame | None = None
                     ) -> Tuple[pd.DataFrame, pd.DataFrame]:

    result = allocated_df.copy()
    buff = buffer_raw.copy()

    art_col_res = find_col(result, ORDER_SCHEMA["artikel"])
    qty_col_res = find_col(result, ORDER_SCHEMA["qty"])

    art_col_buf = find_col(buff, BUFFER_SCHEMA["artikel"])
    qty_col_buf = find_col(buff, BUFFER_SCHEMA["qty"])
    dt_col_buf  = find_col(buff, BUFFER_SCHEMA["dt"], required=False, default=None)
    id_col_buf  = find_col(buff, BUFFER_SCHEMA["id"], required=False, default=None)
    status_col_buf = find_col(buff, BUFFER_SCHEMA["status"], required=False, default=None)

    b = buff.copy()
    b["_artikel"] = b[art_col_buf].astype(str).str.strip()
    b["_qty"] = b[qty_col_buf].map(to_num).astype(float)
    b["_received"] = smart_to_datetime(b[dt_col_buf]) if dt_col_buf and dt_col_buf in b.columns else pd.NaT
    b["_source_id"] = b[id_col_buf].astype(str) if id_col_buf and id_col_buf in b.columns else "SRC-" + b.index.astype(str)

    if status_col_buf and status_col_buf in b.columns:
        _s = b[status_col_buf].astype(str).str.strip()
        _snum = pd.to_numeric(_s.str.extract(r"(-?\d+)")[0], errors="coerce")
        allowed_str = {str(x) for x in REFILL_BUFFER_STATUSES}
        b = b[_s.isin(allowed_str) | _snum.isin(REFILL_BUFFER_STATUSES)].copy()

    used_help_ids: set[str] = set()
    if "Källtyp" in result.columns and "Källa" in result.columns:
        used_help_ids = set(result[result["Källtyp"].astype(str) == "HELPALL"]["Källa"].dropna().astype(str).tolist())

    saldo_sum: Dict[str, float] = {}
    plockplats_by_art: Dict[str, str] = {}
    if isinstance(saldo_df, pd.DataFrame) and not saldo_df.empty:
        try:
            s_norm = normalize_saldo(saldo_df)
            for _, r in s_norm.iterrows():
                art = str(r["Artikel"]).strip()
                saldo_sum[art] = float(saldo_sum.get(art, 0.0) + float(r.get("Plocksaldo", 0.0)))
                pp = str(r.get("Plockplats", "") or "").strip()
                if pp and art not in plockplats_by_art:
                    plockplats_by_art[art] = pp
        except Exception:
            saldo_sum = {}
            plockplats_by_art = {}

    npu_sum: Dict[str, float] = {}
    if isinstance(not_putaway_df, pd.DataFrame) and not not_putaway_df.empty:
        try:
            npu = not_putaway_df.copy()
            npu_art_col = find_col(npu, NOT_PUTAWAY_SCHEMA["artikel"])
            npu_qty_col = find_col(npu, NOT_PUTAWAY_SCHEMA["antal"])
            grp = npu.groupby(npu[npu_art_col].astype(str).str.strip())[npu_qty_col] \
                     .apply(lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum()))
            npu_sum = {str(k): float(v) for k, v in grp.to_dict().items()}
        except Exception:
            npu_sum = {}

    def fifo_for_art(art_key: str) -> pd.DataFrame:
        d = b[b["_artikel"] == art_key].copy()
        if not d.empty and used_help_ids:
            d = d[~d["_source_id"].astype(str).isin(used_help_ids)].copy()
        return d.sort_values("_received")

    hp_like = result[result.get("Källtyp", "").isin(["HUVUDPLOCK", "SKRYMMANDE"])].copy()
    rows_hp: List[dict] = []
    if not hp_like.empty:
        hp_like["_zon"] = np.where(hp_like["Källtyp"].astype(str) == "SKRYMMANDE", "S", "A")
        needs = (hp_like
                 .assign(_art=hp_like[art_col_res].astype(str).str.strip(),
                         _qty=pd.to_numeric(hp_like[qty_col_res], errors="coerce").fillna(0.0))
                 .groupby(["_art", "_zon"], as_index=False)["_qty"].sum())

        for art_key, grp_art in needs.groupby("_art"):
            total_need = float(grp_art["_qty"].sum())
            if total_need <= 0: continue
            adjusted_total = max(0.0, round(total_need) - float(saldo_sum.get(art_key, 0.0)))
            if adjusted_total <= 0: continue

            parts = []
            allocated_sum = 0
            for _, r in grp_art.iterrows():
                zone = str(r["_zon"])
                part = (float(r["_qty"]) / total_need) * adjusted_total if total_need > 0 else 0.0
                val = int(round(part))
                parts.append([zone, val]); allocated_sum += val
            diff = int(adjusted_total) - int(allocated_sum)
            if parts: parts[0][1] += diff

            fifo_df = fifo_for_art(art_key)
            tillgangligt = float(pd.to_numeric(fifo_df["_qty"], errors="coerce").sum()) if not fifo_df.empty else 0.0

            for zone, behov_int in parts:
                behov_int = int(max(0, behov_int))
                if behov_int <= 0: continue
                behov_kvar = float(behov_int); pall_count = 0
                for q in (fifo_df["_qty"].astype(float) if not fifo_df.empty else []):
                    if behov_kvar <= 0: break
                    pall_count += 1; behov_kvar -= float(q)

                rows_hp.append({
                    "Artikel": art_key,
                    "Zon": zone,
                    "Behov (kolli)": behov_int,
                    "FIFO-baserad beräkning": int(pall_count),
                    "Tillräckligt tillgängligt saldo i buffert": "Ja" if tillgangligt >= behov_int else "Nej",
                    "Plockplats": plockplats_by_art.get(art_key, ""),
                    "Ej inlagrade (antal)": int(round(npu_sum.get(art_key, 0.0)))
                })

    refill_hp_df = pd.DataFrame(rows_hp)
    if not refill_hp_df.empty:
        refill_hp_df = refill_hp_df.sort_values(["Zon", "FIFO-baserad beräkning"], ascending=[True, False])

    # --- AUTOSTORE (R) ---
    refill_autostore_df = pd.DataFrame()
    try:
        as_df = result.copy()
        if not as_df.empty:
            mask_autostore = as_df["Källtyp"].astype(str) == "AUTOSTORE" if "Källtyp" in as_df.columns else pd.Series(False, index=as_df.index)
            k_blank = as_df["Källa"].isna() | (as_df["Källa"].astype(str).str.strip() == "") if "Källa" in as_df.columns else pd.Series(True, index=as_df.index)
            as_df = as_df[mask_autostore & k_blank].copy()
        if not as_df.empty:
            art_col_res_as = find_col(as_df, ORDER_SCHEMA["artikel"])
            qty_col_res_as = find_col(as_df, ORDER_SCHEMA["qty"])
            behov_per_art_as = as_df.groupby(as_df[art_col_res_as].astype(str).str.strip())[qty_col_res_as] \
                                   .apply(lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())) \
                                   .to_dict()

            rows_as: List[dict] = []
            for art, behov in behov_per_art_as.items():
                art_key = str(art).strip()
                fifo_df = fifo_for_art(art_key)
                tillgangligt = float(pd.to_numeric(fifo_df["_qty"], errors="coerce").sum()) if not fifo_df.empty else 0.0
                behov_int = int(max(0, round(behov) - float(saldo_sum.get(art_key, 0.0))))
                if behov_int <= 0: continue
                remaining = float(behov_int); pall_count = 0
                for q in (fifo_df["_qty"].astype(float) if not fifo_df.empty else []):
                    if remaining <= 0: break
                    pall_count += 1; remaining -= float(q)

                rows_as.append({
                    "Artikel": art_key,
                    "Behov (kolli)": behov_int,
                    "FIFO-baserad beräkning": int(pall_count),
                    "Tillräckligt tillgängligt saldo i buffert": "Ja" if tillgangligt >= behov_int else "Nej",
                    "Plockplats": plockplats_by_art.get(art_key, ""),
                    "Ej inlagrade (antal)": int(round(npu_sum.get(art_key, 0.0)))
                })

            refill_autostore_df = pd.DataFrame(rows_as)
            if not refill_autostore_df.empty:
                refill_autostore_df = refill_autostore_df.sort_values("FIFO-baserad beräkning", ascending=False)
    except Exception:
        refill_autostore_df = pd.DataFrame()

    return refill_hp_df, refill_autostore_df
