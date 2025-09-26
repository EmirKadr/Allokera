# logic/allocation.py
from __future__ import annotations
from typing import Deque, Dict, List, Tuple
import pandas as pd
from collections import defaultdict, deque

from ..io.schemas import ORDER_SCHEMA, BUFFER_SCHEMA
from ..utils.common import find_col, smart_to_datetime, to_num
from ..config.constants import ALLOC_BUFFER_STATUSES, NEAR_MISS_PCT, INVALID_LOC_PREFIXES, INVALID_LOC_EXACT

def allocate(orders_raw: pd.DataFrame, buffer_raw: pd.DataFrame, log=None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    def _log(msg: str):
        if log: log(msg)

    # Kolumnupptäckt via schema
    order_article_col = find_col(orders_raw, ORDER_SCHEMA["artikel"])
    order_qty_col     = find_col(orders_raw, ORDER_SCHEMA["qty"])
    order_id_col      = find_col(orders_raw, ORDER_SCHEMA["ordid"], required=False, default=None)
    order_line_col    = find_col(orders_raw, ORDER_SCHEMA["radid"], required=False, default=None)
    order_status_col  = find_col(orders_raw, ORDER_SCHEMA["status"], required=False, default=None)

    buff_article_col  = find_col(buffer_raw, BUFFER_SCHEMA["artikel"])
    buff_qty_col      = find_col(buffer_raw, BUFFER_SCHEMA["qty"])
    buff_loc_col      = find_col(buffer_raw, BUFFER_SCHEMA["loc"])
    buff_dt_col       = find_col(buffer_raw, BUFFER_SCHEMA["dt"], required=False, default=None)
    buff_id_col       = find_col(buffer_raw, BUFFER_SCHEMA["id"], required=False, default=None)
    buff_status_col   = find_col(buffer_raw, BUFFER_SCHEMA["status"], required=False, default=None)

    _log(f"Order-kolumner: Artikel='{order_article_col}', Antal='{order_qty_col}', OrderId='{order_id_col}', Rad='{order_line_col}', Status='{order_status_col}'")
    _log(f"Buffert-kolumner: Artikel='{buff_article_col}', Antal='{buff_qty_col}', Lagerplats='{buff_loc_col}', Tid='{buff_dt_col}', ID='{buff_id_col}', Status='{buff_status_col}'")

    # Normalisera orders
    orders = orders_raw.copy()
    orders["_artikel"] = orders[order_article_col].astype(str).str.strip()
    orders["_qty"] = orders[order_qty_col].map(to_num).astype(float)
    orders["_order_id"] = orders[order_id_col].astype(str) if order_id_col and order_id_col in orders.columns else ""
    orders["_order_line"] = orders[order_line_col].astype(str) if order_line_col and order_line_col in orders.columns else orders.index.astype(str)

    # Ignorera Status=35
    if order_status_col and order_status_col in orders.columns:
        _status_str = orders[order_status_col].astype(str).str.strip()
        _status_num = pd.to_numeric(_status_str.str.extract(r"(-?\d+)")[0], errors="coerce")
        _before = len(orders)
        orders = orders[~(_status_num == 35)].copy()
        _removed = _before - len(orders)
        if _removed:
            _log(f"Ignorerar {_removed} orderrad(er) pga Status = 35.")
    else:
        _log("OBS: Ingen order-statuskolumn hittad; kan inte filtrera Status = 35.")

    # Normalisera buffert
    buffer_df = buffer_raw.copy()
    buffer_df["_artikel"] = buffer_df[buff_article_col].astype(str).str.strip()
    buffer_df["_qty"] = buffer_df[buff_qty_col].map(to_num).astype(float)
    buffer_df["_loc"] = buffer_df[buff_loc_col].astype(str).str.strip()
    buffer_df["_received"] = smart_to_datetime(buffer_df[buff_dt_col]) if buff_dt_col and buff_dt_col in buffer_df.columns else pd.NaT
    buffer_df["_source_id"] = buffer_df[buff_id_col].astype(str) if buff_id_col and buff_id_col in buffer_df.columns else "SRC-" + buffer_df.index.astype(str)

    # Statusfilter (29/30/32)
    if buff_status_col and buff_status_col in buffer_df.columns:
        status_series = buffer_df[buff_status_col].astype(str).str.strip()
        status_num = pd.to_numeric(status_series.str.extract(r"(-?\d+)")[0], errors="coerce")
        allowed_str = {str(x) for x in ALLOC_BUFFER_STATUSES}
        mask_allowed = status_series.isin(allowed_str) | status_num.isin(ALLOC_BUFFER_STATUSES)
        removed = int((~mask_allowed).sum())
        if removed:
            _log(f"Filtrerar bort {removed} buffertpall(ar) pga Status ej i {sorted(ALLOC_BUFFER_STATUSES)}.")
        buffer_df = buffer_df[mask_allowed].copy()
    else:
        _log("OBS: Hittade ingen statuskolumn; ingen statusfiltrering tillämpas.")

    # Platsfilter
    loc_upper = buffer_df["_loc"].str.upper()
    mask_exclude = loc_upper.str.startswith(INVALID_LOC_PREFIXES, na=False) | loc_upper.isin(INVALID_LOC_EXACT)
    excluded_count = int(mask_exclude.sum())
    if excluded_count:
        _log(f"Filtrerar bort {excluded_count} rad(er) från bufferten pga lagerplats-regler ({INVALID_LOC_PREFIXES}*, {', '.join(sorted(INVALID_LOC_EXACT))}).")
    buffer_df = buffer_df[~mask_exclude].copy()

    # Liten minnesopt
    try: buffer_df["_artikel"] = buffer_df["_artikel"].astype("category")
    except Exception: pass

    buffer_df["_is_autostore"] = buffer_df["_loc"].str.contains("AUTOSTORE", case=False, na=False)
    buffer_df = buffer_df[buffer_df["_qty"] > 0].copy()

    far_future = pd.Timestamp("2262-04-11")
    buffer_df["_received_ord"] = buffer_df["_received"].fillna(far_future)

    pallets = buffer_df[~buffer_df["_is_autostore"]].copy().sort_values(by=["_artikel", "_received_ord", "_source_id"])
    bins = buffer_df[buffer_df["_is_autostore"]].copy().sort_values(by=["_artikel", "_received_ord", "_source_id"])

    pallet_queues: Dict[str, Deque[dict]] = defaultdict(deque)
    for _, r in pallets.iterrows():
        pallet_queues[str(r["_artikel"]).strip()].append({"source_id": r["_source_id"], "qty": float(r["_qty"]), "loc": r["_loc"], "received": r["_received"]})

    bin_queues: Dict[str, Deque[dict]] = defaultdict(deque)
    for _, r in bins.iterrows():
        bin_queues[str(r["_artikel"]).strip()].append({"source_id": r["_source_id"], "qty": float(r["_qty"]), "loc": r["_loc"], "received": r["_received"]})

    allocated_rows: List[dict] = []
    near_miss_rows: List[dict] = []

    def clone_row(orow: pd.Series) -> dict:
        return orow.to_dict()

    def record_near_miss(orow: pd.Series, pal: dict, need: float) -> None:
        if need <= 0: return
        diff = pal["qty"] - need
        if diff <= 0: return
        pct = diff / need
        if pct <= NEAR_MISS_PCT:
            near_miss_rows.append({
                "Artikel": str(orow["_artikel"]),
                "OrderID": str(orow["_order_id"]),
                "OrderRad": str(orow["_order_line"]),
                "PallID": str(pal["source_id"]),
                "Källplats": str(pal["loc"]),
                "Mottagen": pal["received"],
                "Behov_vid_tillfället": need,
                "Pall_kvantitet": pal["qty"],
                "Skillnad": diff,
                "Procentuell skillnad (%)": pct * 100.0,
                "Anledning": "Pallen var ≤15% större än återstående behov (kan ej brytas)"
            })

    for _, orow in orders.iterrows():
        art = str(orow["_artikel"]).strip()
        need = float(orow["_qty"])
        if need <= 0: continue

        # 1) HELPALL
        pq = pallet_queues.get(art, deque())
        new_pq = deque()
        tmp = deque(pq)
        any_helpall = False
        while tmp and need > 0:
            pal = tmp.popleft()
            pal_qty = pal["qty"]
            if pal_qty <= need:
                sub = clone_row(orow)
                sub[order_qty_col] = pal_qty
                sub["Zon (beräknad)"] = "H"
                sub["Källtyp"] = "HELPALL"
                sub["Källa"] = pal["source_id"]
                sub["Källplats"] = pal["loc"]
                allocated_rows.append(sub)
                need -= pal_qty
                any_helpall = True
            else:
                record_near_miss(orow, pal, need)
                new_pq.append(pal)
        while tmp: new_pq.append(tmp.popleft())
        pallet_queues[art] = new_pq

        # 2) AUTOSTORE
        any_autostore = False
        bq = bin_queues.get(art, deque())
        new_bq = deque()
        while bq and need > 0:
            binr = bq.popleft()
            take = min(binr["qty"], need)
            if take > 0:
                sub = clone_row(orow)
                sub[order_qty_col] = take
                sub["Zon (beräknad)"] = "R"
                sub["Källtyp"] = "AUTOSTORE"
                sub["Källa"] = binr["source_id"]
                sub["Källplats"] = binr["loc"]
                allocated_rows.append(sub)
                binr["qty"] -= take
                need -= take
                any_autostore = True
            if binr["qty"] > 0: new_bq.append(binr)
        while bq: new_bq.append(bq.popleft())
        bin_queues[art] = new_bq

        # 3) HUVUDPLOCK
        any_mainpick = False
        if need > 0:
            sub = clone_row(orow)
            sub[order_qty_col] = need
            sub["Zon (beräknad)"] = "A"
            sub["Källtyp"] = "HUVUDPLOCK"
            sub["Källa"] = ""
            sub["Källplats"] = ""
            allocated_rows.append(sub)
            any_mainpick = True
            need = 0.0

        # Near-miss markering
        if not any_helpall and (any_autostore or any_mainpick):
            for r in near_miss_rows:
                if r["OrderID"] == str(orow["_order_id"]) and r["OrderRad"] == str(orow["_order_line"]):
                    r["Gäller (INSTEAD R/A)"] = True
        else:
            for r in near_miss_rows:
                if r["OrderID"] == str(orow["_order_id"]) and r["OrderRad"] == str(orow["_order_line"]):
                    r["Gäller (INSTEAD R/A)"] = False

    allocated_df = pd.DataFrame(allocated_rows)

    # Om en artikel har AUTOSTORE-rad → gör alla dess icke-HELPALL till AUTOSTORE
    try:
        if not allocated_df.empty and ("Källtyp" in allocated_df.columns):
            if "Zon (beräknad)" not in allocated_df.columns:
                allocated_df["Zon (beräknad)"] = ""
            low = {c.lower(): c for c in allocated_df.columns}
            art_col_res = None
            for n in ["artikel", "article", "artnr", "art.nr", "artikelnummer", "_artikel"]:
                if n.lower() in low:
                    art_col_res = low[n.lower()]; break
            if art_col_res:
                auto_arts = set(allocated_df.loc[allocated_df["Källtyp"].astype(str) == "AUTOSTORE", art_col_res].astype(str).str.strip())
                if auto_arts:
                    mask_same = allocated_df[art_col_res].astype(str).str.strip().isin(auto_arts)
                    mask_change = mask_same & (allocated_df["Källtyp"].astype(str) != "HELPALL")
                    allocated_df.loc[mask_change, "Källtyp"] = "AUTOSTORE"
                    allocated_df.loc[mask_change, "Zon (beräknad)"] = "R"
    except Exception:
        pass

    added_cols = ["Zon (beräknad)", "Källtyp", "Källa", "Källplats"]
    ordered_cols = [c for c in orders_raw.columns] + [c for c in added_cols if c not in orders_raw.columns]
    if not allocated_df.empty:
        allocated_df = allocated_df[ordered_cols]
    else:
        allocated_df = pd.DataFrame(columns=ordered_cols)

    near_miss_df = pd.DataFrame(near_miss_rows)
    return allocated_df, near_miss_df
