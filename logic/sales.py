# logic/sales.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Optional
from ..io.excel_export import open_sales_excel


# ---------------------------
# Hjälpfunktioner (snabba, robusta)
# ---------------------------

def _to_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.normalize()

def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _first_nonempty(s: pd.Series) -> str:
    for x in s:
        if isinstance(x, str) and x.strip():
            return x.strip()
    return ""

def _detect_col(df: pd.DataFrame, candidates) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        lc = cand.lower()
        if lc in cols:
            return cols[lc]
    # fuzzy: contains
    for name in df.columns:
        low = name.lower()
        if any(lc in low for lc in [c.lower() for c in candidates]):
            return name
    return None

def _prep_plocklogg(df_norm: pd.DataFrame) -> pd.DataFrame:
    """Förväntar sig minst: Artikelnummer, Plockat, Datum.
       Försöker hämta Zon från kolumn 'Zon' eller 'Plockplats' i loggen om de finns."""
    df = df_norm.copy()
    # Artikelnr
    if "Artikelnummer" not in df.columns:
        raise ValueError("Plocklogg saknar kolumn 'Artikelnummer' efter normalisering.")
    df["Artikelnummer"] = df["Artikelnummer"].astype(str).str.strip()

    # Datum
    dt_col = _detect_col(df, ["Datum", "Datum/tid", "Date"])
    if not dt_col:
        raise ValueError("Plocklogg saknar datumkolumn (t.ex. 'Datum').")
    df["DatumNorm"] = _to_date(df[dt_col])

    # Plockat
    qty_col = _detect_col(df, ["Plockat", "Antal", "Quantity", "Qty"])
    if not qty_col:
        raise ValueError("Plocklogg saknar kolumn för antal (t.ex. 'Plockat').")
    df["Plockat"] = _to_num(df[qty_col]).fillna(0.0)

    # Zon (om möjligt)
    zon_col = _detect_col(df, ["Zon"])
    if zon_col:
        df["Zon"] = df[zon_col].astype(str).str.strip().str.upper().str[0]
    else:
        # Prova avleda zon från en plockplats-kolumn i loggen
        place_col = _detect_col(df, ["Plockplats", "Lagerplats", "Plats"])
        if place_col:
            df["Zon"] = df[place_col].astype(str).str.strip().str.upper().str[0]
        else:
            df["Zon"] = pd.Series([None] * len(df), dtype="object")

    # Rensa bort rader utan giltigt datum
    df = df[~df["DatumNorm"].isna()].copy()
    return df

def _build_daily(df: pd.DataFrame, mask: Optional[pd.Series] = None) -> pd.DataFrame:
    """Aggregerar till per-dag per artikel (summa plockat)."""
    sub = df[mask].copy() if mask is not None else df.copy()
    if sub.empty:
        return pd.DataFrame(columns=["Artikelnummer", "DatumNorm", "DagSum"])
    grp = (sub.groupby(["Artikelnummer", "DatumNorm"], as_index=False)["Plockat"]
               .sum()
               .rename(columns={"Plockat": "DagSum"}))
    return grp

def _days_and_avg(daily_df: pd.DataFrame) -> pd.DataFrame:
    """Räknar antal dagar (med DagSum>0) och snitt per plockdag."""
    if daily_df.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Dagar", "SnittPerDag"])
    # räkna bara dagar där något faktiskt plockats (>0)
    d = daily_df[daily_df["DagSum"] > 0].copy()
    if d.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Dagar", "SnittPerDag"])
    days = d.groupby("Artikelnummer")["DatumNorm"].nunique().rename("Dagar")
    avg = d.groupby("Artikelnummer")["DagSum"].mean().rename("SnittPerDag")
    out = pd.concat([days, avg], axis=1).reset_index()
    return out

def _prep_saldo(saldo_norm: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Förväntar: Artikel (artikelnummer), Plockplats (text)."""
    if saldo_norm is None or saldo_norm.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Plockplats"])
    s = saldo_norm.copy()
    # I normaliserad saldo använder du oftast 'Artikel' som artikelnummer → döp om.
    if "Artikelnummer" not in s.columns and "Artikel" in s.columns:
        s = s.rename(columns={"Artikel": "Artikelnummer"})
    s["Artikelnummer"] = s["Artikelnummer"].astype(str).str.strip()
    if "Plockplats" in s.columns:
        s["Plockplats"] = s["Plockplats"].astype(str).fillna("").str.strip()
    else:
        s["Plockplats"] = ""
    # välj en plockplats per artikel: första icke-tomma
    agg = (s.groupby("Artikelnummer", as_index=False)
             .agg({"Plockplats": _first_nonempty}))
    return agg

def _prep_buffer(buffer_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Förväntar: artikelkolumn + 'Antal' på buffertpallar. Returnerar medel 'Antal per pall' per artikel
       efter att ha filtrerat bort extremt små outliers (<50% av medianen)."""
    if buffer_df is None or buffer_df.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Antal per pall"])

    b = buffer_df.copy()
    art_col = _detect_col(b, ["Artikelnummer", "Artikel", "Art.nr", "Artikelnr"])
    qty_col = _detect_col(b, ["Antal", "Quantity", "Qty", "Kolli"])
    if not art_col or not qty_col:
        return pd.DataFrame(columns=["Artikelnummer", "Antal per pall"])

    b["Artikelnummer"] = b[art_col].astype(str).str.strip()
    b["AntalRaw"] = _to_num(b[qty_col])

    def _robust_mean(vals: pd.Series) -> float:
        vals = vals.dropna()
        vals = vals[vals > 0]
        if len(vals) == 0:
            return np.nan
        med = float(vals.median())
        if med <= 0:
            return float(vals.mean())
        # ta bort extremt små outliers (< 50% av medianen)
        filt = vals[vals >= 0.5 * med]
        if len(filt) == 0:
            filt = vals
        return float(filt.mean())

    agg = (b.groupby("Artikelnummer")["AntalRaw"]
             .apply(_robust_mean)
             .rename("Antal per pall")
             .reset_index())
    return agg


# ---------------------------
# Huvud-API (behåll namnen så GUI kan anropa som tidigare)
# ---------------------------

def compute_sales_metrics(
    df_norm: pd.DataFrame,
    today=None,
    saldo_norm: Optional[pd.DataFrame] = None,
    buffer_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """
    "Metrics"-bas som open_sales_insights använder för att bygga flikarna.
    Gör MINIMAL beräkning:
      - Antal dagar i plock (alla zoner)
      - Snitt per plockdag (alla zoner)
      - Zon-sättning per artikel (för 'Endast EH i Plock')
      - Antal E-dagar + E-snitt
      - Plockplats (från saldo)
      - Antal per pall (från buffertpallar)
    """
    df = _prep_plocklogg(df_norm)

    # Per dag per artikel (alla zoner)
    daily_all = _build_daily(df)
    any_stats = _days_and_avg(daily_all).rename(
        columns={"Dagar": "Antal dagar i plock",
                 "SnittPerDag": "Snitt beställt per plockdag"}
    )

    # Zoner per artikel (för att veta "Endast E" resp "Endast E & H")
    # Om 'Zon' saknas blir set tomt → hamnar inte i EH-fliken
    zmap = (df.dropna(subset=["Zon"])
              .groupby("Artikelnummer")["Zon"]
              .apply(lambda s: "".join(sorted(set([str(x)[:1] for x in s if isinstance(x, str) and x]))))
              .rename("ZonSet")
              .reset_index())

    # Endast E-data (för E-dagar och E-snitt)
    if "Zon" in df.columns and df["Zon"].notna().any():
        daily_e = _build_daily(df, mask=(df["Zon"] == "E"))
        e_stats = _days_and_avg(daily_e).rename(
            columns={"Dagar": "Antal dagar i plock (Endast E-zon)",
                     "SnittPerDag": "Snitt beställt per plockdag (Endast E-zon)"}
        )
    else:
        e_stats = pd.DataFrame(columns=["Artikelnummer",
                                        "Antal dagar i plock (Endast E-zon)",
                                        "Snitt beställt per plockdag (Endast E-zon)"])

    # Slå ihop bas
    base = any_stats.merge(zmap, on="Artikelnummer", how="left") \
                    .merge(e_stats, on="Artikelnummer", how="left")

    # Plockplats från saldo
    saldo_m = _prep_saldo(saldo_norm)
    base = base.merge(saldo_m, on="Artikelnummer", how="left")

    # Antal per pall från buffert
    buff_m = _prep_buffer(buffer_df)
    base = base.merge(buff_m, on="Artikelnummer", how="left")

    # Rensa typer/format
    for col in ["Antal dagar i plock",
                "Antal dagar i plock (Endast E-zon)"]:
        if col in base.columns:
            base[col] = _to_num(base[col]).astype("Int64")

    for col in ["Snitt beställt per plockdag",
                "Snitt beställt per plockdag (Endast E-zon)",
                "Antal per pall"]:
        if col in base.columns:
            base[col] = _to_num(base[col]).round(2)

    # Se till att viktiga kolumner finns
    for col in ["Plockplats", "Antal per pall",
                "Antal dagar i plock",
                "Snitt beställt per plockdag",
                "Antal dagar i plock (Endast E-zon)",
                "Snitt beställt per plockdag (Endast E-zon)"]:
        if col not in base.columns:
            base[col] = pd.NA

    # Om Artikel (namn) finns i df_norm: lägg till (kan vara bra vid behov)
    if "Artikel" in getattr(df_norm, "columns", []):
        names = df_norm[["Artikelnummer", "Artikel"]].drop_duplicates()
        base = base.merge(names, on="Artikelnummer", how="left")

    # Kolumnordning för intern bas
    ordered = ["Artikelnummer", "Artikel", "Plockplats",
               "Antal dagar i plock", "Snitt beställt per plockdag",
               "Antal per pall",
               "Antal dagar i plock (Endast E-zon)",
               "Snitt beställt per plockdag (Endast E-zon)",
               "ZonSet"]
    cols = [c for c in ordered if c in base.columns] + [c for c in base.columns if c not in ordered]
    base = base[cols]

    return base


def open_sales_insights(metrics: pd.DataFrame) -> str:
    """
    Bygger exakt två flikar enligt önskemål och öppnar i Excel:
      - 'Rekomenderade buffertuppdateringar'
      - 'Endast EH i Plock'
    """
    if metrics is None or metrics.empty:
        raise RuntimeError("Inga data att visa.")

    df = metrics.copy()

    # -------- Flik 1: Rekomenderade buffertuppdateringar --------
    # Plockplats slutar på "1"
    if "Plockplats" in df.columns:
        mask_1 = df["Plockplats"].astype(str).str.endswith("1")
    else:
        mask_1 = pd.Series([False] * len(df))

    rec = (df[mask_1]
             .loc[:, ["Artikelnummer",
                      "Plockplats",
                      "Antal dagar i plock",
                      "Snitt beställt per plockdag",
                      "Antal per pall"]]
             .sort_values(by=["Antal dagar i plock", "Snitt beställt per plockdag"],
                          ascending=[True, True])
             .reset_index(drop=True))

    # -------- Flik 2: Endast EH i Plock --------
    # Behöver ZonSet (skapad från plocklogg). 'E' → bara E. 'EH' → E & H. Annat → exkluderas.
    # (Om ZonSet saknas → tom flik)
    if "ZonSet" in df.columns:
        only_e  = df["ZonSet"].fillna("").str.fullmatch(r"E", na=False)
        only_eh = df["ZonSet"].fillna("").str.fullmatch(r"EH|HE", na=False)
    else:
        only_e = pd.Series([False] * len(df))
        only_eh = pd.Series([False] * len(df))

    ehe = pd.concat([
        df[only_e ].assign(Kategori="Endast E"),
        df[only_eh].assign(Kategori="Endast E & H"),
    ], ignore_index=True)

    # Välj kolumner & sortera
    ehe_cols = ["Artikelnummer",
                "Plockplats",
                "Antal dagar i plock (Endast E-zon)",
                "Snitt beställt per plockdag (Endast E-zon)",
                "Antal per pall",
                "Kategori"]
    # Säkerställ att kolumner finns
    for c in ehe_cols:
        if c not in ehe.columns:
            ehe[c] = pd.NA

    ehe = (ehe[ehe_cols]
             .sort_values(by=["Antal dagar i plock (Endast E-zon)",
                              "Snitt beställt per plockdag (Endast E-zon)"],
                          ascending=[False, False])
             .reset_index(drop=True))

    # Bygg xlsx
    sheets: Dict[str, pd.DataFrame] = {
        "Rekomenderade buffertuppdateringar": rec,
        "Endast EH i Plock": ehe,
    }
    return open_sales_excel(sheets, label="sales_insights_fast")
