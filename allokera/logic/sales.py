# logic/sales.py
from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Optional
from ..io.excel_export import open_sales_excel


# ---------------------------
# Hjälpfunktioner
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
    """Sök kolumn via exakta namn (case-insensitivt) och därefter 'contains'."""
    if df is None or df.empty:
        return None
    # exakta (case-insensitiv)
    lowmap = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lowmap:
            return lowmap[cand.lower()]
    # contains
    lcands = [c.lower() for c in candidates]
    for col in df.columns:
        low = col.lower()
        if any(c in low for c in lcands):
            return col
    return None

def _prep_plocklogg(df_norm: pd.DataFrame) -> pd.DataFrame:
    """Förväntar: Artikelnummer, datum, plockat. Hämtar/deriverar Zon om möjligt."""
    if df_norm is None or df_norm.empty:
        raise ValueError("Plocklogg är tom.")
    df = df_norm.copy()

    # Artikelnr
    if "Artikelnummer" not in df.columns:
        raise ValueError("Plocklogg saknar kolumn 'Artikelnummer' efter normalisering.")
    df["Artikelnummer"] = df["Artikelnummer"].astype(str).str.strip()

    # Datum
    dt_col = _detect_col(df, ["Datum", "Datum/tid", "Date", "Tidpunkt"])
    if not dt_col:
        raise ValueError("Plocklogg saknar datumkolumn (t.ex. 'Datum').")
    df["DatumNorm"] = _to_date(df[dt_col])

    # Plockat (antal)
    qty_col = _detect_col(df, ["Plockat", "Antal", "Quantity", "Qty"])
    if not qty_col:
        raise ValueError("Plocklogg saknar kolumn för antal (t.ex. 'Plockat'/'Antal').")
    df["Plockat"] = _to_num(df[qty_col]).fillna(0.0)

    # --- ZON ---
    # 1) Direkt zon-kolumn
    zon_col = _detect_col(df, [
        "Zon", "Lagerzon", "Zon (beräknad)", "Zon (Beräknad)",
        "Plockzon", "PickZone", "Zone"
    ])
    if zon_col:
        z = df[zon_col].astype(str).str.strip().str.upper()
        df["Zon"] = z.str[0]  # första bokstaven räcker (E, H, ...)
    else:
        # 2) Försök härleda från plats-kolumn (plockplats/lagerplats/…)
        place_col = _detect_col(df, [
            "Plockplats", "Lagerplats", "Plats", "Location", "Lagerlokation",
            "PickLocation", "LagPlats", "Lokation", "Loc"
        ])
        if place_col:
            place = df[place_col].astype(str).str.strip().str.upper()
            # Ta första bokstav a–z om den finns, annars NaN
            # (t.ex. 'E12-03' → 'E')
            z_guess = place.str.extract(r'^\s*([A-ZÅÄÖ])', expand=False)
            df["Zon"] = z_guess.str.replace("Å","A").str.replace("Ä","A").str.replace("Ö","O")
        else:
            df["Zon"] = pd.NA

    # Rensa rader utan giltigt datum
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
    d = daily_df[daily_df["DagSum"] > 0].copy()
    if d.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Dagar", "SnittPerDag"])
    days = d.groupby("Artikelnummer")["DatumNorm"].nunique().rename("Dagar")
    avg  = d.groupby("Artikelnummer")["DagSum"].mean().rename("SnittPerDag")
    out = pd.concat([days, avg], axis=1).reset_index()
    return out

def _prep_saldo(saldo_norm: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Saldo för plockplats per artikel."""
    if saldo_norm is None or saldo_norm.empty:
        return pd.DataFrame(columns=["Artikelnummer", "Plockplats"])
    s = saldo_norm.copy()
    if "Artikelnummer" not in s.columns and "Artikel" in s.columns:
        s = s.rename(columns={"Artikel": "Artikelnummer"})
    s["Artikelnummer"] = s["Artikelnummer"].astype(str).str.strip()
    if "Plockplats" in s.columns:
        s["Plockplats"] = s["Plockplats"].astype(str).fillna("").str.strip()
    else:
        s["Plockplats"] = ""
    agg = (s.groupby("Artikelnummer", as_index=False)
             .agg({"Plockplats": _first_nonempty}))
    return agg

def _prep_buffer(buffer_df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Medel 'Antal per pall' per artikel från buffertpallar, ta bort extremt små outliers (<50% av median)."""
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
        filt = vals[vals >= 0.5 * med]  # ta bort extremt små outliers
        if len(filt) == 0:
            filt = vals
        return float(filt.mean())

    agg = (b.groupby("Artikelnummer")["AntalRaw"]
             .apply(_robust_mean)
             .rename("Antal per pall")
             .reset_index())
    return agg


# ---------------------------
# Huvud-API (snabbt underlag)
# ---------------------------

def compute_sales_metrics(
    df_norm: pd.DataFrame,
    today=None,
    saldo_norm: Optional[pd.DataFrame] = None,
    buffer_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Tar fram minsta möjliga underlag för våra två flikar."""
    df = _prep_plocklogg(df_norm)

    # Per dag per artikel (alla zoner)
    daily_all = _build_daily(df)
    any_stats = _days_and_avg(daily_all).rename(
        columns={"Dagar": "Antal dagar i plock",
                 "SnittPerDag": "Snitt beställt per plockdag"}
    )

    # Zon-set per artikel
    # (Filtrera bort NaN/okända zoner så de inte stör 'Endast E' / 'E & H')
    if "Zon" in df.columns:
        zmap = (df.dropna(subset=["Zon"])
                  .groupby("Artikelnummer")["Zon"]
                  .apply(lambda s: "".join(sorted(set([str(x)[:1] for x in s if isinstance(x, str) and x]))))
                  .rename("ZonSet")
                  .reset_index())
    else:
        zmap = pd.DataFrame({"Artikelnummer": [], "ZonSet": []})

    # Endast E (E-dagar och E-snitt)
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

    # Bas
    base = any_stats.merge(zmap, on="Artikelnummer", how="left") \
                    .merge(e_stats, on="Artikelnummer", how="left")

    # Plockplats från saldo
    saldo_m = _prep_saldo(saldo_norm)
    base = base.merge(saldo_m, on="Artikelnummer", how="left")

    # Antal per pall från buffert
    buff_m = _prep_buffer(buffer_df)
    base = base.merge(buff_m, on="Artikelnummer", how="left")

    # Typer/format
    for col in ["Antal dagar i plock",
                "Antal dagar i plock (Endast E-zon)"]:
        if col in base.columns:
            base[col] = _to_num(base[col]).astype("Int64")

    for col in ["Snitt beställt per plockdag",
                "Snitt beställt per plockdag (Endast E-zon)",
                "Antal per pall"]:
        if col in base.columns:
            base[col] = _to_num(base[col]).round(2)

    # Om Artikel finns i df_norm, lägg till
    if "Artikel" in getattr(df_norm, "columns", []):
        names = df_norm[["Artikelnummer", "Artikel"]].drop_duplicates()
        base = base.merge(names, on="Artikelnummer", how="left")

    # Kolumnordning (intern)
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
    """Bygger två flikar enligt krav och öppnar i Excel."""
    if metrics is None or metrics.empty:
        raise RuntimeError("Inga data att visa.")

    df = metrics.copy()

    # -------- Flik 1: Rekomenderade buffertuppdateringar --------
    mask_1 = df.get("Plockplats", pd.Series([""]*len(df))).astype(str).str.endswith("1")
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
    # ZonSet = t.ex. "E", "EH", "HE", "H", "AR" ...
    zonset = df.get("ZonSet")
    if zonset is not None:
        only_e  = zonset.fillna("").str.fullmatch(r"E", na=False)
        only_eh = zonset.fillna("").str.fullmatch(r"EH|HE", na=False)
        ehe = pd.concat([
            df[only_e ].assign(Kategori="Endast E"),
            df[only_eh].assign(Kategori="Endast E & H"),
        ], ignore_index=True)
    else:
        ehe = df.iloc[0:0].copy()
        ehe["Kategori"] = pd.Series(dtype=object)

    ehe_cols = ["Artikelnummer",
                "Plockplats",
                "Antal dagar i plock (Endast E-zon)",
                "Snitt beställt per plockdag (Endast E-zon)",
                "Antal per pall",
                "Kategori"]
    for c in ehe_cols:
        if c not in ehe.columns:
            ehe[c] = pd.NA

    ehe = (ehe[ehe_cols]
             .sort_values(by=["Antal dagar i plock (Endast E-zon)",
                              "Snitt beställt per plockdag (Endast E-zon)"],
                          ascending=[False, False])
             .reset_index(drop=True))

    sheets: Dict[str, pd.DataFrame] = {
        "Rekomenderade buffertuppdateringar": rec,
        "Endast EH i Plock": ehe,
    }
    return open_sales_excel(sheets, label="sales_insights_fast")
