# io/file_readers.py
from __future__ import annotations
import pandas as pd
import numpy as np
from .schemas import NOT_PUTAWAY_SCHEMA, SALDO_SCHEMA, PICK_LOG_SCHEMA
from ..utils.common import _clean_columns, smart_to_datetime, to_num, find_col

def _read_not_putaway_csv(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="utf-8-sig")
        if df.shape[1] == 1 and len(df):
            first = str(df.iloc[0, 0])
            if "\t" in first:
                df = pd.read_csv(path, dtype=str, sep="\t", engine="python", encoding="utf-8-sig")
        return _clean_columns(df)
    except Exception:
        return _clean_columns(pd.read_csv(path, dtype=str, sep="\t", engine="python", encoding="utf-8-sig"))

def normalize_not_putaway(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.copy()
    def col(key: str, required: bool, default=None) -> str:
        return find_col(df, NOT_PUTAWAY_SCHEMA[key], required=required, default=default)
    art_col  = col("artikel", True)
    name_col = col("namn", False, default=None)
    qty_col  = col("antal", True)
    st_col   = col("status", False, default=None)
    pall_col = col("pallnr", False, default=None)
    sscc_col = col("sscc", False, default=None)
    chg_col  = col("andrad", False, default=None)
    exp_col  = col("utgang", False, default=None)
    out = pd.DataFrame({
        "Artikel": df[art_col].astype(str).str.strip(),
        "Namn":    df[name_col].astype(str).str.strip() if name_col else "",
        "Antal":   df[qty_col].map(to_num).astype(float),
        "Status":  pd.to_numeric(df[st_col], errors="coerce") if st_col else pd.Series([np.nan]*len(df)),
        "Pall nr": df[pall_col].astype(str) if pall_col else "",
        "SSCC":    df[sscc_col].astype(str) if sscc_col else "",
        "Ändrad":  smart_to_datetime(df[chg_col]) if chg_col else pd.NaT,
        "Utgång":  smart_to_datetime(df[exp_col]) if exp_col else pd.NaT,
    })
    for c in ["Namn","Pall nr","SSCC"]:
        if c in out.columns: out[c] = out[c].fillna("").astype(str).str.strip()
    return out

def normalize_saldo(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _clean_columns(df_raw.copy())
    def col(key: str, required: bool, default=None) -> str:
        from .schemas import SALDO_SCHEMA
        from ..utils.common import find_col
        return find_col(df, SALDO_SCHEMA[key], required=required, default=default)
    art_col   = col("artikel", True)
    saldo_col = col("plocksaldo", False, default=None)
    plats_col = col("plockplats", False, default=None)
    if saldo_col is None:
        return pd.DataFrame(columns=["Artikel", "Plocksaldo", "Plockplats"])
    out = pd.DataFrame({
        "Artikel": df[art_col].astype(str).str.strip(),
        "Plocksaldo": pd.to_numeric(df[saldo_col].map(to_num), errors="coerce").fillna(0.0),
        "Plockplats": (df[plats_col].astype(str).str.strip() if plats_col else pd.Series([""]*len(df))),
    })
    agg = (out.groupby("Artikel", as_index=False)
              .agg({"Plocksaldo":"sum","Plockplats":lambda s: next((x for x in s if isinstance(x,str) and x.strip()), "")}))
    return agg

def normalize_pick_log(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = _clean_columns(df_raw.copy())

    # Här tar vi fasta kolumner:
    # Kolumn M = Artikelnummer (index 12 om 0-baserat)
    # Kolumn N = Artikel (index 13 om 0-baserat)
    try:
        art_col = df.columns[12]  # M
        name_col = df.columns[13] # N
    except Exception as e:
        raise RuntimeError(f"Kunde inte hitta kolumner M/N i plocklogg: {e}")

    # Hämta övriga viktiga kolumner (antal, datum) via schema
    qty_col = find_col(df, PICK_LOG_SCHEMA["antal"], required=True)
    dt_col  = find_col(df, PICK_LOG_SCHEMA["datum"], required=True)

    out = pd.DataFrame({
        "Artikelnummer": df[art_col].astype(str).str.strip(),
        "Artikel": df[name_col].astype(str).str.strip(),
        "Plockat": pd.to_numeric(df[qty_col].map(to_num), errors="coerce").fillna(0.0).astype(float),
        "Datum": smart_to_datetime(df[dt_col]),
    })
    return out
