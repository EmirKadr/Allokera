# utils/common.py
from __future__ import annotations
import os, sys, re, subprocess, tempfile
import pandas as pd
import numpy as np
import tkinter as tk
from typing import List
from io import StringIO  # not used but handy

def _open_df_in_excel(df, label: str = "data") -> str:
    """Skriv DF (eller {blad: DF}) till temporär fil och öppna i OS:et."""
    import importlib
    if isinstance(df, dict):
        engine = None
        if importlib.util.find_spec("openpyxl"):
            engine = "openpyxl"
        elif importlib.util.find_spec("xlsxwriter"):
            engine = "xlsxwriter"
        else:
            raise RuntimeError("Saknar Excel-skrivare (installera 'openpyxl' eller 'xlsxwriter').")
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{label}.xlsx")
        path = tmp.name; tmp.close()
        with pd.ExcelWriter(path, engine=engine) as writer:
            for sheet, d in df.items():
                dd = d if isinstance(d, pd.DataFrame) else pd.DataFrame(d)
                dd.to_excel(writer, sheet_name=str(sheet)[:31] or "Sheet1", index=False)
    else:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f"_{label}.csv")
        path = tmp.name; tmp.close()
        (df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)).to_csv(path, index=False, encoding="utf-8-sig")
    try:
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        elif sys.platform == "darwin":
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["xdg-open", path])
    except Exception:
        pass
    return path

def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df.rename(columns=lambda c: str(c).replace("\ufeff", "").strip(), inplace=True)
    except Exception:
        pass
    return df

def smart_to_datetime(s) -> pd.Series:
    """Robust datumtolkning (ISO→dayfirst=False, annars True; fallback tvärtom)."""
    try:
        ser = pd.Series(s) if not isinstance(s, pd.Series) else s
        vals = ser.dropna().astype(str).str.strip()
        sample = vals.head(50)
        numeric_like = (sample.str.match(r"^\d{8}$").sum() >= max(1, int(len(sample) * 0.6)))
        if numeric_like:
            dt = pd.to_datetime(ser, format="%Y%m%d", errors="coerce")
            if not dt.isna().all():
                return dt
        iso_like = (sample.str.match(r"^\d{4}-\d{2}-\d{2}").sum() >= max(1, int(len(sample) * 0.6)))
        primary_dayfirst = False if iso_like else True
        dt = pd.to_datetime(ser, errors="coerce", dayfirst=primary_dayfirst)
        if hasattr(dt, "isna") and getattr(dt, "isna")().all():
            dt = pd.to_datetime(ser, errors="coerce", dayfirst=not primary_dayfirst)
        return dt
    except Exception:
        try: return pd.to_datetime(s, errors="coerce", dayfirst=True)
        except Exception: return pd.to_datetime(s, errors="coerce", dayfirst=False)

def to_num(x) -> float:
    import pandas as pd, re
    if pd.isna(x): return 0.0
    s = str(x).replace(" ", "").replace(",", ".")
    m = re.search(r"[-+]?\d*\.?\d+", s)
    return float(m.group()) if m else 0.0

def find_col(df: pd.DataFrame, candidates: List[str], required: bool = True, default=None) -> str:
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in cols: return cols[cand.lower()]
    for key, orig in cols.items():
        for cand in candidates:
            if cand.lower() in key: return orig
    if required and default is None:
        raise KeyError(f"Hittar inte kolumnerna {candidates} i {list(df.columns)}")
    return default

def logprintln(txt_widget: tk.Text, msg: str) -> None:
    txt_widget.configure(state="normal")
    txt_widget.insert("end", msg + "\n")
    txt_widget.see("end")
    txt_widget.configure(state="disabled")
    txt_widget.update()

def _first_path_from_dnd(event_data: str) -> str:
    raw = str(event_data).strip()
    if raw.startswith("{") and raw.endswith("}"): raw = raw[1:-1]
    if raw.startswith('"') and raw.endswith('"'): raw = raw[1:-1]
    return raw
