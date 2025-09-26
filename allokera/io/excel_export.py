# io/excel_export.py
import pandas as pd
from ..utils.common import _open_df_in_excel

def open_sales_excel(sheets_dict_or_df, label: str = "sales_insights") -> str:
    return _open_df_in_excel(sheets_dict_or_df, label=label)

def open_refill_excel(hp_df: pd.DataFrame, autostore_df: pd.DataFrame) -> str:
    return _open_df_in_excel({"Refill HP": hp_df, "Refill AUTOSTORE": autostore_df}, label="refill")

def open_allocated_excel(allocated_df: pd.DataFrame) -> str:
    return _open_df_in_excel(allocated_df, label="allocated_orders")

def open_nearmiss_excel(near_df: pd.DataFrame) -> str:
    return _open_df_in_excel(near_df, label="near_miss_15pct_INSTEAD_R_or_A")
