# gui/app.py
from __future__ import annotations
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import numpy as np

from ..logic.allocation import allocate
from ..logic.refill import calculate_refill, annotate_refill, _reclassify_skrymmande
from ..logic.sales import compute_sales_metrics, open_sales_insights

from ..io.file_readers import (
    _read_not_putaway_csv,
    normalize_not_putaway,
    normalize_saldo,
    normalize_pick_log,
)
from ..io.excel_export import (
    open_refill_excel,
    open_allocated_excel,
    open_nearmiss_excel,
)
from ..io.schemas import ORDER_SCHEMA
from ..utils.common import (
    logprintln,
    _clean_columns,
    _first_path_from_dnd,
    find_col,
)
from ..config.constants import APP_TITLE

try:
    from tkinterdnd2 import DND_FILES, TkinterDnD
except ImportError:
    DND_FILES = None
    TkinterDnD = None


class App(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.master = master
        self.pack(fill="both", expand=True)
        self._create_widgets()

    def _log(self, msg: str, level: str = "info") -> None:
        logprintln(self.log, msg)

    def _create_widgets(self) -> None:
        self.columnconfigure(1, weight=1)

        ttk.Label(self, text="Beställningslinjer (CSV):").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.orders_var = tk.StringVar()
        self.orders_entry = ttk.Entry(self, textvariable=self.orders_var); self.orders_entry.grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(self, text="Bläddra...", command=self.pick_orders).grid(row=0, column=2, padx=8)

        ttk.Label(self, text="Buffertpallar (CSV):").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        self.buffer_var = tk.StringVar()
        self.buffer_entry = ttk.Entry(self, textvariable=self.buffer_var); self.buffer_entry.grid(row=1, column=1, sticky="ew", padx=8)
        ttk.Button(self, text="Bläddra...", command=self.pick_buffer).grid(row=1, column=2, padx=8)

        ttk.Label(self, text="Saldo inkl. automation (CSV):").grid(row=2, column=0, sticky="w", padx=8, pady=6)
        self.automation_var = tk.StringVar()
        self.automation_entry = ttk.Entry(self, textvariable=self.automation_var); self.automation_entry.grid(row=2, column=1, sticky="ew", padx=8)
        ttk.Button(self, text="Bläddra...", command=self.pick_automation).grid(row=2, column=2, padx=8)

        ttk.Label(self, text="Ej inlagrade artiklar (CSV):").grid(row=3, column=0, sticky="w", padx=8, pady=6)
        self.not_putaway_var = tk.StringVar()
        self.not_putaway_entry = ttk.Entry(self, textvariable=self.not_putaway_var); self.not_putaway_entry.grid(row=3, column=1, sticky="ew", padx=8)
        ttk.Button(self, text="Bläddra...", command=self.pick_not_putaway).grid(row=3, column=2, padx=8)

        ttk.Label(self, text="Plocklogg (CSV/XLSX) — Sales:").grid(row=4, column=0, sticky="w", padx=8, pady=6)
        self.sales_var = tk.StringVar()
        self.sales_entry = ttk.Entry(self, textvariable=self.sales_var); self.sales_entry.grid(row=4, column=1, sticky="ew", padx=8)
        ttk.Button(self, text="Bläddra...", command=self.pick_sales).grid(row=4, column=2, padx=8)

        # Körning + öppna/rapporter
        self.run_btn = ttk.Button(self, text="Kör allokering", command=self.run_allocation); self.run_btn.grid(row=5, column=0, columnspan=3, pady=10)

        self.open_result_btn = ttk.Button(self, text="Öppna allokerade pallar", command=self.open_result_in_excel, state="disabled")
        self.open_nearmiss_btn = ttk.Button(self, text="Öppna near-miss", command=self.open_nearmiss_in_excel, state="disabled")
        self.open_refill_btn = ttk.Button(self, text="Öppna påfyllningspallar", command=self.open_refill_in_excel, state="disabled")
        self.open_sales_btn = ttk.Button(self, text="Öppna försäljningsinsikter", command=self.compute_and_open_sales, state="normal")
        self.open_result_btn.grid(row=99, column=0, pady=10)
        self.open_nearmiss_btn.grid(row=99, column=1, pady=10)
        self.open_refill_btn.grid(row=99, column=2, pady=10)
        self.open_sales_btn.grid(row=99, column=3, pady=10)

        ttk.Label(self, text="Logg / Summering:").grid(row=6, column=0, sticky="w", padx=8)
        self.log = tk.Text(self, height=14, width=110, state="disabled"); self.log.grid(row=7, column=0, columnspan=4, sticky="nsew", padx=8, pady=8)
        self.rowconfigure(7, weight=1)

        ttk.Label(self, text="Summering per Källtyp").grid(row=8, column=0, sticky="w", padx=8)
        self.summary_table = ttk.Treeview(self, columns=("ktyp", "antal_rader", "antal_kolli"), show="headings", height=5)
        self.summary_table.heading("ktyp", text="Källtyp")
        self.summary_table.heading("antal_rader", text="antal rader")
        self.summary_table.heading("antal_kolli", text="antal kolli")
        self.summary_table.column("ktyp", anchor="w", width=160)
        self.summary_table.column("antal_rader", anchor="e", width=140)
        self.summary_table.column("antal_kolli", anchor="e", width=140)
        self.summary_table.grid(row=9, column=0, columnspan=4, sticky="ew", padx=8, pady=(0,8))
        for ktyp in ("HELPALL", "AUTOSTORE", "HUVUDPLOCK", "SKRYMMANDE"):
            self.summary_table.insert("", "end", iid=ktyp, values=(ktyp, 0, 0))

        # cache
        self.last_result_df = None
        self.last_nearmiss_instead_df = None
        self._orders_raw = None
        self._buffer_raw = None
        self._result_df = None
        self._not_putaway_raw = None
        self._not_putaway_norm = None
        self._saldo_norm = None
        self._sales_metrics_df = None
        self._last_refill_hp_df = None
        self._last_refill_autostore_df = None

        # Drag & Drop – bind per entry om libben finns
        if TkinterDnD and DND_FILES:
            def bind_drop(entry_widget: ttk.Entry, var: tk.StringVar) -> None:
                try:
                    entry_widget.drop_target_register(DND_FILES)
                    def _on_drop(event, _var=var):
                        path = _first_path_from_dnd(event.data)
                        if path: _var.set(path)
                    entry_widget.dnd_bind("<<Drop>>", _on_drop)
                except Exception:
                    pass
            bind_drop(self.orders_entry, self.orders_var)
            bind_drop(self.buffer_entry, self.buffer_var)
            bind_drop(self.automation_entry, self.automation_var)
            bind_drop(self.not_putaway_entry, self.not_putaway_var)
            bind_drop(self.sales_entry, self.sales_var)

    # -------- File pickers --------
    def pick_orders(self):       p = filedialog.askopenfilename(title="Välj beställningsrader (CSV)", filetypes=[("CSV", "*.csv"), ("Alla filer","*.*")]);  self.orders_var.set(p or self.orders_var.get())
    def pick_automation(self):   p = filedialog.askopenfilename(title="Välj Saldo inkl. automation (CSV)", filetypes=[("CSV", "*.csv"), ("Alla filer","*.*")]); self.automation_var.set(p or self.automation_var.get())
    def pick_buffer(self):       p = filedialog.askopenfilename(title="Välj buffertpallar (CSV)", filetypes=[("CSV", "*.csv"), ("Alla filer","*.*")]);      self.buffer_var.set(p or self.buffer_var.get())
    def pick_not_putaway(self):  p = filedialog.askopenfilename(title="Välj 'Ej inlagrade artiklar' (CSV)", filetypes=[("CSV", "*.csv"), ("Alla filer","*.*")]); self.not_putaway_var.set(p or self.not_putaway_var.get())
    def pick_sales(self):        p = filedialog.askopenfilename(title="Välj plocklogg (CSV/XLSX)", filetypes=[("CSV/XLSX", "*.csv;*.xlsx"), ("CSV","*.csv"), ("Excel","*.xlsx"), ("Alla filer","*.*")]); self.sales_var.set(p or self.sales_var.get())

    # -------- Öppna/Export --------
    def open_result_in_excel(self):
        if isinstance(self.last_result_df, pd.DataFrame) and not self.last_result_df.empty:
            try: open_allocated_excel(self.last_result_df)
            except Exception as e: messagebox.showerror(APP_TITLE, f"Kunde inte öppna resultat i Excel:\n{e}")
        else:
            messagebox.showinfo(APP_TITLE, "Det finns inget resultat att öppna ännu. Kör allokeringen först.")

    def open_nearmiss_in_excel(self):
        if isinstance(self.last_nearmiss_instead_df, pd.DataFrame) and not self.last_nearmiss_instead_df.empty:
            try: open_nearmiss_excel(self.last_nearmiss_instead_df)
            except Exception as e: messagebox.showerror(APP_TITLE, f"Kunde inte öppna near-miss i Excel:\n{e}")
        else:
            messagebox.showinfo(APP_TITLE, "Det finns ingen near-miss INSTEAD R/A att öppna ännu.")

    def open_refill_in_excel(self):
        if isinstance(self._last_refill_hp_df, pd.DataFrame) or isinstance(self._last_refill_autostore_df, pd.DataFrame):
            try:
                hp = self._last_refill_hp_df.copy() if isinstance(self._last_refill_hp_df, pd.DataFrame) else pd.DataFrame()
                asr = self._last_refill_autostore_df.copy() if isinstance(self._last_refill_autostore_df, pd.DataFrame) else pd.DataFrame()
                if isinstance(self._sales_metrics_df, pd.DataFrame) and not self._sales_metrics_df.empty:
                    hp = annotate_refill(hp, self._sales_metrics_df)
                    asr = annotate_refill(asr, self._sales_metrics_df)
                open_refill_excel(hp, asr)
            except Exception as e:
                messagebox.showerror(APP_TITLE, f"Kunde inte öppna påfyllningspallar i Excel:\n{e}")
        else:
            messagebox.showinfo(APP_TITLE, "Det finns ingen påfyllningspallsrapport att öppna ännu. Kör allokeringen först.")

    # -------- SALES: körs vid klick --------
    def compute_and_open_sales(self):
        path = self.sales_var.get().strip()
        if not path:
            messagebox.showinfo(APP_TITLE, "Välj plocklogg (CSV/XLSX) först.")
            return

        # Se till att SALDO är laddat (om fil valts men inte läst än)
        if not isinstance(self._saldo_norm, pd.DataFrame):
            auto_path = self.automation_var.get().strip()
            if auto_path:
                try:
                    auto_raw = pd.read_csv(auto_path, dtype=str, sep=None, engine="python")
                    auto_raw = _clean_columns(auto_raw)
                    self._saldo_norm = normalize_saldo(auto_raw)
                    self._log(f"Saldo inläst för sales: {len(self._saldo_norm)} rader.")
                except Exception as e:
                    self._log(f"Kunde inte läsa saldo-CSV för sales: {e}")

        # Se till att BUFFERT är laddad (om fil valts men inte läst än)
        if not isinstance(self._buffer_raw, pd.DataFrame):
            buffer_path = self.buffer_var.get().strip()
            if buffer_path:
                try:
                    buf_raw = pd.read_csv(buffer_path, dtype=str, sep=None, engine="python")
                    self._buffer_raw = _clean_columns(buf_raw)
                    self._log(f"Buffertpallar inläst för sales: {len(self._buffer_raw)} rader.")
                except Exception as e:
                    self._log(f"Kunde inte läsa buffert-CSV för sales: {e}")

        # Läs plockloggen robust
        try:
            if path.lower().endswith(".xlsx"):
                xls = pd.ExcelFile(path)
                df_raw = xls.parse(xls.sheet_names[0], dtype=str)
            else:
                try:
                    df_raw = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="utf-8-sig")
                except Exception:
                    try:
                        df_raw = pd.read_csv(path, dtype=str, sep="\t", engine="python", encoding="utf-8-sig")
                    except Exception:
                        df_raw = pd.read_csv(path, dtype=str, sep=";", engine="python", encoding="utf-8-sig")
            df_raw = _clean_columns(df_raw)
            df_norm = normalize_pick_log(df_raw)
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Kunde inte läsa/normalisera plockloggen:\n{e}")
            return

        # Beräkna & öppna
        try:
            sd  = self._saldo_norm  if isinstance(self._saldo_norm,  pd.DataFrame) else None
            buf = self._buffer_raw  if isinstance(self._buffer_raw,  pd.DataFrame) else None
            metrics = compute_sales_metrics(df_norm, saldo_norm=sd, buffer_df=buf)
            self._sales_metrics_df = metrics.copy()

            # Logg: storlek + enkel träffbild
            n_art = int(metrics["Artikelnummer"].nunique()) if "Artikelnummer" in metrics.columns else len(metrics)
            # kandidater som slutar på "1"
            got_1 = int(metrics["Plockplats"].astype(str).str.endswith("1").sum()) if "Plockplats" in metrics.columns else 0
            # EH-kandidater (Endast E / Endast E & H)
            if "ZonSet" in metrics.columns:
                only_e  = metrics["ZonSet"].fillna("").str.fullmatch(r"E", na=False).sum()
                only_eh = metrics["ZonSet"].fillna("").str.fullmatch(r"EH|HE", na=False).sum()
            else:
                only_e = only_eh = 0

            self._log(f"Plocklogg inläst: {len(df_norm)} rader → {n_art} artiklar i underlaget.")
            if sd is not None:
                self._log(f"Saldo: Plats slutar på '1' för {got_1} artiklar (kandidater för buffertuppdatering).")
            if "ZonSet" in metrics.columns:
                self._log(f"Zon: Endast E = {only_e}, Endast E & H = {only_eh} artiklar.")

            xls_path = open_sales_insights(metrics)
            self._log(f"Öppnade försäljningsinsikter i Excel (temporär fil): {xls_path}")
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Fel vid beräkning/öppning av försäljningsinsikter:\n{e}")

    # -------- Summering --------
    def update_summary_table(self, result_df: pd.DataFrame) -> None:
        for ktyp in ("HELPALL", "AUTOSTORE", "HUVUDPLOCK", "SKRYMMANDE"):
            try:
                sub = result_df[result_df.get("Källtyp", "") == ktyp]
                row_count = int(len(sub))
                qty_col = find_col(result_df, ORDER_SCHEMA["qty"], required=False, default=None)
                kolli = float(pd.to_numeric(sub[qty_col], errors="coerce").sum()) if qty_col and not sub.empty else 0.0
            except Exception:
                row_count, kolli = 0, 0.0
            if ktyp == "HELPALL":
                row_text = f"{row_count} pallar"
            elif ktyp == "AUTOSTORE":
                row_text = f"{row_count} rader"
            elif ktyp in ("HUVUDPLOCK", "SKRYMMANDE"):
                pallar = (row_count / 20.0) if row_count else 0.0
                pallar_str = f"{pallar:.2f}".replace(".", ",")
                row_text = f"{row_count} rader ({pallar_str} pallar)"
            else:
                row_text = str(row_count)
            kolli_text = f"{int(round(kolli))}"
            if ktyp not in self.summary_table.get_children(""):
                self.summary_table.insert("", "end", iid=ktyp, values=(ktyp, row_text, kolli_text))
            else:
                self.summary_table.item(ktyp, values=(ktyp, row_text, kolli_text))

    # -------- Efter-allokering extra logg --------
    def _log_post_allocation(self, result: pd.DataFrame, near_instead: pd.DataFrame,
                             hp_df: pd.DataFrame, as_df: pd.DataFrame) -> None:
        self._log(f"Auto-refill klart: HP {len(hp_df)} rader, AUTOSTORE {len(as_df)} rader (cachad).")

        # Summering per zon
        try:
            qty_col = find_col(result, ORDER_SCHEMA["qty"], required=False, default=None)
            if qty_col and ("Zon (beräknad)" in result.columns):
                sums = (pd.to_numeric(result[qty_col], errors="coerce")
                          .groupby(result["Zon (beräknad)"]).sum())
                self._log("\nSummering per zon:")
                for z in ["A", "H", "R", "S"]:
                    val = int(round(float(sums.get(z, 0)))) if hasattr(sums, "get") else 0
                    self._log(f"  Zon {z}: {val}")
        except Exception:
            pass

        # 15% near-miss statistik
        try:
            nm_count = int(len(near_instead)) if isinstance(near_instead, pd.DataFrame) else 0
            self._log("\n15% near-miss statistik:")
            self._log(f"  Near-miss som slutade som R/A: {nm_count}")
        except Exception:
            pass

    # -------- Orkestrering (allokering) --------
    def run_allocation(self) -> None:
        orders_path = self.orders_var.get().strip()
        buffer_path = self.buffer_var.get().strip()
        automation_path = self.automation_var.get().strip()
        not_putaway_path = self.not_putaway_var.get().strip()

        if not orders_path or not buffer_path:
            messagebox.showerror(APP_TITLE, "Välj både beställningsfil och buffertfil.")
            return

        try:
            self._log("Läser in filer...")
            orders_raw = pd.read_csv(orders_path, dtype=str, sep=None, engine="python")
            buffer_raw = pd.read_csv(buffer_path, dtype=str, sep=None, engine="python")

            if not not_putaway_path:
                self._not_putaway_raw = None
                self._not_putaway_norm = None
            else:
                npu_raw = _read_not_putaway_csv(not_putaway_path)
                self._not_putaway_raw = npu_raw.copy()
                self._not_putaway_norm = normalize_not_putaway(npu_raw)
                self._log(f"'Ej inlagrade' inläst ({len(self._not_putaway_norm)} rader).")

            if automation_path:
                auto_raw = pd.read_csv(automation_path, dtype=str, sep=None, engine="python")
                self._saldo_norm = normalize_saldo(_clean_columns(auto_raw))
            else:
                self._saldo_norm = None

            self._orders_raw = _clean_columns(orders_raw)
            self._buffer_raw = _clean_columns(buffer_raw)
        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Kunde inte läsa CSV-filerna:\n{e}")
            return

        try:
            self._log("\n--------------")
            self._log("Kör allokering (Helpall → AutoStore → Huvudplock, FIFO) + 15%-near-miss loggning + Status {29,30,32}-filter...")
            result, near = allocate(self._orders_raw, self._buffer_raw, log=self._log)
            result = _reclassify_skrymmande(result, self._saldo_norm)
            self._log("Skapar resultat i minnet...")

            near_instead = near[near.get("Gäller (INSTEAD R/A)", False) == True].copy() if "Gäller (INSTEAD R/A)" in near.columns else near.iloc[0:0].copy()
            self.last_result_df = result.copy()
            self.last_nearmiss_instead_df = near_instead.copy()
            self._result_df = result.copy()

            try:
                self.update_summary_table(result)
            except Exception:
                pass

            # Auto-refill
            try:
                hp_df, as_df = calculate_refill(
                    result,
                    self._buffer_raw,
                    saldo_df=(self._saldo_norm if isinstance(self._saldo_norm, pd.DataFrame) else pd.DataFrame()),
                    not_putaway_df=(self._not_putaway_norm if isinstance(self._not_putaway_norm, pd.DataFrame) else pd.DataFrame()),
                )
                self._last_refill_hp_df = hp_df
                self._last_refill_autostore_df = as_df
                self._log_post_allocation(result, near_instead, hp_df, as_df)
            except Exception as e:
                self._log(f"Refill kunde inte beräknas: {e}")

            self.open_result_btn.configure(state="normal")
            self.open_nearmiss_btn.configure(state="normal" if not near_instead.empty else "disabled")
            self.open_refill_btn.configure(state="normal")

        except Exception as e:
            messagebox.showerror(APP_TITLE, f"Allokering misslyckades:\n{e}")
