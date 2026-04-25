# -*- coding: utf-8 -*-
"""
股票分析工具 v4.0 - 重构修正版
"""

from __future__ import annotations

import os
import time
import queue
import threading
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import akshare as ak
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# ─────────────────────────────────────────────
# 常量定义
# ─────────────────────────────────────────────

APP_TITLE   = "股票分析工具 v4.0 - 多线程 + 增强实时行情 + 所属行业"
APP_GEOMETRY = "1300x980"
APP_MINSIZE = (1200, 880)

DEFAULT_START_DATE = "20240101"
MAX_WORKERS        = 8
BIG_INFLOW_THRESH  = 50_000_000
API_SLEEP          = 0.08
STOCK_SLEEP        = 0.2

LHB_CODE_COL     = "代码"
LHB_NAME_COL     = "名称"
LHB_DATE_COL     = "上榜日"
LHB_BUY_COL      = "龙虎榜净买额"
LHB_TURNOVER_COL = "换手率"

REALTIME_COLS = ["最新价", "涨跌幅", "换手率", "量比", "振幅"]

IMPORT_CODE_CANDIDATES = ["代码", "code", "股票代码", "symbol", "Symbol", "Code", "证券代码"]
IMPORT_NAME_CANDIDATES = ["名称", "股票简称", "name", "简称", "Name", "证券简称"]

DISPLAY_CODE_LIMIT = 500

# ─────────────────────────────────────────────
# 可复用组件
# ─────────────────────────────────────────────

class ScrollableFrame(ttk.Frame):
    """带垂直滚动条的容器 Frame"""
    def __init__(self, container: tk.Widget, *args, **kwargs) -> None:
        super().__init__(container, *args, **kwargs)
        canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        self.scrollable_frame = ttk.Frame(canvas)

        self.scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.canvas = canvas

# ─────────────────────────────────────────────
# 数据获取层 (DataFetcher)
# ─────────────────────────────────────────────

class DataFetcher:
    @staticmethod
    def get_stock_name(code: str) -> str:
        try:
            info = ak.stock_individual_info_em(symbol=code)
            if not info.empty:
                row = info[info["item"] == "股票简称"]
                if not row.empty:
                    return str(row["value"].iloc[0])
        except Exception:
            pass
        return code

    @staticmethod
    def get_stock_industry(code: str) -> str:
        try:
            info = ak.stock_individual_info_em(symbol=code)
            if not info.empty:
                row = info[info["item"].str.contains("行业", na=False)]
                if not row.empty:
                    return str(row["value"].iloc[0])
            return "未知"
        except Exception:
            return "获取失败"

    @staticmethod
    def get_price_history(code: str, start: str, end: str) -> pd.DataFrame:
        try:
            df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start, end_date=end, adjust="qfq")
            if not df.empty:
                df["日期"] = pd.to_datetime(df["日期"])
            return df
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def get_fund_flow(code: str) -> pd.DataFrame:
        market = "sh" if code.startswith(("6", "9")) else "sz"
        try:
            df = ak.stock_individual_fund_flow(stock=code, market=market)
            if not df.empty and "日期" in df.columns:
                df["日期"] = pd.to_datetime(df["日期"])
            return df
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def get_realtime_price(code: str) -> Optional[float]:
        try:
            df = ak.stock_bid_ask_em(symbol=code)
            if not df.empty and "最新价" in df.columns:
                return float(df["最新价"].iloc[0])
        except Exception:
            pass
        return None

    @staticmethod
    def get_all_realtime_spot() -> pd.DataFrame:
        try:
            df = ak.stock_zh_a_spot_em()
            cols_needed = ["代码"] + REALTIME_COLS
            available = [c for c in cols_needed if c in df.columns]
            df = df[available].copy()
            for col in REALTIME_COLS:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").round(2)
            return df
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def get_lhb(start: str, end: str) -> pd.DataFrame:
        try:
            return ak.stock_lhb_detail_em(start_date=start, end_date=end)
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def build_lhb_summary(lhb_df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
        has_turnover = LHB_TURNOVER_COL in lhb_df.columns
        agg_dict: dict = {
            LHB_NAME_COL: "first",
            LHB_DATE_COL: ["count", "min", "max"],
            LHB_BUY_COL: "sum",
        }
        if has_turnover:
            agg_dict[LHB_TURNOVER_COL] = list

        summary = lhb_df.groupby(LHB_CODE_COL).agg(agg_dict).reset_index()
        columns = [LHB_CODE_COL, "简称", "上榜次数", "首次上榜", "最后上榜", "累计净买额"]
        if has_turnover:
            columns.append("上榜日换手率列表")
        summary.columns = columns

        if has_turnover:
            summary["平均上榜换手率(%)"] = summary["上榜日换手率列表"].apply(
                lambda x: round(sum(x) / len(x), 2) if (isinstance(x, list) and x) else None
            )
            summary["上榜日换手率列表"] = summary["上榜日换手率列表"].apply(
                lambda x: ", ".join(f"{v:.2f}%" for v in x) if isinstance(x, list) else ""
            )

        summary["累计净买额"] = summary["累计净买额"].round(0).astype(int)
        return summary, has_turnover

    @staticmethod
    def merge_realtime_to_summary(summary: pd.DataFrame, realtime_df: pd.DataFrame, code_col: str) -> pd.DataFrame:
        if realtime_df.empty:
            for col in REALTIME_COLS:
                summary[col] = "N/A"
            return summary

        summary = pd.merge(summary, realtime_df, left_on=code_col, right_on="代码", how="left")
        summary.drop(columns=["代码_y"], errors="ignore", inplace=True)
        summary.rename(columns={"代码_x": code_col}, errors="ignore", inplace=True)

        for col in REALTIME_COLS:
            if col not in summary.columns:
                summary[col] = pd.NA
            summary[col] = summary[col].fillna("N/A")
        return summary

# ─────────────────────────────────────────────
# 主应用 (UI 层)
# ─────────────────────────────────────────────

class StockAnalyzerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry(APP_GEOMETRY)
        self.root.minsize(*APP_MINSIZE)

        self._setup_style()
        self.task_queue = queue.Queue()
        self.running = False
        self.stop_requested = False
        self.canvases = []
        self.current_mode = "single"

        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        root.rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)

        self._build_top_bar(main_frame)
        self._build_content_notebook(main_frame)
        main_frame.rowconfigure(1, weight=1)
        self.switch_mode()

    def _setup_style(self) -> None:
        style = ttk.Style()
        style.configure("Big.TButton", font=("Segoe UI", 11, "bold"), padding=10)

    def _build_top_bar(self, parent: ttk.Frame) -> None:
        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        self._build_date_frame(top)
        self._build_mode_frame(top)
        self._build_button_frame(top)
        self.progress = ttk.Progressbar(top, mode="indeterminate", length=200)

    def _build_date_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="日期范围", padding=8)
        frame.pack(side="left", padx=10)
        ttk.Label(frame, text="开始:").pack(side="left", padx=(0, 4))
        self.start_date_entry = ttk.Entry(frame, width=12, font=("Segoe UI", 10))
        self.start_date_entry.pack(side="left", padx=4)
        self.start_date_entry.insert(0, DEFAULT_START_DATE)

        ttk.Label(frame, text="结束:").pack(side="left", padx=(10, 4))
        self.end_date_entry = ttk.Entry(frame, width=12, font=("Segoe UI", 10))
        self.end_date_entry.pack(side="left", padx=4)
        self.end_date_entry.insert(0, datetime.today().strftime("%Y%m%d"))

    def _build_mode_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.LabelFrame(parent, text="分析模式", padding=8)
        frame.pack(side="left", padx=10)
        self.mode_var = tk.StringVar(value="single")
        for label, value in [("个股分析", "single"), ("龙虎榜扫描", "scan")]:
            ttk.Radiobutton(frame, text=label, variable=self.mode_var, value=value, command=self.switch_mode).pack(side="left", padx=10)

    def _build_button_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.pack(side="right", padx=10)
        self.fetch_button = ttk.Button(frame, text="开始执行", command=self.start_task, style="Big.TButton")
        self.fetch_button.pack(side="left", padx=5)
        self.stop_button = ttk.Button(frame, text="停止", command=self.request_stop, state="disabled")
        self.stop_button.pack(side="left", padx=5)

    def _build_content_notebook(self, parent: ttk.Frame) -> None:
        self.content_notebook = ttk.Notebook(parent)
        self.content_notebook.grid(row=1, column=0, sticky="nsew")
        self.tab_single = ttk.Frame(self.content_notebook)
        self.tab_scan = ttk.Frame(self.content_notebook)
        self.content_notebook.add(self.tab_single, text="个股分析")
        self.content_notebook.add(self.tab_scan, text="龙虎榜扫描")
        self._build_single_tab()
        self._build_scan_tab()

    def _build_single_tab(self) -> None:
        input_frame = ttk.LabelFrame(self.tab_single, text="股票代码", padding=10)
        input_frame.pack(fill="x", pady=5)
        self.codes_entry = ttk.Entry(input_frame, font=("Segoe UI", 10))
        self.codes_entry.pack(fill="x", pady=5)
        self.codes_entry.insert(0, "600519,000001,300750")
        btn_bar = ttk.Frame(input_frame)
        btn_bar.pack(fill="x", pady=(8, 2))
        ttk.Button(btn_bar, text="从文件导入代码", command=self.import_codes_from_file).pack(side="left", padx=5)
        self.output_text_single = self._build_log_widget(self.tab_single, "日志")
        chart_frame = ttk.LabelFrame(self.tab_single, text="图表", padding=5)
        chart_frame.pack(fill="both", expand=True, pady=5)
        self.notebook_single = ttk.Notebook(chart_frame)
        self.notebook_single.pack(fill="both", expand=True)

    def _build_scan_tab(self) -> None:
        self.output_text_scan = self._build_log_widget(self.tab_scan, "扫描日志")
        filter_frame = ttk.LabelFrame(self.tab_scan, text="过滤条件", padding=8)
        filter_frame.pack(fill="x", pady=5)
        ttk.Label(filter_frame, text="最低平均换手率(%):").pack(side="left")
        self.turnover_threshold_entry = ttk.Entry(filter_frame, width=8)
        self.turnover_threshold_entry.pack(side="left", padx=5)
        result_frame = ttk.LabelFrame(self.tab_scan, text="结果表格", padding=5)
        result_frame.pack(fill="both", expand=True, pady=5)
        tree_scroll = ttk.Scrollbar(result_frame, orient="vertical")
        tree_scroll.pack(side="right", fill="y")
        self.tree = ttk.Treeview(result_frame, show="headings", yscrollcommand=tree_scroll.set)
        self.tree.pack(fill="both", expand=True)
        tree_scroll.config(command=self.tree.yview)

    def _build_log_widget(self, parent: ttk.Frame, label: str) -> tk.Text:
        frame = ttk.LabelFrame(parent, text=label, padding=5)
        frame.pack(fill="x", pady=5)
        text = tk.Text(frame, height=6, font=("Consolas", 10), wrap="word")
        text.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        sb.pack(side="right", fill="y")
        text.configure(yscrollcommand=sb.set)
        return text

    def switch_mode(self) -> None:
        self.current_mode = self.mode_var.get()
        idx = 0 if self.current_mode == "single" else 1
        self.codes_entry.configure(state="normal" if idx==0 else "disabled")
        self.content_notebook.select(idx)

    def log(self, message: str, mode: Optional[str] = None) -> None:
        target = self.output_text_single if (mode or self.current_mode) == "single" else self.output_text_scan
        target.insert(tk.END, message + "\n")
        target.see(tk.END)

    def start_task(self) -> None:
        if self.running: return
        start_d = self.start_date_entry.get().strip()
        end_d = self.end_date_entry.get().strip()
        save_dir = filedialog.askdirectory()
        if not save_dir: return

        self._reset_ui_for_task()
        threading.Thread(target=self._worker, args=(start_d, end_d, save_dir), daemon=True).start()
        self.root.after(400, self._poll_queue)

    def _reset_ui_for_task(self) -> None:
        self.running = True
        self.stop_requested = False
        self.fetch_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.progress.pack(side="right", padx=10)
        self.progress.start(12)

    def request_stop(self) -> None:
        self.stop_requested = True
        self.log("停止请求已发送...")

    def _worker(self, start: str, end: str, save_dir: str) -> None:
        try:
            if self.current_mode == "single":
                self._run_single_mode(start, end, save_dir)
            else:
                self._process_lhb_scan(start, end, save_dir)
        except Exception as e:
            self.task_queue.put(("fatal", str(e)))

    def _run_single_mode(self, start: str, end: str, save_dir: str) -> None:
        codes = [c.strip() for c in self.codes_entry.get().split(",") if c.strip()]
        lhb_all = DataFetcher.get_lhb(start, end)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_single_stock, c, start, end, lhb_all, save_dir) for c in codes]
            for f in as_completed(futures):
                if self.stop_requested: break
                f.result()
        self.task_queue.put(("finish", True))

    def _process_single_stock(self, code, start, end, lhb_all, save_dir):
        if self.stop_requested: return
        name = DataFetcher.get_stock_name(code)
        price_df = DataFetcher.get_price_history(code, start, end)
        fund_df = DataFetcher.get_fund_flow(code)
        rt_price = DataFetcher.get_realtime_price(code)
        self.task_queue.put(("single_done", name, code, price_df, fund_df, None, rt_price))

    def _process_lhb_scan(self, start, end, save_dir):
        df = DataFetcher.get_lhb(start, end)
        if not df.empty:
            summary, _ = DataFetcher.build_lhb_summary(df)
            realtime = DataFetcher.get_all_realtime_spot()
            summary = DataFetcher.merge_realtime_to_summary(summary, realtime, LHB_CODE_COL)
            self.task_queue.put(("scan_done", summary))
        self.task_queue.put(("finish", True))

    def _poll_queue(self) -> None:
        if not self.running: return
        try:
            while True:
                msg = self.task_queue.get_nowait()
                cmd = msg[0]
                if cmd == "single_done":
                    self._create_single_tab(f"{msg[1]}({msg[2]})", msg[3], msg[4])
                elif cmd == "scan_done":
                    self._show_scan_result(msg[1])
                elif cmd == "finish":
                    self._finish_task(msg[1])
                    break
                elif cmd == "fatal":
                    messagebox.showerror("错误", msg[1])
                    self._finish_task(False)
                    break
        except queue.Empty:
            pass
        self.root.after(300, self._poll_queue)

    def _create_single_tab(self, title, price_df, fund_df):
        tab = ttk.Frame(self.notebook_single)
        self.notebook_single.add(tab, text=title)
        scroll = ScrollableFrame(tab)
        scroll.pack(fill="both", expand=True)
        if not price_df.empty:
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.plot(price_df["日期"], price_df["收盘"])
            ax.set_title(f"价格 - {title}")
            canvas = FigureCanvasTkAgg(fig, master=scroll.scrollable_frame)
            canvas.draw()
            canvas.get_tk_widget().pack()
            self.canvases.append(canvas)

    def _show_scan_result(self, df):
        self.tree["columns"] = list(df.columns)
        for col in df.columns:
            self.tree.heading(col, text=col)
        for _, row in df.iterrows():
            self.tree.insert("", "end", values=list(row))

    def _finish_task(self, success):
        self.running = False
        self.progress.stop()
        self.progress.pack_forget()
        self.fetch_button.config(state="normal")
        self.stop_button.config(state="disabled")
        self.log("任务已结束")

    # 辅助方法...
    def import_codes_from_file(self): pass

if __name__ == "__main__":
    root = tk.Tk()
    app = StockAnalyzerApp(root)
    root.mainloop()