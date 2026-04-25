# -*- coding: utf-8 -*-
"""
股票分析工具 v4.0 - 重构版
功能：
  1. 个股分析：历史价格 + 资金流 + 龙虎榜 + 实时报价 + 主力大额流入筛选
  2. 龙虎榜扫描：全市场扫描 + 汇总统计 + 增强实时行情（最新价/涨跌幅/换手率/量比/振幅）+ 所属行业

重构要点：
  - DataFetcher：将所有数据获取/处理逻辑从 UI 类中剥离
  - 常量集中定义，消除魔法字符串
  - UI 构建细粒度拆解，每个区域独立方法
  - 统一错误处理，消除裸 except
  - 关键方法添加类型注解
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
# 常量
# ─────────────────────────────────────────────

APP_TITLE   = "股票分析工具 v4.0 - 多线程 + 增强实时行情 + 所属行业"
APP_GEOMETRY = "1300x980"
APP_MINSIZE  = (1200, 880)

DEFAULT_START_DATE = "20240101"
MAX_WORKERS        = 8          # 个股并行线程数
BIG_INFLOW_THRESH  = 50_000_000 # 主力大额流入阈值（元）
API_SLEEP          = 0.08       # 接口调用间隔（秒）
STOCK_SLEEP        = 0.2        # 个股处理后间隔（秒）

# 龙虎榜原始列名
LHB_CODE_COL     = "代码"
LHB_NAME_COL     = "名称"
LHB_DATE_COL     = "上榜日"
LHB_BUY_COL      = "龙虎榜净买额"
LHB_TURNOVER_COL = "换手率"

# 实时行情列
REALTIME_COLS = ["最新价", "涨跌幅", "换手率", "量比", "振幅"]

# 导入文件中的候选列名
IMPORT_CODE_CANDIDATES = ["代码", "code", "股票代码", "symbol", "Symbol", "Code", "证券代码"]
IMPORT_NAME_CANDIDATES = ["名称", "股票简称", "name", "简称", "Name", "证券简称"]

DISPLAY_CODE_LIMIT = 500   # 代码框最多显示数量


# ─────────────────────────────────────────────
# 可复用组件
# ─────────────────────────────────────────────

class ScrollableFrame(ttk.Frame):
    """带垂直滚动条的容器 Frame。"""

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
# 数据获取层
# ─────────────────────────────────────────────

class DataFetcher:
    """封装所有 akshare 接口调用，与 UI 完全解耦。"""

    @staticmethod
    def get_stock_name(code: str) -> str:
        """获取股票简称，失败时返回 code 本身。"""
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
        """获取股票所属行业，失败返回 '获取失败'。"""
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
    def get_price_history(
        code: str,
        start: str,
        end: str,
    ) -> pd.DataFrame:
        """拉取前复权日线数据，失败返回空 DataFrame。"""
        try:
            df = ak.stock_zh_a_hist(
                symbol=code, period="daily",
                start_date=start, end_date=end, adjust="qfq"
            )
            if not df.empty:
                df["日期"] = pd.to_datetime(df["日期"])
            return df
        except Exception:
            return pd.DataFrame()

    @staticmethod
    def get_fund_flow(code: str) -> pd.DataFrame:
        """拉取个股资金流向，失败返回空 DataFrame。"""
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
        """获取单只股票实时最新价，失败返回 None。"""
        try:
            df = ak.stock_bid_ask_em(symbol=code)
            if not df.empty and "最新价" in df.columns:
                return float(df["最新价"].iloc[0])
        except Exception:
            pass
        return None

    @staticmethod
    def get_all_realtime_spot() -> pd.DataFrame:
        """批量获取全市场实时行情，失败返回空 DataFrame。"""
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
        """拉取龙虎榜明细，失败返回空 DataFrame。"""
        try:
            return ak.stock_lhb_detail_em(start_date=start, end_date=end)
        except Exception:
            return pd.DataFrame()

    # ── 龙虎榜汇总 ────────────────────────────

    @staticmethod
    def build_lhb_summary(lhb_df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
        """对龙虎榜明细做 groupby 汇总。"""
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
    def merge_realtime_to_summary(
        summary: pd.DataFrame,
        realtime_df: pd.DataFrame,
        code_col: str,
    ) -> pd.DataFrame:
        """将全市场实时行情 merge 到 summary 中。"""
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
# 主应用（UI 层）
# ─────────────────────────────────────────────

class StockAnalyzerApp:
    """股票分析工具主窗口，负责 UI 交互与任务调度。"""

    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(APP_TITLE)
        self.root.geometry(APP_GEOMETRY)
        self.root.minsize(*APP_MINSIZE)

        self._setup_style()

        # 状态变量
        self.task_queue: queue.Queue = queue.Queue()
        self.running: bool = False
        self.stop_requested: bool = False
        self.canvases: list = []
        self.current_mode: str = "single"

        # 构建 UI
        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        root.rowconfigure(0, weight=1)
        root.columnconfigure(0, weight=1)

        self._build_top_bar(main_frame)
        self._build_content_notebook(main_frame)
        main_frame.rowconfigure(1, weight=1)

        self.switch_mode()

    # ── 样式 ──────────────────────────────────

    def _setup_style(self) -> None:
        style = ttk.Style()
        style.configure("Big.TButton", font=("Segoe UI", 11, "bold"), padding=10)

    # ── UI 构建 ───────────────────────────────

    def _build_top_bar(self, parent: ttk.Frame) -> None:
        """顶部公共控制栏：日期、模式、按钮、进度条。"""
        top = ttk.Frame(parent)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        self._build_date_frame(top)
        self._build_mode_frame(top)
        self._build_button_frame(top)

        self.progress = ttk.Progressbar(top, mode="indeterminate", length=200)
        self.progress.pack(side="right", padx=10)
        self.progress.pack_forget()

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
            ttk.Radiobutton(
                frame, text=label, variable=self.mode_var,
                value=value, command=self.switch_mode
            ).pack(side="left", padx=10)

    def _build_button_frame(self, parent: ttk.Frame) -> None:
        frame = ttk.Frame(parent)
        frame.pack(side="right", padx=10)

        self.fetch_button = ttk.Button(
            frame, text="开始执行", command=self.start_task, style="Big.TButton"
        )
        self.fetch_button.pack(side="left", padx=5)

        self.stop_button = ttk.Button(
            frame, text="停止", command=self.request_stop, state="disabled"
        )
        self.stop_button.pack(side="left", padx=5)

    def _build_content_notebook(self, parent: ttk.Frame) -> None:
        self.content_notebook = ttk.Notebook(parent)
        self.content_notebook.grid(row=1, column=0, sticky="nsew")

        self.tab_single = ttk.Frame(self.content_notebook)
        self.tab_scan   = ttk.Frame(self.content_notebook)
        self.content_notebook.add(self.tab_single, text="个股分析")
        self.content_notebook.add(self.tab_scan,   text="龙虎榜扫描")

        self._build_single_tab()
        self._build_scan_tab()

    def _build_single_tab(self) -> None:
        """构建个股分析 Tab 内容。"""
        input_frame = ttk.LabelFrame(
            self.tab_single,
            text="股票代码（英文逗号分隔，或从文件导入）",
            padding=10
        )
        input_frame.pack(fill="x", pady=5)

        self.codes_entry = ttk.Entry(input_frame, font=("Segoe UI", 10))
        self.codes_entry.pack(fill="x", pady=5)
        self.codes_entry.insert(0, "600519,000001,300750")

        btn_bar = ttk.Frame(input_frame)
        btn_bar.pack(fill="x", pady=(8, 2))

        ttk.Button(btn_bar, text="从文件导入代码",
                   command=self.import_codes_from_file).pack(side="left", padx=5)
        ttk.Button(btn_bar, text="下载导入模板",
                   command=self.save_import_template).pack(side="left", padx=5)

        # 已修复：使用单引号包裹文本，避免双引号语法冲突
        ttk.Label(
            btn_bar,
            text='支持格式：每行一个代码的txt，或带"代码"列（可选"名称"列）的csv/xlsx',
            foreground="gray",
            font=("Segoe UI", 9)
        ).pack(side="left", padx=15)

        # 日志
        self.output_text_single = self._build_log_widget(self.tab_single, "日志")

        # 图表区
        chart_frame = ttk.LabelFrame(self.tab_single, text="图表", padding=5)
        chart_frame.pack(fill="both", expand=True, pady=5)
        self.notebook_single = ttk.Notebook(chart_frame)
        self.notebook_single.pack(fill="both", expand=True)

    def _build_scan_tab(self) -> None:
        """构建龙虎榜扫描 Tab 内容。"""
        self.output_text_scan = self._build_log_widget(self.tab_scan, "扫描日志")

        # 过滤条件
        filter_frame = ttk.LabelFrame(self.tab_scan, text="过滤条件（可选）", padding=8)
        filter_frame.pack(fill="x", pady=5)
        ttk.Label(filter_frame, text="最低平均上榜换手率(%)：").pack(side="left", padx=(0, 8))
        self.turnover_threshold_entry = ttk.Entry(filter_frame, width=8, font=("Segoe UI", 10))
        self.turnover_threshold_entry.pack(side="left")
        ttk.Label(filter_frame, text="  （留空表示不过滤）",
                  foreground="gray").pack(side="left", padx=10)

        # 结果表格
        result_frame = ttk.LabelFrame(
            self.tab_scan, text="扫描结果（含实时行情 + 所属行业）", padding=5
        )
        result_frame.pack(fill="both", expand=True, pady=5)

        tree_scroll = ttk.Scrollbar(result_frame, orient="vertical")
        tree_scroll.pack(side="right", fill="y")
        self.tree = ttk.Treeview(result_frame, show="headings",
                                 yscrollcommand=tree_scroll.set)
        self.tree.pack(fill="both", expand=True)
        tree_scroll.config(command=self.tree.yview)

    @staticmethod
    def _build_log_widget(parent: ttk.Frame, label: str) -> tk.Text:
        """创建带滚动条的日志文本框。"""
        frame = ttk.LabelFrame(parent, text=label, padding=5)
        frame.pack(fill="x", pady=5)

        text = tk.Text(frame, height=6, font=("Consolas", 10),
                       state="normal", wrap="word", bg="#fdfdfd")
        text.pack(side="left", fill="both", expand=True)
        sb = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        sb.pack(side="right", fill="y")
        text.configure(yscrollcommand=sb.set)
        return text

    # ── 模式切换 ──────────────────────────────

    def switch_mode(self) -> None:
        self.current_mode = self.mode_var.get()
        if self.current_mode == "single":
            self.codes_entry.configure(state="normal")
            self.content_notebook.select(0)
        else:
            self.codes_entry.configure(state="disabled")
            self.content_notebook.select(1)

    # ── 日志输出 ──────────────────────────────

    def log(self, message: str, mode: Optional[str] = None) -> None:
        if mode is None:
            mode = self.current_mode
        widget = self.output_text_single if mode == "single" else self.output_text_scan
        widget.insert(tk.END, message + "\n")
        widget.see(tk.END)
        self.root.update_idletasks()

    # ── 文件导入 ──────────────────────────────

    def import_codes_from_file(self) -> None:
        path = filedialog.askopenfilename(
            title="选择股票代码文件",
            filetypes=[
                ("文本文件", "*.txt"), ("CSV文件", "*.csv"),
                ("Excel文件", "*.xlsx *.xls"), ("所有文件", "*.*"),
            ]
        )
        if not path:
            return

        codes: list[str] = []
        names_map: dict[str, str] = {}

        try:
            ext = os.path.splitext(path)[1].lower()

            if ext == ".txt":
                codes, names_map = self._parse_txt_codes(path)
            elif ext in (".csv", ".xlsx", ".xls"):
                codes, names_map = self._parse_table_codes(path, ext)
            else:
                messagebox.showwarning("格式不支持", f"不支持的文件格式：{ext}")
                return

            if not codes:
                messagebox.showinfo("提示", "文件中未读取到任何有效股票代码")
                return

            codes = sorted(set(codes))
            self.codes_entry.delete(0, tk.END)
            self.codes_entry.insert(0, ",".join(codes[:DISPLAY_CODE_LIMIT]))

            msg = f"成功导入 {len(codes)} 个有效代码"
            if names_map:
                msg += f"（其中 {len(names_map)} 个带有名称）"
            if len(codes) > DISPLAY_CODE_LIMIT:
                msg += f"\n（输入框仅显示前 {DISPLAY_CODE_LIMIT} 个，程序会处理全部）"

            messagebox.showinfo("导入完成", msg)
            self.log(f"从文件导入 {len(codes)} 个代码", "single")

        except Exception as e:
            messagebox.showerror("导入失败", f"读取文件出错：\n{e}")

    @staticmethod
    def _parse_txt_codes(path: str) -> tuple[list[str], dict[str, str]]:
        codes, names_map = [], {}
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith(("#", ";", "//")):
                    continue
                parts = [p.strip() for p in line.replace(",", " ").split() if p.strip()]
                if not parts:
                    continue
                code = parts[0]
                if code.isdigit() and len(code) in (6, 8):
                    codes.append(code)
                    if len(parts) >= 2:
                        names_map[code] = " ".join(parts[1:])
        return codes, names_map

    @staticmethod
    def _parse_table_codes(
        path: str, ext: str
    ) -> tuple[list[str], dict[str, str]]:
        df = (
            pd.read_csv(path, dtype=str, encoding="utf-8-sig", encoding_errors="replace")
            if ext == ".csv"
            else pd.read_excel(path, dtype=str)
        )
        df.columns = df.columns.str.strip()

        code_col = next(
            (c for c in IMPORT_CODE_CANDIDATES if c in df.columns), None
        )
        if not code_col:
            messagebox.showwarning("格式问题", "文件中未找到'代码'或'股票代码'列")
            return [], {}

        codes_series = df[code_col].dropna().astype(str).str.strip()
        codes = [c for c in codes_series if c.isdigit() and len(c) in (6, 8)]

        name_col = next(
            (c for c in IMPORT_NAME_CANDIDATES if c in df.columns), None
        )
        names_map: dict[str, str] = {}
        if name_col:
            for _, row in df.iterrows():
                code = str(row[code_col]).strip()
                name = str(row[name_col]).strip()
                if code in codes and name and name.lower() not in ("nan", ""):
                    names_map[code] = name

        return codes, names_map

    def save_import_template(self) -> None:
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel 文件", "*.xlsx"), ("CSV 文件", "*.csv")],
            title="保存股票代码导入模板"
        )
        if not path:
            return
        try:
            template = pd.DataFrame({
                "代码": ["600519", "000001", "300750", "603259", "688981", ""],
                "名称": ["贵州茅台", "平安银行", "宁德时代", "药明康德", "中芯国际", "（可选填写）"],
            })
            if path.lower().endswith(".csv"):
                template.to_csv(path, index=False, encoding="utf-8-sig")
            else:
                template.to_excel(path, index=False)
            messagebox.showinfo("完成", f"模板已保存至：\n{path}")
        except Exception as e:
            messagebox.showerror("保存失败", str(e))

    # ── 任务控制 ──────────────────────────────

    def start_task(self) -> None:
        if self.running:
            messagebox.showinfo("提示", "已有任务正在执行...")
            return

        start_date = self.start_date_entry.get().strip()
        end_date   = self.end_date_entry.get().strip()
        if not self._validate_dates(start_date, end_date):
            return

        save_dir = filedialog.askdirectory(title="请选择保存文件夹")
        if not save_dir:
            self.log("操作已取消")
            return

        self._reset_ui_for_task()

        thread = threading.Thread(
            target=self._worker,
            args=(start_date, end_date, save_dir),
            daemon=True
        )
        thread.start()
        self.root.after(400, self._poll_queue)

    def _validate_dates(self, start: str, end: str) -> bool:
        try:
            datetime.strptime(start, "%Y%m%d")
            datetime.strptime(end, "%Y%m%d")
            return True
        except ValueError:
            messagebox.showerror("错误", "日期格式必须为 YYYYMMDD")
            return False

    def _reset_ui_for_task(self) -> None:
        self.running = True
        self.stop_requested = False
        self.fetch_button.config(state="disabled")
        self.stop_button.config(state="normal")
        self.progress.pack(side="right", padx=10)
        self.progress.start(12)

        if self.current_mode == "single":
            self.output_text_single.delete(1.0, tk.END)
            for tab in self.notebook_single.tabs():
                self.notebook_single.forget(tab)
            for c in self.canvases:
                try:
                    c.get_tk_widget().destroy()
                except Exception:
                    pass
            self.canvases.clear()
            self.log("个股分析任务启动（多线程 + 断点续传）...", "single")
        else:
            self.output_text_scan.delete(1.0, tk.END)
            for item in self.tree.get_children():
                self.tree.delete(item)
            self.log("龙虎榜扫描任务启动（增强实时行情 + 所属行业）...", "scan")

    def request_stop(self) -> None:
        if not self.running:
            return
        self.stop_requested = True
        self.log("\n→ 用户请求停止，正在等待当前操作完成...\n")
        self.stop_button.config(state="disabled")
        self.progress.stop()
        self.progress.configure(mode="determinate")
        self.progress["value"] = 50

    # ── 后台 Worker ───────────────────────────

    def _worker(self, start: str, end: str, save_dir: str) -> None:
        try:
            if self.current_mode == "single":
                self._run_single_mode(start, end, save_dir)
            else:
                self._process_lhb_scan(start, end, save_dir)
        except Exception as e:
            self.task_queue.put(("fatal", str(e)))

    def _run_single_mode(self, start: str, end: str, save_dir: str) -> None:
        codes_str = self.codes_entry.get().strip()
        if not codes_str:
            self.task_queue.put(("log", "请至少输入一个股票代码", "single"))
            self.task_queue.put(("finish", False))
            return

        codes = [c.strip() for c in codes_str.split(",") if c.strip()]
        lhb_all = DataFetcher.get_lhb(start, end)

        self.task_queue.put((
            "log",
            f"启动 {MAX_WORKERS} 线程并行处理 {len(codes)} 只股票（支持断点续传）...",
            "single"
        ))

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for code in codes:
                if self.stop_requested:
                    break
                futures.append(executor.submit(
                    self._process_single_stock, code, start, end, lhb_all, save_dir
                ))
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    self.task_queue.put(("log", f"线程异常: {str(e)[:150]}", "single"))

        if not self.stop_requested:
            self.task_queue.put(("finish", True))

    def _process_single_stock(
        self,
        code: str,
        start: str,
        end: str,
        lhb_all: pd.DataFrame,
        save_dir: str,
    ) -> None:
        if self.stop_requested:
            return

        try:
            self.task_queue.put(("log", f"\n▶ 处理 {code} ...", "single"))

            stock_name = DataFetcher.get_stock_name(code)
            self.task_queue.put(("log", f"  名称：{stock_name}", "single"))

            excel_path = os.path.join(save_dir, f"{stock_name}_{code}.xlsx")

            # 断点续传
            existing_price, existing_max_date = self._load_existing_price(excel_path)
            fetch_start = self._calc_fetch_start(start, existing_max_date)

            price_df_new = pd.DataFrame()
            if fetch_start <= end:
                price_df_new = DataFetcher.get_price_history(code, fetch_start, end)
                if not price_df_new.empty:
                    self.task_queue.put(("log", f"  新增价格数据：{len(price_df_new)} 条", "single"))
                else:
                    self.task_queue.put(("log", "  价格拉取无数据", "single"))

            price_df = self._merge_price(existing_price, price_df_new)
            fund_df = DataFetcher.get_fund_flow(code)
            game_df = self._filter_lhb(lhb_all, code)
            rt_price = DataFetcher.get_realtime_price(code)

            price_str = f"{rt_price:.2f}" if rt_price is not None else "N/A"
            self.task_queue.put(("log", f"  实时最新价：{price_str}", "single"))

            self._save_single_excel(
                excel_path, code, stock_name, start, end,
                price_df, fund_df, game_df, rt_price
            )

            self.task_queue.put(("log", f"  已保存/更新：{excel_path}", "single"))
            self.task_queue.put((
                "single_done", stock_name, code, price_df, fund_df, game_df, rt_price
            ))
            time.sleep(STOCK_SLEEP)

        except Exception:
            self.task_queue.put((
                "log", f"处理 {code} 失败：{traceback.format_exc(limit=2)}", "single"
            ))

    def _load_existing_price(
        self, excel_path: str
    ) -> tuple[pd.DataFrame, Optional[pd.Timestamp]]:
        if not os.path.exists(excel_path):
            return pd.DataFrame(), None
        try:
            with pd.ExcelFile(excel_path) as xls:
                if "历史价格" not in xls.sheet_names:
                    return pd.DataFrame(), None
                df = pd.read_excel(xls, "历史价格")
                if "日期" not in df.columns:
                    return df, None
                df["日期"] = pd.to_datetime(df["日期"])
                max_date = df["日期"].max()
                self.task_queue.put((
                    "log", f"  已有历史文件 → 最后日期 {max_date.date()}", "single"
                ))
                return df, max_date
        except Exception as e:
            self.task_queue.put((
                "log", f"  读取历史文件失败（将全量更新）: {e}", "single"
            ))
            return pd.DataFrame(), None

    def _calc_fetch_start(
        self, default_start: str, existing_max: Optional[pd.Timestamp]
    ) -> str:
        if existing_max is None:
            return default_start
        next_day = (existing_max + pd.Timedelta(days=1)).strftime("%Y%m%d")
        if next_day > default_start:
            self.task_queue.put((
                "log", f"  增量拉取 {next_day} ~ ...", "single"
            ))
            return next_day
        return default_start

    @staticmethod
    def _merge_price(
        existing: pd.DataFrame, new: pd.DataFrame
    ) -> pd.DataFrame:
        if not existing.empty and not new.empty:
            df = pd.concat([existing, new], ignore_index=True)
            return df.drop_duplicates(subset=["日期"]).sort_values("日期").reset_index(drop=True)
        return new if not new.empty else existing

    @staticmethod
    def _filter_lhb(lhb_all: pd.DataFrame, code: str) -> pd.DataFrame:
        if lhb_all.empty:
            return pd.DataFrame()
        df = lhb_all[lhb_all["代码"] == code].copy()
        if not df.empty and "上榜日" in df.columns:
            df["上榜日"] = pd.to_datetime(df["上榜日"])
        return df

    def _save_single_excel(
        self,
        excel_path: str,
        code: str,
        stock_name: str,
        start: str,
        end: str,
        price_df: pd.DataFrame,
        fund_df: pd.DataFrame,
        game_df: pd.DataFrame,
        rt_price: Optional[float],
    ) -> None:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            if not price_df.empty:
                price_df.to_excel(writer, "历史价格", index=False)
            if not fund_df.empty:
                fund_df.to_excel(writer, "资金流向", index=False)
            if not game_df.empty:
                game_df.to_excel(writer, "龙虎榜", index=False)

            pd.DataFrame({
                "股票代码": [code],
                "股票名称": [stock_name],
                "实时最新价": [rt_price if rt_price is not None else "N/A"],
                "分析日期范围": [f"{start} ~ {end}"],
                "最后更新时间": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            }).to_excel(writer, "实时与汇总", index=False)

            if not fund_df.empty and "主力净额" in fund_df.columns:
                fund_numeric = pd.to_numeric(fund_df["主力净额"], errors="coerce")
                big_inflow = fund_df[fund_numeric > BIG_INFLOW_THRESH].copy()
                if not big_inflow.empty:
                    big_inflow.to_excel(writer, "主力大额流入日 (>5000万)", index=False)
                    self.task_queue.put((
                        "log",
                        f"  ★ 发现 {len(big_inflow)} 天主力净流入 > 5000万元",
                        "single"
                    ))

    # ── 龙虎榜扫描 ────────────────────────────

    def _process_lhb_scan(self, start: str, end: str, save_dir: str) -> None:
        save_path = os.path.join(
            save_dir, f"龙虎榜扫描_{start}_{end}_含实时行情_所属行业.xlsx"
        )

        if os.path.exists(save_path):
            try:
                df = pd.read_excel(save_path)
                self.task_queue.put(("log", f"检测到已有扫描结果，直接加载（{len(df)} 条）", "scan"))
                self.task_queue.put(("scan_done", df))
                return
            except Exception:
                self.task_queue.put(("log", "历史扫描文件损坏，将重新扫描", "scan"))

        self.task_queue.put(("log", f"扫描 {start} ~ {end} 的龙虎榜...", "scan"))

        lhb_df = DataFetcher.get_lhb(start, end)
        if lhb_df.empty:
            self.task_queue.put(("log", "无数据或获取失败", "scan"))
            self.task_queue.put(("scan_done", pd.DataFrame()))
            return

        required = [LHB_CODE_COL, LHB_NAME_COL, LHB_DATE_COL, LHB_BUY_COL]
        missing = [c for c in required if c not in lhb_df.columns]
        if missing:
            self.task_queue.put((
                "log", f"缺少关键列：{', '.join(missing)} → 无法汇总", "scan"
            ))
            self.task_queue.put(("scan_done", pd.DataFrame()))
            return

        has_turnover = LHB_TURNOVER_COL in lhb_df.columns
        if not has_turnover:
            self.task_queue.put(("log", "警告：未找到'换手率'列，将不进行换手率过滤", "scan"))

        summary, _ = DataFetcher.build_lhb_summary(lhb_df)
        summary = self._apply_turnover_filter(summary, has_turnover)
        summary = summary.sort_values("上榜次数", ascending=False).reset_index(drop=True)

        self.task_queue.put((
            "log", "正在获取全市场实时行情（最新价 + 涨跌幅 + 换手率 + 量比 + 振幅）...", "scan"
        ))
        realtime_df = DataFetcher.get_all_realtime_spot()
        if not realtime_df.empty:
            self.task_queue.put(("log", f"成功获取 {len(realtime_df)} 只股票实时行情", "scan"))
        else:
            self.task_queue.put(("log", "增强实时行情获取失败，标记为 N/A", "scan"))

        summary = DataFetcher.merge_realtime_to_summary(summary, realtime_df, LHB_CODE_COL)

        summary = self._enrich_industry(summary)

        summary.to_excel(save_path, index=False)
        self.task_queue.put(("log", f"结果已保存：{save_path}", "scan"))
        self.task_queue.put(("scan_done", summary))

    def _apply_turnover_filter(
        self, summary: pd.DataFrame, has_turnover: bool
    ) -> pd.DataFrame:
        threshold_str = self.turnover_threshold_entry.get().strip()
        if not (has_turnover and threshold_str):
            return summary
        try:
            threshold = float(threshold_str)
            if threshold > 0:
                before = len(summary)
                summary = summary[
                    summary["平均上榜换手率(%)"].notna() &
                    (summary["平均上榜换手率(%)"] >= threshold)
                ].reset_index(drop=True)
                self.task_queue.put((
                    "log",
                    f"按平均换手率 ≥ {threshold}% 过滤：{before} → {len(summary)} 只",
                    "scan"
                ))
        except ValueError:
            self.task_queue.put((
                "log", f"换手率阈值 '{threshold_str}' 格式错误，已忽略过滤", "scan"
            ))
        return summary

    def _enrich_industry(self, summary: pd.DataFrame) -> pd.DataFrame:
        """为 summary 中每只股票查询所属行业（串行，带间隔）。"""
        self.task_queue.put(("log", "正在获取各股票所属行业信息（约需几秒）...", "scan"))
        industry_dict: dict[str, str] = {}

        for code in summary[LHB_CODE_COL].tolist():
            if self.stop_requested:
                self.task_queue.put(("log", "用户请求停止，行业信息获取中断", "scan"))
                break
            industry_dict[code] = DataFetcher.get_stock_industry(code)
            time.sleep(API_SLEEP)

        summary["所属行业"] = summary[LHB_CODE_COL].map(industry_dict).fillna("N/A")
        return summary

    # ── 消息队列轮询 ──────────────────────────

    def _poll_queue(self) -> None:
        if not self.running:
            return

        try:
            while True:
                msg = self.task_queue.get_nowait()
                cmd = msg[0]

                if cmd == "log":
                    mode = msg[2] if len(msg) > 2 else self.current_mode
                    self.log(msg[1], mode)

                elif cmd == "single_done":
                    _, name, code, price, fund, game, rt_price = msg
                    title = f"{name} ({code})"
                    if rt_price is not None:
                        title += f" - {rt_price:.2f}"
                    self._create_single_tab(title, price, fund)

                elif cmd == "scan_done":
                    self._show_scan_result(msg[1])

                elif cmd == "finish":
                    self._finish_task(success=msg[1])

                elif cmd == "fatal":
                    self.log(f"重大错误：{msg[1]}")
                    self._finish_task(success=False)

        except queue.Empty:
            pass

        self.root.after(300, self._poll_queue)

    # ── UI 更新 ───────────────────────────────

    def _create_single_tab(
        self,
        title: str,
        price_df: pd.DataFrame,
        fund_df: pd.DataFrame,
    ) -> None:
        tab = ttk.Frame(self.notebook_single)
        self.notebook_single.add(tab, text=title)
        scroll = ScrollableFrame(tab)
        scroll.pack(fill="both", expand=True)
        frame = scroll.scrollable_frame

        if not price_df.empty:
            fig, ax = plt.subplots(figsize=(10, 4.5))
            ax.plot(price_df["日期"], price_df["收盘"], color="#1f77b4", lw=1.8)
            ax.set_title(f"收盘价走势 - {title}")
            ax.grid(True, ls="--", alpha=0.4)
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            self._embed_figure(fig, frame)

        if not fund_df.empty and "主力净额" in fund_df.columns:
            fig, ax = plt.subplots(figsize=(10, 4.5))
            values = pd.to_numeric(fund_df["主力净额"], errors="coerce").fillna(0)
            colors = ["#2ca02c" if v >= 0 else "#d62728" for v in values]
            ax.bar(fund_df["日期"], values, color=colors, alpha=0.85)
            ax.set_title(f"主力资金净额 - {title}")
            ax.grid(True, axis="y", ls="--", alpha=0.4)
            ax.tick_params(axis="x", rotation=35)
            fig.tight_layout()
            self._embed_figure(fig, frame)

    def _embed_figure(self, fig: plt.Figure, parent: tk.Widget) -> None:
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(
            side=tk.TOP, fill=tk.BOTH, expand=True, pady=8, padx=8
        )
        self.canvases.append(canvas)

    def _show_scan_result(self, df: pd.DataFrame) -> None:
        if df.empty:
            self.log("无符合条件的上榜股票", "scan")
            return

        for item in self.tree.get_children():
            self.tree.delete(item)

        columns = list(df.columns)
        self.tree["columns"] = columns
        self.tree["show"] = "headings"

        col_widths = {
            "上榜日换手率列表": 160, "简称": 160,
            "平均上榜换手率(%)": 130, "所属行业": 140,
            "涨跌幅": 90, "换手率": 90, "量比": 90, "振幅": 90,
        }
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=col_widths.get(col, 110), anchor="center")

        for _, row in df.iterrows():
            values = [
                str(row[col]) if pd.notna(row[col]) else "" for col in columns
            ]
            self.tree.insert("", "end", values=values)

        self.log(f"找到 {len(df)} 只符合条件的上榜股票（含实时行情 + 所属行业）", "scan")

    def _finish_task(self, success: bool = True) -> None:
        self.progress.stop()
        self.progress.pack_forget()
        self.fetch_button.config(state="normal")
        self.stop_button.config(state="disabled")
        self.running = False

        mode_text = "个股分析" if self.current_mode == "single" else "龙虎榜扫描"
        if success:
            self.log(f"\n=== {mode_text} 完成 ===\n", self.current_mode)
            messagebox.showinfo("完成", f"{mode_text} 已完成")
        else:
            self.log(f"\n=== {mode_text} 中断或出错 ===\n", self.current_mode)
            messagebox.showwarning("提示", f"{mode_text} 未正常完成")


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────

if __name__ == "__main__":
    root = tk.Tk()
    app = StockAnalyzerApp(root)
    root.mainloop()