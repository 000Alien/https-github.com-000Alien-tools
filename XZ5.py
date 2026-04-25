# -*- coding: utf-8 -*-
"""
股票分析工具 v2 - 含实时股价 + 批量导入代码 + 龙虎榜扫描增加换手率统计 + 换手率阈值过滤
功能：
1. 个股分析：历史价格 + 资金流 + 龙虎榜 + 实时报价
2. 龙虎榜扫描：全市场扫描 + 汇总统计 + 实时最新价 + 上榜换手率信息 + 可设置最低平均换手率过滤
支持停止任务
"""

import akshare as ak
import pandas as pd
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import threading
import queue
import traceback
import time
import os


class ScrollableFrame(ttk.Frame):
    def __init__(self, container, *args, **kwargs):
        super().__init__(container, *args, **kwargs)
        canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        scrollable_frame = ttk.Frame(canvas)

        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        self.scrollable_frame = scrollable_frame
        self.canvas = canvas


class StockAnalyzerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("股票分析工具 v2 - 含实时股价 + 批量导入 + 换手率过滤")
        self.root.geometry("1200x950")
        self.root.minsize(1100, 850)

        style = ttk.Style()
        style.configure("Big.TButton", font=("Segoe UI", 11, "bold"), padding=10)

        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        # 顶部公共区域
        top_frame = ttk.Frame(main_frame)
        top_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))

        date_frame = ttk.LabelFrame(top_frame, text="日期范围", padding=8)
        date_frame.pack(side="left", padx=10)

        ttk.Label(date_frame, text="开始:").pack(side="left", padx=(0, 4))
        self.start_date_entry = ttk.Entry(date_frame, width=12, font=("Segoe UI", 10))
        self.start_date_entry.pack(side="left", padx=4)
        self.start_date_entry.insert(0, "20240101")

        ttk.Label(date_frame, text="结束:").pack(side="left", padx=(10, 4))
        self.end_date_entry = ttk.Entry(date_frame, width=12, font=("Segoe UI", 10))
        self.end_date_entry.pack(side="left", padx=4)
        today = datetime.today().strftime("%Y%m%d")
        self.end_date_entry.insert(0, today)

        mode_frame = ttk.LabelFrame(top_frame, text="分析模式", padding=8)
        mode_frame.pack(side="left", padx=10)

        self.mode_var = tk.StringVar(value="single")
        ttk.Radiobutton(mode_frame, text="个股分析", variable=self.mode_var,
                        value="single", command=self.switch_mode).pack(side="left", padx=10)
        ttk.Radiobutton(mode_frame, text="龙虎榜扫描", variable=self.mode_var,
                        value="scan", command=self.switch_mode).pack(side="left", padx=10)

        btn_frame = ttk.Frame(top_frame)
        btn_frame.pack(side="right", padx=10)

        self.fetch_button = ttk.Button(btn_frame, text="开始执行", command=self.start_task,
                                       style="Big.TButton")
        self.fetch_button.pack(side="left", padx=5)

        self.stop_button = ttk.Button(btn_frame, text="停止", command=self.request_stop,
                                      state="disabled")
        self.stop_button.pack(side="left", padx=5)

        self.progress = ttk.Progressbar(top_frame, mode='indeterminate', length=200)
        self.progress.pack(side="right", padx=10)
        self.progress.pack_forget()

        # 主内容 Notebook
        self.content_notebook = ttk.Notebook(main_frame)
        self.content_notebook.grid(row=1, column=0, sticky="nsew")
        main_frame.rowconfigure(1, weight=1)

        # 创建两个 tab
        self.tab_single = ttk.Frame(self.content_notebook)
        self.tab_scan   = ttk.Frame(self.content_notebook)

        self.content_notebook.add(self.tab_single, text="个股分析")
        self.content_notebook.add(self.tab_scan,   text="龙虎榜扫描")

        # ================= 个股分析 Tab 布局 =================
        single_input_frame = ttk.LabelFrame(self.tab_single, text="股票代码（英文逗号分隔，或从文件导入）", padding=10)
        single_input_frame.pack(fill="x", pady=5)

        self.codes_entry = ttk.Entry(single_input_frame, font=("Segoe UI", 10))
        self.codes_entry.pack(fill="x", pady=5)
        self.codes_entry.insert(0, "600519,000001,300750")

        import_btn_frame = ttk.Frame(single_input_frame)
        import_btn_frame.pack(fill="x", pady=(8, 2))

        ttk.Button(import_btn_frame, text="从文件导入代码", command=self.import_codes_from_file).pack(side="left", padx=5)
        ttk.Button(import_btn_frame, text="下载导入模板", command=self.save_import_template).pack(side="left", padx=5)

        ttk.Label(import_btn_frame,
                  text="支持格式：每行一个代码的txt，或带“代码”列（可选“名称”列）的csv/xlsx",
                  foreground="gray", font=("Segoe UI", 9)).pack(side="left", padx=15)

        log_frame_single = ttk.LabelFrame(self.tab_single, text="日志", padding=5)
        log_frame_single.pack(fill="x", pady=5)

        self.output_text_single = tk.Text(log_frame_single, height=6, font=("Consolas", 10),
                                          state="normal", wrap="word", bg="#fdfdfd")
        self.output_text_single.pack(fill="both", expand=True)
        scrollbar_log1 = ttk.Scrollbar(log_frame_single, orient="vertical", command=self.output_text_single.yview)
        scrollbar_log1.pack(side="right", fill="y")
        self.output_text_single.configure(yscrollcommand=scrollbar_log1.set)

        chart_frame_single = ttk.LabelFrame(self.tab_single, text="图表", padding=5)
        chart_frame_single.pack(fill="both", expand=True, pady=5)

        self.notebook_single = ttk.Notebook(chart_frame_single)
        self.notebook_single.pack(fill="both", expand=True)

        # ================= 龙虎榜扫描 Tab 布局 =================
        log_frame_scan = ttk.LabelFrame(self.tab_scan, text="扫描日志", padding=5)
        log_frame_scan.pack(fill="x", pady=5)

        self.output_text_scan = tk.Text(log_frame_scan, height=6, font=("Consolas", 10),
                                        state="normal", wrap="word", bg="#fdfdfd")
        self.output_text_scan.pack(fill="both", expand=True)
        scrollbar_log2 = ttk.Scrollbar(log_frame_scan, orient="vertical", command=self.output_text_scan.yview)
        scrollbar_log2.pack(side="right", fill="y")
        self.output_text_scan.configure(yscrollcommand=scrollbar_log2.set)

        # 新增：换手率阈值过滤区域
        filter_frame = ttk.LabelFrame(self.tab_scan, text="过滤条件（可选）", padding=8)
        filter_frame.pack(fill="x", pady=5)

        ttk.Label(filter_frame, text="最低平均上榜换手率(%)：").pack(side="left", padx=(0, 8))
        self.turnover_threshold_entry = ttk.Entry(filter_frame, width=8, font=("Segoe UI", 10))
        self.turnover_threshold_entry.pack(side="left")
        self.turnover_threshold_entry.insert(0, "")  # 默认空 = 不过滤
        ttk.Label(filter_frame, text="  （留空表示不过滤）", foreground="gray").pack(side="left", padx=10)

        result_frame = ttk.LabelFrame(self.tab_scan, text="扫描结果（含实时最新价 + 换手率）", padding=5)
        result_frame.pack(fill="both", expand=True, pady=5)

        tree_scroll = ttk.Scrollbar(result_frame, orient="vertical")
        tree_scroll.pack(side="right", fill="y")

        self.tree = ttk.Treeview(result_frame, show="headings", yscrollcommand=tree_scroll.set)
        self.tree.pack(fill="both", expand=True)
        tree_scroll.config(command=self.tree.yview)

        # 变量初始化
        self.task_queue = queue.Queue()
        self.running = False
        self.stop_requested = False
        self.canvases = []
        self.current_mode = "single"

        self.switch_mode()

    def log(self, message, mode=None):
        if mode is None:
            mode = self.current_mode
        text_widget = self.output_text_single if mode == "single" else self.output_text_scan
        text_widget.insert(tk.END, message + "\n")
        text_widget.see(tk.END)
        self.root.update_idletasks()

    def switch_mode(self):
        self.current_mode = self.mode_var.get()
        if self.current_mode == "single":
            self.codes_entry.configure(state="normal")
            self.content_notebook.select(0)
        else:
            self.codes_entry.configure(state="disabled")
            self.content_notebook.select(1)

    def import_codes_from_file(self):
        file_path = filedialog.askopenfilename(
            title="选择股票代码文件",
            filetypes=[
                ("文本文件", "*.txt"),
                ("CSV文件", "*.csv"),
                ("Excel文件", "*.xlsx *.xls"),
                ("所有文件", "*.*")
            ]
        )
        if not file_path:
            return

        codes = []
        names_map = {}

        try:
            ext = os.path.splitext(file_path)[1].lower()

            if ext == '.txt':
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith(('#', ';', '//')):
                            continue
                        parts = [p.strip() for p in line.replace(',', ' ').split() if p.strip()]
                        if not parts:
                            continue
                        code = parts[0]
                        if code.isdigit() and len(code) in (6, 8):
                            codes.append(code)
                            if len(parts) >= 2:
                                names_map[code] = ' '.join(parts[1:])

            elif ext in ['.csv', '.xlsx']:
                if ext == '.csv':
                    df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig', encoding_errors='replace')
                else:
                    df = pd.read_excel(file_path, dtype=str)

                df.columns = df.columns.str.strip()

                code_col = None
                for cand in ['代码', 'code', '股票代码', 'symbol', 'Symbol', 'Code', '证券代码']:
                    if cand in df.columns:
                        code_col = cand
                        break

                if not code_col:
                    messagebox.showwarning("格式问题", "文件中未找到“代码”或“股票代码”列")
                    return

                codes_series = df[code_col].dropna().astype(str).str.strip()
                codes = [c for c in codes_series if c.isdigit() and len(c) in (6, 8)]

                name_col = None
                for cand in ['名称', '股票简称', 'name', '简称', 'Name', '证券简称']:
                    if cand in df.columns:
                        name_col = cand
                        break

                if name_col:
                    for _, row in df.iterrows():
                        code = str(row[code_col]).strip()
                        name = str(row[name_col]).strip()
                        if code in codes and name and name.lower() not in ['nan', '']:
                            names_map[code] = name

            if not codes:
                messagebox.showinfo("提示", "文件中未读取到任何有效股票代码")
                return

            codes = sorted(set(codes))

            display_limit = 500
            display_codes = codes[:display_limit]
            self.codes_entry.delete(0, tk.END)
            self.codes_entry.insert(0, ",".join(display_codes))

            msg = f"成功导入 {len(codes)} 个有效代码"
            if names_map:
                msg += f"（其中 {len(names_map)} 个带有名称）"
            if len(codes) > display_limit:
                msg += f"\n（输入框仅显示前 {display_limit} 个，程序会处理全部）"

            messagebox.showinfo("导入完成", msg)
            self.log(f"从文件导入 {len(codes)} 个代码", "single")

        except Exception as e:
            messagebox.showerror("导入失败", f"读取文件出错：\n{str(e)}")

    def save_import_template(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel 文件", "*.xlsx"), ("CSV 文件", "*.csv")],
            title="保存股票代码导入模板"
        )
        if not path:
            return

        try:
            template_data = {
                "代码": ["600519", "000001", "300750", "603259", "688981", ""],
                "名称": ["贵州茅台", "平安银行", "宁德时代", "药明康德", "中芯国际", "（可选填写）"]
            }
            df = pd.DataFrame(template_data)

            if path.lower().endswith('.csv'):
                df.to_csv(path, index=False, encoding='utf-8-sig')
            else:
                df.to_excel(path, index=False)

            messagebox.showinfo("完成", f"模板已保存至：\n{path}")
        except Exception as e:
            messagebox.showerror("保存失败", str(e))

    def start_task(self):
        if self.running:
            messagebox.showinfo("提示", "已有任务正在执行...")
            return

        start_date = self.start_date_entry.get().strip()
        end_date = self.end_date_entry.get().strip()

        try:
            datetime.strptime(start_date, "%Y%m%d")
            datetime.strptime(end_date, "%Y%m%d")
        except ValueError:
            messagebox.showerror("错误", "日期格式必须为 YYYYMMDD")
            return

        save_dir = filedialog.askdirectory(title="请选择保存文件夹")
        if not save_dir:
            self.log("操作已取消")
            return

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
                except:
                    pass
            self.canvases.clear()
            self.log("个股分析任务启动（含实时股价）...", "single")
        else:
            self.output_text_scan.delete(1.0, tk.END)
            for item in self.tree.get_children():
                self.tree.delete(item)
            self.log("龙虎榜扫描任务启动（含实时最新价 + 换手率过滤）...", "scan")

        thread = threading.Thread(
            target=self._worker,
            args=(start_date, end_date, save_dir),
            daemon=True
        )
        thread.start()

        self.root.after(400, self._poll_queue)

    def request_stop(self):
        if not self.running:
            return
        self.stop_requested = True
        self.log("\n→ 用户请求停止，正在等待当前操作完成...\n")
        self.stop_button.config(state="disabled")
        self.progress.stop()
        self.progress.configure(mode='determinate')
        self.progress['value'] = 50

    def _get_realtime_price(self, code):
        try:
            df = ak.stock_bid_ask_em(symbol=code)
            if not df.empty and '最新价' in df.columns:
                return float(df['最新价'].iloc[0])
        except:
            pass
        return None

    def _get_all_realtime_spot(self):
        try:
            df = ak.stock_zh_a_spot_em()
            if not df.empty and '代码' in df.columns and '最新价' in df.columns:
                return df.set_index('代码')['最新价'].to_dict()
        except Exception as e:
            self.log(f"批量实时行情获取失败: {e}，将逐个获取...", self.current_mode)
        return {}

    def _worker(self, start_date, end_date, save_dir):
        try:
            if self.current_mode == "single":
                codes_str = self.codes_entry.get().strip()
                if not codes_str:
                    self.task_queue.put(("log", "请至少输入一个股票代码", "single"))
                    self.task_queue.put(("finish", False))
                    return

                codes = [c.strip() for c in codes_str.split(',') if c.strip()]

                lhb_all = self._safe_get_lhb(start_date, end_date)

                for code in codes:
                    if self.stop_requested:
                        self.task_queue.put(("log", "任务已手动停止", "single"))
                        break
                    self._process_single_stock(code, start_date, end_date, lhb_all, save_dir)

                if not self.stop_requested:
                    self.task_queue.put(("finish", True))

            else:
                self._process_lhb_scan(start_date, end_date, save_dir)

        except Exception as e:
            self.task_queue.put(("fatal", str(e)))

    def _safe_get_lhb(self, start, end):
        try:
            return ak.stock_lhb_detail_em(start_date=start, end_date=end)
        except Exception as e:
            self.task_queue.put(("log", f"龙虎榜批量获取失败: {e}", self.current_mode))
            return pd.DataFrame()

    def _process_single_stock(self, code, start, end, lhb_all, save_dir):
        try:
            self.task_queue.put(("log", f"\n处理 {code} ...", "single"))

            stock_name = code
            try:
                info = ak.stock_individual_info_em(symbol=code)
                if not info.empty:
                    stock_name = info[info['item'] == '股票简称']['value'].iloc[0]
            except:
                pass
            self.task_queue.put(("log", f"  名称：{stock_name}", "single"))

            price_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start,
                                           end_date=end, adjust="qfq")
            if not price_df.empty:
                price_df['日期'] = pd.to_datetime(price_df['日期'])
                self.task_queue.put(("log", f"  价格数据：{len(price_df)}条", "single"))

            market = "sh" if code.startswith(('6', '9')) else "sz"
            fund_df = pd.DataFrame()
            try:
                fund_df = ak.stock_individual_fund_flow(stock=code, market=market)
                if not fund_df.empty and '日期' in fund_df.columns:
                    fund_df['日期'] = pd.to_datetime(fund_df['日期'])
            except:
                pass

            game_df = pd.DataFrame()
            if not lhb_all.empty:
                game_df = lhb_all[lhb_all['代码'] == code].copy()
                if not game_df.empty and '上榜日' in game_df.columns:
                    game_df['上榜日'] = pd.to_datetime(game_df['上榜日'])

            realtime_price = self._get_realtime_price(code)
            price_str = f"{realtime_price:.2f}" if realtime_price is not None else "不可用"
            self.task_queue.put(("log", f"  实时最新价：{price_str}（可能为上日收盘）", "single"))

            excel_path = f"{save_dir}/{stock_name}_{code}.xlsx"
            with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
                if not price_df.empty: price_df.to_excel(writer, "历史价格")
                if not fund_df.empty:  fund_df.to_excel(writer, "资金流向")
                if not game_df.empty:  game_df.to_excel(writer, "龙虎榜")
            self.task_queue.put(("log", f"  已保存：{excel_path}", "single"))

            self.task_queue.put(("single_done", stock_name, code, price_df, fund_df, game_df, realtime_price))

            time.sleep(0.3)

        except Exception as e:
            self.task_queue.put(("log", f"处理 {code} 失败：{traceback.format_exc(limit=2)}", "single"))

    def _process_lhb_scan(self, start, end, save_dir):
        self.task_queue.put(("log", f"扫描 {start} ~ {end} 的龙虎榜...", "scan"))

        lhb_df = self._safe_get_lhb(start, end)
        if lhb_df.empty:
            self.task_queue.put(("log", "无数据或获取失败", "scan"))
            self.task_queue.put(("scan_done", pd.DataFrame()))
            return

        self.task_queue.put(("log", f"龙虎榜返回的列名：{list(lhb_df.columns)}", "scan"))

        code_col   = '代码'
        name_col   = '名称'
        date_col   = '上榜日'
        buy_col    = '龙虎榜净买额'
        turnover_col = '换手率'

        required_cols = [code_col, name_col, date_col, buy_col]
        missing = [col for col in required_cols if col not in lhb_df.columns]

        if missing:
            self.task_queue.put(("log", f"缺少关键列：{', '.join(missing)} → 无法汇总", "scan"))
            self.task_queue.put(("scan_done", pd.DataFrame()))
            return

        has_turnover = turnover_col in lhb_df.columns
        if not has_turnover:
            self.task_queue.put(("log", "警告：未找到‘换手率’列，将不进行换手率过滤", "scan"))

        agg_dict = {
            name_col: 'first',
            date_col: ['count', 'min', 'max'],
            buy_col: 'sum'
        }
        if has_turnover:
            agg_dict[turnover_col] = list

        summary = lhb_df.groupby(code_col).agg(agg_dict).reset_index()

        columns = [code_col, '简称', '上榜次数', '首次上榜', '最后上榜', '累计净买额']
        if has_turnover:
            columns.append('上榜日换手率列表')

        summary.columns = columns

        if has_turnover:
            summary['平均上榜换手率(%)'] = summary['上榜日换手率列表'].apply(
                lambda x: round(sum(x) / len(x), 2) if isinstance(x, list) and len(x) > 0 else None
            )
            summary['上榜日换手率列表'] = summary['上榜日换手率列表'].apply(
                lambda x: ", ".join(f"{v:.2f}%" for v in x) if isinstance(x, list) else ""
            )

        summary['累计净买额'] = summary['累计净买额'].round(0).astype(int)

        # 换手率阈值过滤
        threshold_str = self.turnover_threshold_entry.get().strip()
        before_count = len(summary)
        if has_turnover and threshold_str:
            try:
                threshold = float(threshold_str)
                if threshold > 0:
                    summary = summary[
                        (summary['平均上榜换手率(%)'].notna()) &
                        (summary['平均上榜换手率(%)'] >= threshold)
                    ].reset_index(drop=True)
                    self.task_queue.put(("log", f"按平均换手率 ≥ {threshold}% 过滤：{before_count} → {len(summary)} 只", "scan"))
            except ValueError:
                self.task_queue.put(("log", f"换手率阈值输入 '{threshold_str}' 格式错误，已忽略过滤", "scan"))

        # 排序
        summary = summary.sort_values('上榜次数', ascending=False).reset_index(drop=True)

        self.log("正在获取实时股价（全市场）...", "scan")
        spot_dict = self._get_all_realtime_spot()
        summary['最新价'] = summary[code_col].map(spot_dict).fillna("N/A")

        save_path = f"{save_dir}/龙虎榜扫描_{start}_{end}_含实时价_换手率.xlsx"
        summary.to_excel(save_path, index=False)
        self.task_queue.put(("log", f"结果已保存：{save_path}", "scan"))
        self.task_queue.put(("scan_done", summary))

    def _poll_queue(self):
        if not self.running:
            return

        try:
            while True:
                msg = self.task_queue.get_nowait()
                cmd = msg[0]

                if cmd == "log":
                    text, mode = msg[1], msg[2] if len(msg) > 2 else self.current_mode
                    self.log(text, mode)

                elif cmd == "single_done":
                    _, name, code, price, fund, game, realtime_price = msg
                    tab_title = f"{name} ({code})"
                    if realtime_price is not None:
                        tab_title += f" - {realtime_price:.2f}"
                    self._create_single_tab(tab_title, code, price, fund, game)

                elif cmd == "scan_done":
                    df = msg[1]
                    self._show_scan_result(df)

                elif cmd == "finish":
                    success = msg[1]
                    self._finish_task(success=success)

                elif cmd == "fatal":
                    self.log(f"重大错误：{msg[1]}")
                    self._finish_task(success=False)

        except queue.Empty:
            pass

        self.root.after(300, self._poll_queue)

    def _create_single_tab(self, title, code, price_df, fund_df, game_df):
        tab = ttk.Frame(self.notebook_single)
        self.notebook_single.add(tab, text=title)

        scroll = ScrollableFrame(tab)
        scroll.pack(fill="both", expand=True)

        if not price_df.empty:
            fig = plt.figure(figsize=(10, 4.5))
            ax = fig.add_subplot(111)
            ax.plot(price_df['日期'], price_df['收盘'], color='#1f77b4', lw=1.8)
            ax.set_title(f"收盘价走势 - {title}")
            ax.grid(True, ls="--", alpha=0.4)
            ax.tick_params(axis='x', rotation=35)
            fig.tight_layout()
            self._embed(fig, scroll.scrollable_frame)

        if not fund_df.empty and '主力净额' in fund_df.columns:
            fig = plt.figure(figsize=(10, 4.5))
            ax = fig.add_subplot(111)
            values = pd.to_numeric(fund_df['主力净额'], errors='coerce').fillna(0)
            colors = ['#2ca02c' if v >= 0 else '#d62728' for v in values]
            ax.bar(fund_df['日期'], values, color=colors, alpha=0.85)
            ax.set_title(f"主力资金净额 - {title}")
            ax.grid(True, axis='y', ls="--", alpha=0.4)
            ax.tick_params(axis='x', rotation=35)
            fig.tight_layout()
            self._embed(fig, scroll.scrollable_frame)

    def _embed(self, fig, parent):
        canvas = FigureCanvasTkAgg(fig, master=parent)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.TOP, fill=tk.BOTH, expand=True, pady=8, padx=8)
        self.canvases.append(canvas)

    def _show_scan_result(self, df):
        if df.empty:
            self.log("无符合条件的上榜股票", "scan")
            return

        for item in self.tree.get_children():
            self.tree.delete(item)

        columns = list(df.columns)
        self.tree["columns"] = columns
        self.tree["show"] = "headings"

        for col in columns:
            self.tree.heading(col, text=col)
            width = 160 if col in ["上榜日换手率列表", "简称"] else 110
            if col == "平均上榜换手率(%)":
                width = 130
            self.tree.column(col, width=width, anchor="center")

        for _, row in df.iterrows():
            values = [str(row[col]) if pd.notna(row[col]) else "" for col in columns]
            self.tree.insert("", "end", values=values)

        self.log(f"找到 {len(df)} 只符合条件的上榜股票（含实时最新价 + 换手率）", "scan")

    def _finish_task(self, success=True):
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


if __name__ == "__main__":
    root = tk.Tk()
    app = StockAnalyzerApp(root)
    root.mainloop()