import os
import time
import datetime
import tkinter as tk
from tkinter import filedialog, DoubleVar, HORIZONTAL, messagebox
import pandas as pd
import akshare as ak
import sqlite3
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from apscheduler.schedulers.background import BackgroundScheduler

# ==================== 中文字体配置 ====================
matplotlib.rcParams['font.sans-serif'] = ['SimHei']  # 黑体
matplotlib.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# ==================== 配置 ====================
DB_PATH = "data/stock.db"
EXPORT_PATH_DEFAULT = "data/export"
LOG_PATH = "logs/app.log"

START_DATE = "20220101"
END_DATE = "20251231"

WEIGHTS = {
    "short": {"超大单净占比": 0.6, "大单净占比": 0.4},
    "mid": {"超大单净占比": 0.4, "大单净占比": 0.3, "主力净占比": 0.3},
    "long": {"主力净占比": 0.6, "大单净占比": 0.4},
}

ALERT_THRESHOLD = 8.0
ALERT_DAYS = 3
TOP_N = 50
ALERT_SOUND = False

# 创建目录
for d in ["data", EXPORT_PATH_DEFAULT, "logs"]:
    if not os.path.exists(d):
        os.makedirs(d)


# ==================== 数据库操作 ====================
def get_conn():
    return sqlite3.connect(DB_PATH)


def save_df(df, table):
    with get_conn() as conn:
        df.to_sql(table, conn, if_exists="replace", index=False)


def export_table(df, path, fmt='csv'):
    if fmt == 'csv':
        df.to_csv(path, index=False, encoding='utf-8-sig')
    else:
        df.to_excel(path, index=False)


# ==================== 下载模块 ====================
def download_price(code):
    df = ak.stock_zh_a_hist(symbol=code, start_date=START_DATE, end_date=END_DATE, adjust="qfq")
    save_df(df, f"price_{code}")


def download_fund(code):
    df = ak.stock_individual_fund_flow(symbol=code)
    save_df(df, f"fund_{code}")


def batch_download(codes):
    for code in codes:
        try:
            download_price(code)
            download_fund(code)
            time.sleep(0.5)
        except Exception as e:
            print(f"{code} 下载失败：{e}")


# ==================== 博弈指标 ====================
def calc_battle_index(df):
    df = df.copy()
    df["短线博弈"] = sum(df[k] * v for k, v in WEIGHTS["short"].items() if k in df.columns)
    df["中线博弈"] = sum(df[k] * v for k, v in WEIGHTS["mid"].items() if k in df.columns)
    df["长线博弈"] = sum(df[k] * v for k, v in WEIGHTS["long"].items() if k in df.columns)
    return df


# ==================== 异动预警 ====================
def check_alert(df, threshold, days):
    recent = df.tail(days)
    if "主力净占比" in df.columns and (recent["主力净占比"] > threshold).all():
        return True
    return False


def alert_popup(root, msg):
    if ALERT_SOUND:
        try:
            import winsound
            winsound.Beep(1000, 500)
        except:
            pass
    messagebox.showwarning("资金异动预警", msg)


# ==================== 扫描 TOP50 ====================
def scan_market(top_n=TOP_N):
    codes = ak.stock_zh_a_spot_em()["代码"].tolist()
    result = []
    conn = get_conn()
    for code in codes[:1000]:
        try:
            df = pd.read_sql(f"SELECT * FROM fund_{code}", conn)
            df = calc_battle_index(df)
            score = df.iloc[-1]["中线博弈"] if "中线博弈" in df.columns else 0
            result.append((code, score))
        except:
            continue
    df_result = pd.DataFrame(result, columns=["代码", "中线博弈"]).sort_values("中线博弈", ascending=False)
    return df_result.head(top_n)


# ==================== GUI ====================
class ChartDashboard:
    def __init__(self, parent):
        self.parent = parent
        self.conn = get_conn()
        self.export_path = EXPORT_PATH_DEFAULT

        # 顶部输入 + 按钮
        self.top_frame = tk.Frame(parent)
        self.top_frame.pack(side=tk.TOP, fill=tk.X)

        tk.Label(self.top_frame, text="股票代码:").pack(side=tk.LEFT)
        self.stock_entry = tk.Entry(self.top_frame)
        self.stock_entry.pack(side=tk.LEFT, padx=5)
        tk.Button(self.top_frame, text="刷新单股图", command=self.plot_single).pack(side=tk.LEFT, padx=5)
        tk.Button(self.top_frame, text="刷新TOP50", command=self.plot_top50).pack(side=tk.LEFT, padx=5)
        tk.Button(self.top_frame, text="选择导出路径", command=self.select_export_path).pack(side=tk.LEFT, padx=5)

        # 滑块调整中线博弈权重
        self.weight_vars = {}
        self.weight_frame = tk.Frame(parent)
        self.weight_frame.pack(side=tk.TOP, fill=tk.X, pady=5)
        for idx, key in enumerate(WEIGHTS['mid'].keys()):
            var = DoubleVar(value=WEIGHTS['mid'][key])
            tk.Label(self.weight_frame, text=key).grid(row=0, column=idx)
            tk.Scale(self.weight_frame, from_=0, to=1, resolution=0.05, orient=HORIZONTAL,
                     variable=var, length=120).grid(row=1, column=idx)
            self.weight_vars[key] = var

        # Matplotlib 图表
        self.fig, self.axs = plt.subplots(2, 1, figsize=(10, 8))
        plt.tight_layout()
        self.canvas = FigureCanvasTkAgg(self.fig, master=parent)
        self.canvas.get_tk_widget().pack()

    def select_export_path(self):
        path = filedialog.askdirectory(initialdir=self.export_path)
        if path:
            self.export_path = path

    def _create_today_folder(self):
        today_str = datetime.datetime.now().strftime("%Y%m%d")
        path_today = os.path.join(self.export_path, today_str)
        if not os.path.exists(path_today):
            os.makedirs(path_today)
        return path_today

    def plot_single(self):
        code = self.stock_entry.get()
        if not code:
            return
        try:
            df_price = pd.read_sql(f"SELECT * FROM price_{code}", self.conn)
            df_fund = pd.read_sql(f"SELECT * FROM fund_{code}", self.conn)
        except:
            return
        for k in self.weight_vars:
            WEIGHTS['mid'][k] = self.weight_vars[k].get()
        df_fund = calc_battle_index(df_fund)

        self.axs[0].clear()
        self.axs[0].plot(pd.to_datetime(df_price['日期']), df_price['收盘'], color='blue', label='收盘价')
        self.axs[0].set_ylabel("收盘价", fontproperties="SimHei")
        self.axs[0].legend(prop={"family": "SimHei"})

        self.axs[1].clear()
        self.axs[1].plot(pd.to_datetime(df_fund['日期']), df_fund['中线博弈'], color='red', label='中线博弈')
        self.axs[1].set_ylabel("中线博弈", fontproperties="SimHei")
        self.axs[1].legend(prop={"family": "SimHei"})

        self.fig.tight_layout()
        self.canvas.draw()

        # 自动导出到当天日期文件夹
        path_today = self._create_today_folder()
        export_table(df_price, os.path.join(path_today, f"{code}_price.csv"))
        export_table(df_fund, os.path.join(path_today, f"{code}_fund.csv"))

        if check_alert(df_fund, ALERT_THRESHOLD, ALERT_DAYS):
            alert_popup(self.parent, f"{code} 主力连续{ALERT_DAYS}天净占比超过{ALERT_THRESHOLD}%")

    def plot_top50(self):
        df_top = scan_market()
        self.axs[0].clear()
        self.axs[1].clear()
        self.axs[0].bar(df_top['代码'], df_top['中线博弈'], color='green')
        self.axs[0].set_ylabel("中线博弈", fontproperties="SimHei")
        self.axs[0].set_title("TOP50 中线博弈", fontproperties="SimHei")
        self.axs[0].tick_params(axis='x', rotation=90)
        self.fig.tight_layout()
        self.canvas.draw()

        # 导出到当天日期文件夹
        path_today = self._create_today_folder()
        export_table(df_top, os.path.join(path_today, "top50_mid_battle.csv"))

        for code in df_top['代码']:
            try:
                df_fund = pd.read_sql(f"SELECT * FROM fund_{code}", self.conn)
                if check_alert(df_fund, ALERT_THRESHOLD, ALERT_DAYS):
                    alert_popup(self.parent, f"{code} 主力连续{ALERT_DAYS}天净占比超过{ALERT_THRESHOLD}%")
            except:
                continue


# ==================== 主窗口 ====================
class MainWindow:
    def __init__(self, root):
        self.root = root
        root.title("资金博弈自动化仪表板")
        self.dashboard = ChartDashboard(root)


# ==================== 自动化任务 ====================
def daily_scan(root):
    print("开始每日自动扫描 TOP50...")
    dashboard = root.children.get('!chartdashboard')
    if dashboard:
        dashboard.plot_top50()
    print("自动扫描完成，TOP50导出完成。")


# ==================== 程序入口 ====================
if __name__ == "__main__":
    root = tk.Tk()
    app = MainWindow(root)

    scheduler = BackgroundScheduler()
    scheduler.add_job(daily_scan, 'cron', hour=16, minute=10, args=[root])
    scheduler.start()

    root.mainloop()
