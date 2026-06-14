# streamlit run v12.py
# 修复说明（v12）：
# 1. 修复 AKShare 涨跌幅列含百分号导致无法筛选的问题
# 2. 彻底解决多线程修改 session_state 的不安全问题
# 3. 优化爬取过程中的状态更新逻辑，杜绝并发冲突
# 4. 微调 JSON 解析健壮性，避免少数特殊字符导致崩溃
# 5. 保留原有断点续爬与 UI 功能
# 6. 20260614修改代码 

import streamlit as st
import pandas as pd
import requests
import re
import time
import random
import json
import ssl
import threading
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import demjson3
import io

# ========================== 重试 Session（线程安全） ==========================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
_thread_local = threading.local()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/'
}

def create_retry_session():
    """创建带重试和 SSL 验证关闭的 Session，每个线程独立使用。"""
    session = requests.Session()
    session.verify = False
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=0.3,
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=64, pool_maxsize=64)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session

def get_thread_session():
    """每个工作线程复用自己的 Session。"""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = create_retry_session()
    return _thread_local.session

# ========================== 页面配置与样式 ==========================
st.set_page_config(page_title="A股最强主线龙头股识别系统", page_icon="🐉", layout="wide")

st.markdown("""
<style>
    .main .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.5rem;
        max-width: 1480px;
    }
    .app-title {
        font-size: 2.1rem;
        font-weight: 800;
        margin: 0 0 .35rem 0;
        color: #f5f7fb;
        letter-spacing: 0;
    }
    .app-subtitle {
        color: #aab4c3;
        font-size: 1rem;
        line-height: 1.7;
        margin-bottom: 1.2rem;
    }
    .hero-band {
        border: 1px solid rgba(148, 163, 184, .22);
        background: rgba(15, 23, 42, .55);
        padding: 1.15rem 1.25rem;
        border-radius: 8px;
        margin-bottom: 1rem;
    }
    .feature-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: .75rem;
        margin: .5rem 0 1rem 0;
    }
    .feature-item {
        border: 1px solid rgba(148, 163, 184, .20);
        background: rgba(30, 41, 59, .45);
        border-radius: 8px;
        padding: .85rem .9rem;
        min-height: 96px;
    }
    .feature-title {
        font-weight: 700;
        color: #e5e7eb;
        margin-bottom: .3rem;
    }
    .feature-desc {
        color: #aab4c3;
        font-size: .88rem;
        line-height: 1.55;
    }
    .section-note {
        border-left: 4px solid #ef4444;
        background: rgba(239, 68, 68, .09);
        padding: .75rem 1rem;
        border-radius: 6px;
        color: #d7dee9;
        margin: .35rem 0 1rem 0;
    }
    div[data-testid="stMetric"] {
        border: 1px solid rgba(148, 163, 184, .18);
        background: rgba(15, 23, 42, .42);
        padding: .55rem .75rem;
        border-radius: 8px;
    }
    .stButton > button, .stDownloadButton > button {
        border-radius: 8px;
        min-height: 2.65rem;
        font-weight: 650;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: .25rem;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding-left: 1rem;
        padding-right: 1rem;
    }
    @media (max-width: 900px) {
        .feature-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 560px) {
        .feature-grid { grid-template-columns: 1fr; }
        .app-title { font-size: 1.6rem; }
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<div class="hero-band">
    <div class="app-title">A股基金持仓与主线识别工作台</div>
    <div class="app-subtitle">
        从基金涨幅筛选、基金持仓爬取、个股反查到龙头评分的一体化工具。<br>
        适合先找强势基金池，再观察这些基金共同持有哪些股票，并支持导出明细继续分析。
    </div>
</div>
""", unsafe_allow_html=True)

# 会话状态初始化
_default_state = {
    'high_funds': pd.DataFrame(),
    'all_holdings': pd.DataFrame(),
    'stock_scores': pd.DataFrame(),
    'fund_holdings_dict': {},
    'crawl_done_codes': set(),
    'crawl_partial': [],
}
for key, default in _default_state.items():
    if key not in st.session_state:
        st.session_state[key] = default

# 侧边栏控件
st.sidebar.title("控制台")
st.sidebar.caption("先设定基金筛选口径，再执行主界面的爬取、导入或查询。")
today_date = datetime.now().date()

with st.sidebar.expander("基金筛选", expanded=True):
    RETURN_PERIOD = st.selectbox("涨幅周期", ["周", "月", "季度", "年"], index=2)
    if RETURN_PERIOD == "周":
        RETURN_START_DATE = today_date - timedelta(days=7)
        RETURN_END_DATE = today_date
        default_threshold = 5.0
    elif RETURN_PERIOD == "月":
        RETURN_START_DATE = today_date - timedelta(days=30)
        RETURN_END_DATE = today_date
        default_threshold = 20.0
    elif RETURN_PERIOD == "季度":
        RETURN_START_DATE = today_date - timedelta(days=90)
        RETURN_END_DATE = today_date
        default_threshold = 40.0
    elif RETURN_PERIOD == "年":
        RETURN_START_DATE = today_date - timedelta(days=365)
        RETURN_END_DATE = today_date
        default_threshold = 80.0

    RETURN_THRESHOLD = st.number_input(
        f"{RETURN_PERIOD}涨幅阈值 (%)",
        min_value=-100.0,
        max_value=500.0,
        value=default_threshold,
        step=5.0,
        help="只保留区间涨幅不低于该数值的基金。",
    )
    MAX_PAGES = st.number_input("最大爬取页数", 1, 50, value=10, help="每页约 100 只基金（AKShare 版本忽略此参数）")

with st.sidebar.expander("持仓爬取", expanded=True):
    HOLDING_WORKERS = st.slider("持仓并发数", 1, 16, 8, help="网络稳定时可调高；失败增多时调低。")
    HOLDING_DELAY = st.slider("请求抖动(秒)", 0.0, 2.0, 0.2, 0.1, help="给并发请求增加少量随机错峰。")
    DELAY_MIN = st.slider("基金列表请求间隔(秒)", 0.2, 5.0, 1.0, 0.1)
    FAST_DIRECT_MODE = st.checkbox("批量优先直连东方财富", value=True)
    USE_AKSHARE = st.checkbox("启用 AKShare 兜底/单只优先", value=True)

with st.sidebar.expander("结果展示", expanded=True):
    TOP_N_STOCKS = st.slider("龙头榜显示数量", 10, 100, 30)
    MIN_HOLDING_FUNDS = st.slider("龙头最少持有基金数", 1, 50, 8)

# ========================== 辅助函数 ==========================
def convert_df_to_csv(df):
    if df.empty:
        return None
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    return csv_buffer.getvalue().encode('utf-8-sig')

def convert_df_to_excel(df):
    if df.empty:
        return None
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='数据', index=False)
    return output.getvalue()

def create_download_buttons(df, title_prefix):
    if df.empty:
        return
    col1, col2, col3 = st.columns(3)
    with col1:
        csv_data = convert_df_to_csv(df)
        if csv_data:
            st.download_button(
                label="📥 下载 CSV",
                data=csv_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{title_prefix}_csv"
            )
    with col2:
        excel_data = convert_df_to_excel(df)
        if excel_data:
            st.download_button(
                label="📊 下载 Excel",
                data=excel_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{title_prefix}_excel"
            )
    with col3:
        st.metric("数据行数", len(df))

def download_fund_individual_holdings():
    st.subheader("💾 下载单只基金持仓")
    col1, col2 = st.columns([2, 1])
    with col1:
        fund_code_input = st.text_input("输入基金代码", placeholder="例如: 022364, 022365",
                                        key="single_fund_code")
    with col2:
        if st.button("获取并下载", key="download_single_fund"):
            if fund_code_input:
                with st.spinner("获取基金持仓中..."):
                    fund_code = fund_code_input.strip().zfill(6)
                    holdings_df = get_fund_holdings(fund_code, f"基金{fund_code}", verbose=True)
                    if not holdings_df.empty:
                        st.success(f"✅ 成功获取 {len(holdings_df)} 只个股")
                        st.dataframe(holdings_df, use_container_width=True)
                        csv_data = convert_df_to_csv(holdings_df)
                        if csv_data:
                            st.download_button(
                                label=f"📥 下载 {fund_code} 持仓 CSV",
                                data=csv_data,
                                file_name=f"基金_{fund_code}_持仓_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                mime="text/csv"
                            )
                    else:
                        st.error("未获取到持仓数据")
            else:
                st.warning("请输入基金代码")

# ========================== JSON 解析、代码标准化、持仓清洗 ==========================
def safe_json_parse(json_str):
    if not json_str:
        return None
    json_str = json_str.strip()
    if json_str.startswith('var '):
        json_str = json_str.split('=', 1)[1].strip()
    if json_str.endswith(';'):
        json_str = json_str[:-1]
    try:
        fixed_str = re.sub(r'([{,])\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', json_str)
        return json.loads(fixed_str)
    except:
        pass
    try:
        return demjson3.decode(json_str)
    except:
        pass
    try:
        json_str_fixed = json_str.replace('null', 'None').replace('true', 'True').replace('false', 'False')
        return eval(json_str_fixed)
    except:
        pass
    return None

def normalize_stock_code(value):
    if pd.isna(value):
        return ''
    text = str(value).strip()
    match = re.search(r'(\d{6})', text)
    if match:
        return match.group(1)
    digits = re.sub(r'\D', '', text)
    return digits[-6:].zfill(6) if digits else ''

def normalize_holdings_df(df):
    if df.empty or '个股代码' not in df.columns:
        return df
    df = df.copy()
    df['个股代码'] = df['个股代码'].apply(normalize_stock_code)
    df = df[df['个股代码'].str.match(r'^\d{6}$', na=False)]
    df = df[df['个股代码'] != '000000']
    if '个股名称' in df.columns:
        df['个股名称'] = df['个股名称'].astype(str).str.strip()
        df = df[~df['个股名称'].isin(['', 'nan', 'None', '股吧', '基金吧'])]
        df = df[~df['个股名称'].str.fullmatch(r'[\d.%-]+', na=False)]
    if '占基金净值比例(%)' in df.columns:
        df['占基金净值比例(%)'] = pd.to_numeric(df['占基金净值比例(%)'], errors='coerce')
        df = df[df['占基金净值比例(%)'].between(0, 100, inclusive='neither')]
    return df

def parse_fund_holdings_html(content, fund_code, fund_name):
    if not content:
        return pd.DataFrame()
    def _flatten_columns(table):
        table = table.copy()
        if isinstance(table.columns, pd.MultiIndex):
            table.columns = [
                ' '.join(str(x) for x in col if str(x) != 'nan').strip()
                for col in table.columns
            ]
        else:
            table.columns = [str(col).strip() for col in table.columns]
        return table
    def _pick_column(columns, keywords):
        for col in columns:
            col_text = str(col).replace(' ', '')
            if any(keyword in col_text for keyword in keywords):
                return col
        return None
    def _to_float(value):
        text = str(value).replace(',', '').replace('%', '').replace('--', '').strip()
        match = re.search(r'-?\d+(?:\.\d+)?', text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None
    try:
        tables = pd.read_html(io.StringIO(content))
    except Exception:
        return pd.DataFrame()
    all_records = []
    for table in tables:
        if table.empty:
            continue
        table = _flatten_columns(table)
        columns = list(table.columns)
        joined_columns = ''.join(str(col) for col in columns)
        if '股票' not in joined_columns or not any(k in joined_columns for k in ['占净值', '持仓占比', '净值比例']):
            continue
        code_col = _pick_column(columns, ['股票代码', '证券代码', '代码'])
        name_col = _pick_column(columns, ['股票名称', '证券名称', '名称'])
        ratio_col = _pick_column(columns, ['占净值比例', '占基金净值', '持仓占比', '净值比例'])
        if not code_col or not name_col or not ratio_col:
            continue
        for _, row in table.iterrows():
            stock_code = normalize_stock_code(row.get(code_col, ''))
            stock_name = str(row.get(name_col, '')).strip()
            ratio = _to_float(row.get(ratio_col, ''))
            if not stock_code or not stock_name or ratio is None:
                continue
            all_records.append({
                '基金代码': fund_code,
                '基金名称': fund_name,
                '个股代码': stock_code,
                '个股名称': stock_name,
                '占基金净值比例(%)': ratio,
            })
    if not all_records:
        return pd.DataFrame()
    return normalize_holdings_df(pd.DataFrame(all_records)).drop_duplicates(subset=['个股代码'])

def fetch_stock_fund_holdings_direct(stock_code):
    stock_code = normalize_stock_code(stock_code)
    if not re.match(r'^\d{6}$', stock_code):
        return pd.DataFrame()
    url = f"https://q.stock.sohu.com/cn/{stock_code}/jjcc.shtml"
    try:
        session = create_retry_session()
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or 'gb18030'
        html = resp.text
    except Exception:
        return pd.DataFrame()
    stock_name = f"股票{stock_code}"
    title_match = re.search(r'<title>\s*([^_(（<\s]+)\s*[_\(（]', html, re.I)
    if title_match:
        stock_name = title_match.group(1).strip()
    report_dates = re.findall(r'(?:报告期|截止日期|日期)[：:\s]*(20\d{2}[-/]\d{1,2}[-/]\d{1,2})', html)
    if not report_dates:
        report_dates = re.findall(r'20\d{2}[-/]\d{1,2}[-/]\d{1,2}', html)
    report_period = max([d.replace('/', '-') for d in report_dates], default='')
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception:
        return pd.DataFrame()
    def _flatten_columns(table):
        if isinstance(table.columns, pd.MultiIndex):
            table = table.copy()
            table.columns = [
                ' '.join(str(x) for x in col if str(x) != 'nan').strip()
                for col in table.columns
            ]
        else:
            table = table.copy()
            table.columns = [str(col).strip() for col in table.columns]
        return table
    def _to_float(value):
        text = str(value).replace(',', '').replace('%', '').replace('--', '').strip()
        match = re.search(r'-?\d+(?:\.\d+)?', text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except Exception:
            return None
    def _pick_column(columns, keywords):
        for col in columns:
            if any(keyword in col for keyword in keywords):
                return col
        return None
    records = []
    for table in tables:
        if table.empty:
            continue
        table = _flatten_columns(table)
        columns = list(table.columns)
        joined_columns = ''.join(columns)
        if '基金' not in joined_columns or not any(k in joined_columns for k in ['持仓', '持股', '占净值']):
            continue
        fund_code_col = _pick_column(columns, ['基金代码', '代码'])
        fund_name_col = _pick_column(columns, ['基金名称', '名称'])
        share_col = _pick_column(columns, ['持仓数量', '持股数量', '持有数量', '持股数'])
        ratio_col = _pick_column(columns, ['占净值比例', '占基金净值', '净值比例'])
        value_col = _pick_column(columns, ['持股市值', '持仓市值'])
        for _, row in table.iterrows():
            fund_code = ''
            if fund_code_col:
                fund_code = normalize_stock_code(row.get(fund_code_col, ''))
            if not re.match(r'^\d{6}$', fund_code) or fund_code == stock_code:
                values = [str(v).strip() for v in row.tolist()]
                for value in values:
                    match = re.search(r'\b(\d{6})\b', value)
                    if match and match.group(1) != stock_code:
                        fund_code = match.group(1)
                        break
            if not re.match(r'^\d{6}$', fund_code) or fund_code == stock_code:
                continue
            fund_name = str(row.get(fund_name_col, '')).strip() if fund_name_col else ''
            if not fund_name or fund_name == 'nan' or re.fullmatch(r'[\d.%-]+', fund_name):
                fund_name = f"基金{fund_code}"
            shares = _to_float(row.get(share_col, '')) if share_col else None
            ratio = _to_float(row.get(ratio_col, '')) if ratio_col else None
            market_value = _to_float(row.get(value_col, '')) if value_col else None
            positive_metrics = [x for x in [shares, ratio, market_value] if x is not None]
            if not positive_metrics or max(positive_metrics) <= 0:
                continue
            records.append({
                '基金代码': fund_code,
                '基金名称': fund_name,
                '个股代码': stock_code,
                '个股名称': stock_name,
                '占基金净值比例(%)': ratio,
                '报告期': report_period,
                '持仓数量(万股)': shares,
                '持股市值(万元)': market_value,
                '数据来源': '搜狐证券最新基金持仓',
            })
    if not records:
        return pd.DataFrame()
    result = pd.DataFrame(records).drop_duplicates(subset=['基金代码', '个股代码'])
    for col in ['占基金净值比例(%)', '持仓数量(万股)', '持股市值(万元)']:
        if col in result.columns:
            result[col] = pd.to_numeric(result[col], errors='coerce')
    return result

# ========================== 获取单只基金持仓（线程安全） ==========================
def get_fund_holdings(fund_code: str, fund_name: str,
                      verbose: bool = True, status_placeholder=None,
                      session=None, use_akshare=None, prefer_direct=False):
    def _log_success(msg):
        if verbose:
            st.success(msg)
        elif status_placeholder:
            status_placeholder.text(msg)
    def _log_info(msg):
        if verbose:
            st.info(msg)
        elif status_placeholder:
            status_placeholder.text(msg)
    def _log_warning(msg):
        if verbose:
            st.warning(msg)
        elif status_placeholder:
            status_placeholder.text(msg)
    session = session or create_retry_session()
    # 实时读取全局配置，避免闭包导致值过时
    use_akshare = USE_AKSHARE if use_akshare is None else use_akshare
    fund_code = str(fund_code).strip().zfill(6)

    def _try_akshare():
        try:
            import akshare as ak
            df = ak.fund_portfolio_hold_em(symbol=fund_code, date=None)
            if not df.empty:
                df = df.rename(columns={
                    '股票代码': '个股代码',
                    '股票名称': '个股名称',
                    '占净值比例': '占基金净值比例(%)',
                    '持仓占比': '占基金净值比例(%)'
                })
                if '个股代码' in df.columns and '占基金净值比例(%)' in df.columns:
                    df['基金代码'] = fund_code
                    df['基金名称'] = fund_name
                    result_df = df[['基金代码', '基金名称', '个股代码', '个股名称', '占基金净值比例(%)']].copy()
                    result_df['个股代码'] = result_df['个股代码'].apply(normalize_stock_code)
                    result_df['占基金净值比例(%)'] = pd.to_numeric(result_df['占基金净值比例(%)'], errors='coerce')
                    result_df = result_df.dropna(subset=['个股代码', '占基金净值比例(%)'])
                    result_df = result_df[result_df['个股代码'] != '']
                    result_df = normalize_holdings_df(result_df)
                    if not result_df.empty:
                        _log_success(f"✅ {fund_code} AKShare 获取成功（{len(result_df)} 只个股）")
                        return result_df
        except Exception as e:
            _log_info(f"AKShare获取失败 {fund_code}: {str(e)[:50]}")
        return pd.DataFrame()

    if use_akshare and not prefer_direct:
        ak_df = _try_akshare()
        if not ak_df.empty:
            return ak_df

    try:
        current_year = datetime.now().year
        urls = [
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&rt=0.{random.randint(100000, 999999)}",
            f"https://fund.eastmoney.com/f10/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&rt=0.{random.randint(100000, 999999)}",
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&year={current_year}&rt=0.{random.randint(100000, 999999)}",
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&year={current_year - 1}&rt=0.{random.randint(100000, 999999)}",
        ]
        for url in urls:
            try:
                resp = session.get(url, timeout=8, verify=False)
                resp.encoding = 'utf-8'
                if resp.text:
                    data = safe_json_parse(resp.text.strip())
                    if data and isinstance(data, dict):
                        content = data.get('content', '')
                        if content:
                            result_df = parse_fund_holdings_html(content, fund_code, fund_name)
                            if not result_df.empty:
                                _log_success(f"✅ {fund_code} JSON解析成功（{len(result_df)} 只个股）")
                                return result_df
                break
            except:
                continue
    except Exception as e:
        _log_info(f"JSON解析失败 {fund_code}: {str(e)[:50]}")

    try:
        url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
        resp = session.get(url, timeout=8, verify=False)
        resp.encoding = 'utf-8'
        if resp.text:
            result_df = parse_fund_holdings_html(resp.text, fund_code, fund_name)
            if not result_df.empty:
                _log_success(f"✅ {fund_code} 页面解析成功（{len(result_df)} 只个股）")
                return result_df
    except:
        pass

    if use_akshare and prefer_direct:
        ak_df = _try_akshare()
        if not ak_df.empty:
            return ak_df

    _log_warning(f"⚠️ {fund_code} {fund_name} 所有方法均获取失败")
    return pd.DataFrame()

# ========================== 1. 爬取高涨幅基金（修复百分号bug） ==========================
def get_return_period_config():
    start_date = pd.to_datetime(RETURN_START_DATE).date()
    end_date = pd.to_datetime(RETURN_END_DATE).date()
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    label = f"{RETURN_PERIOD}涨幅"
    column_name = f"{label}(%)"
    return start_date, end_date, label, column_name

def crawl_high_return_funds():
    """使用 AKShare 的 fund_open_fund_rank_em 获取区间涨幅靠前的基金列表"""
    import akshare as ak

    start_date, end_date, return_label, return_column = get_return_period_config()
    start_text = start_date.strftime('%Y-%m-%d')
    end_text = end_date.strftime('%Y-%m-%d')
    st.info(
        f"🚀 开始通过 AKShare 获取 {start_text} 至 {end_text} "
        f"{return_label} ≥ {RETURN_THRESHOLD}% 的基金..."
    )

    try:
        df = ak.fund_open_fund_rank_em()
        period_map = {
            "周": "近1周",
            "月": "近1月",
            "季度": "近3月",
            "年": "近1年",
        }
        period_col = period_map.get(RETURN_PERIOD)
        if period_col is None:
            st.error(f"不支持的涨幅周期：{RETURN_PERIOD}，请使用周/月/季度/年。")
            return pd.DataFrame()

        if period_col not in df.columns:
            st.error(f"返回数据中缺少 '{period_col}' 列。实际列名：{list(df.columns)}")
            st.dataframe(df.head())
            return pd.DataFrame()

        # ---------- 修复核心：去除百分号并转数值 ----------
        df[period_col] = df[period_col].astype(str).str.rstrip('%')
        df[period_col] = pd.to_numeric(df[period_col], errors='coerce')
        # ------------------------------------------------

        df = df[df[period_col] >= RETURN_THRESHOLD].copy()

        if df.empty:
            st.warning(f"未找到涨幅 ≥ {RETURN_THRESHOLD}% 的基金（周期：{RETURN_PERIOD}）。")
            return pd.DataFrame()

        df['基金代码'] = df['基金代码'].astype(str).str.zfill(6)
        df.rename(columns={
            '基金简称': '基金名称',
            period_col: return_column,
        }, inplace=True)

        df['涨幅周期'] = RETURN_PERIOD
        df['开始日期'] = start_text
        df['结束日期'] = end_text

        result_df = df[['基金代码', '基金名称', return_column, '涨幅周期', '开始日期', '结束日期']]
        st.session_state.high_funds = result_df

        st.success(f"✅ 共找到 **{len(result_df)}** 只基金")
        st.dataframe(result_df.head(20), use_container_width=True, hide_index=True)
        st.markdown("### 💾 下载基金列表")
        create_download_buttons(result_df, f"{return_label}基金列表")
        return result_df

    except Exception as e:
        st.error(f"AKShare 获取失败: {str(e)}")
        st.info("请确认：\n1. 已运行 pip install akshare --upgrade\n2. 函数名使用 fund_open_fund_rank_em")
        return pd.DataFrame()

# ========================== 2. 批量获取持仓（线程安全版） ==========================
def crawl_all_holdings_from_list(fund_list_df):
    if fund_list_df.empty or '基金代码' not in fund_list_df.columns:
        st.error("必须包含 '基金代码' 列")
        return pd.DataFrame()

    total = len(fund_list_df)
    # 从 session_state 恢复断点数据
    done_codes: set = st.session_state.crawl_done_codes
    # 从 crawl_partial 恢复已经获取的持仓（每个元素是 list of dict）
    all_hold = [pd.DataFrame(r) for r in st.session_state.crawl_partial] if st.session_state.crawl_partial else []
    fund_holdings_dict: dict = st.session_state.fund_holdings_dict

    # 构建待爬取列表
    remaining = []
    for pos, (_, row) in enumerate(fund_list_df.iterrows(), start=1):
        code = str(row['基金代码']).strip().zfill(6)
        if code in done_codes:
            continue
        name = row.get('基金名称', f"基金{code}")
        remaining.append((pos, code, name))

    if done_codes:
        st.info(f"⏩ 检测到断点续爬：已完成 {len(done_codes)} 只，剩余 {len(remaining)} 只")

    progress_bar = st.progress(int(len(done_codes) / total * 100) if total else 0)
    status_placeholder = st.empty()
    fund_msg_placeholder = st.empty()

    st.info(f"准备处理 {total} 只基金（本次 {len(remaining)} 只），并发数 {min(HOLDING_WORKERS, max(1, len(remaining)))}")

    # 工作函数：只负责爬取，返回结果，不触碰 session_state
    def _worker(pos, code, name):
        if HOLDING_DELAY > 0:
            time.sleep(random.uniform(0, HOLDING_DELAY))
        df = get_fund_holdings(
            code,
            name,
            verbose=False,
            session=get_thread_session(),
            use_akshare=USE_AKSHARE,
            prefer_direct=FAST_DIRECT_MODE,
        )
        return pos, code, name, df

    error_log = []
    completed = 0
    if remaining:
        max_workers = min(HOLDING_WORKERS, len(remaining))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_worker, pos, code, name): (pos, code, name)
                for pos, code, name in remaining
            }
            for future in as_completed(futures):
                try:
                    pos, code, name, hold_df = future.result()
                except Exception as e:
                    pos, code, name = futures[future]
                    completed += 1
                    error_log.append(code)
                    # 主线程安全更新 session_state
                    done_codes.add(code)
                    st.session_state.crawl_done_codes = done_codes
                    fund_msg_placeholder.text(f"⚠️ {code} {name} 任务异常: {str(e)[:80]}")
                    status_placeholder.text(f"已完成 {len(done_codes)}/{total} | 本次 {completed}/{len(remaining)}")
                    progress_bar.progress(min(int(len(done_codes) / total * 100), 100))
                    continue

                completed += 1
                if not hold_df.empty:
                    all_hold.append(hold_df)
                    fund_holdings_dict[code] = hold_df
                    # 主线程追加到 crawl_partial（安全）
                    st.session_state.crawl_partial.append(hold_df.to_dict('records'))
                    fund_msg_placeholder.text(f"✅ {code} {name} 获取成功（{len(hold_df)} 条）")
                else:
                    error_log.append(code)
                    fund_msg_placeholder.text(f"⚠️ {code} {name} 未获取到持仓")

                done_codes.add(code)
                st.session_state.crawl_done_codes = done_codes
                # 每完成一批（或每完成一个）更新 all_holdings 快照
                if completed % max(1, max_workers) == 0 or completed == len(remaining):
                    if all_hold:
                        combined = normalize_holdings_df(pd.concat(all_hold, ignore_index=True)).drop_duplicates(
                            subset=['基金代码', '个股代码']
                        )
                        st.session_state.all_holdings = combined
                        st.session_state.fund_holdings_dict = fund_holdings_dict
                status_placeholder.text(f"已完成 {len(done_codes)}/{total} | 本次 {completed}/{len(remaining)}")
                progress_bar.progress(min(int(len(done_codes) / total * 100), 100))

    # 最终合并与清理
    if all_hold:
        combined = normalize_holdings_df(pd.concat(all_hold, ignore_index=True)).drop_duplicates(
            subset=['基金代码', '个股代码']
        )
        st.session_state.all_holdings = combined
        st.session_state.fund_holdings_dict = fund_holdings_dict
        # 爬取完成，清空断点续爬状态
        st.session_state.crawl_done_codes = set()
        st.session_state.crawl_partial = []
        st.success(f"✅ 持仓获取完成！共 {len(combined)} 条记录，涉及 {combined['个股代码'].nunique()} 只个股")
        if error_log:
            st.warning(f"以下基金获取失败（共 {len(error_log)} 只）：{', '.join(error_log[:20])}"
                       + ("..." if len(error_log) > 20 else ""))
        st.dataframe(combined.head(100), use_container_width=True)
        st.markdown("### 💾 下载所有基金持仓")
        create_download_buttons(combined, "全部基金持仓汇总")
        if fund_holdings_dict:
            st.markdown("### 💾 下载单只基金持仓")
            selected_fund = st.selectbox(
                "选择要下载的基金",
                options=list(fund_holdings_dict.keys()),
                format_func=lambda x: (
                    f"{x} - {fund_holdings_dict[x]['基金名称'].iloc[0]}"
                    if not fund_holdings_dict[x].empty else x
                )
            )
            if selected_fund:
                create_download_buttons(fund_holdings_dict[selected_fund], f"基金_{selected_fund}_持仓")
        return combined

    st.error("所有基金均获取失败，请检查网络或更换爬取方式")
    return pd.DataFrame()

# ========================== 文件读取 ==========================
def read_fund_file(uploaded_file):
    if uploaded_file is None:
        return pd.DataFrame()
    try:
        if uploaded_file.name.endswith('.csv'):
            for encoding in ['utf-8-sig', 'gbk', 'gb2312', 'utf-8']:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding=encoding)
                    st.success(f"✅ CSV读取成功（{encoding}）")
                    return df
                except UnicodeDecodeError:
                    continue
            st.error("编码无法识别，请转为UTF-8")
            return pd.DataFrame()
        else:
            df = pd.read_excel(uploaded_file)
            st.success("✅ Excel读取成功")
            return df
    except Exception as e:
        st.error(f"文件读取失败: {str(e)}")
        return pd.DataFrame()

# ========================== 龙头评分 ==========================
def identify_leaders():
    if st.session_state.all_holdings.empty:
        st.error("请先获取持仓")
        return
    df = normalize_holdings_df(st.session_state.all_holdings)
    stats = df.groupby(['个股代码', '个股名称']).agg({
        '占基金净值比例(%)': ['sum', 'mean', 'count'],
        '基金代码': 'nunique'
    }).reset_index()
    stats.columns = ['个股代码', '个股名称', '总持仓比(%)', '平均持仓比(%)', '持仓记录数', '持仓基金数']
    stats = stats[stats['持仓基金数'] >= MIN_HOLDING_FUNDS]
    if stats.empty:
        st.warning(f"没有持仓基金数 ≥ {MIN_HOLDING_FUNDS} 的个股")
        return
    max_funds = stats['持仓基金数'].max()
    max_avg = stats['平均持仓比(%)'].max()
    max_total = stats['总持仓比(%)'].max()
    stats['基金数得分'] = (stats['持仓基金数'] / max_funds * 40) if max_funds > 0 else 0
    stats['平均持仓得分'] = (stats['平均持仓比(%)'] / max_avg * 30) if max_avg > 0 else 0
    stats['总持仓得分'] = (stats['总持仓比(%)'] / max_total * 30) if max_total > 0 else 0
    stats['龙头评分'] = stats['基金数得分'] + stats['平均持仓得分'] + stats['总持仓得分']
    stats = stats.sort_values('龙头评分', ascending=False)
    st.session_state.stock_scores = stats
    st.success(f"✅ 龙头评分完成！共识别 {len(stats)} 只候选股（持仓基金数≥{MIN_HOLDING_FUNDS}）")
    display_cols = ['个股代码', '个股名称', '龙头评分', '持仓基金数', '平均持仓比(%)', '总持仓比(%)']
    st.dataframe(stats[display_cols].head(TOP_N_STOCKS), use_container_width=True, hide_index=True)
    st.markdown("### 💾 下载龙头股数据")
    create_download_buttons(stats[display_cols], "龙头股评分排行")

def query_stock_fund_holdings():
    st.subheader("🔎 单个股票的基金持仓情况")
    def _filter_stock_holdings(holdings_df, keyword):
        holdings_df = normalize_holdings_df(holdings_df)
        query_code = normalize_stock_code(keyword)
        has_code = bool(re.search(r'\d', keyword)) and re.match(r'^\d{6}$', query_code)
        if has_code:
            return holdings_df[holdings_df['个股代码'] == query_code].copy()
        return holdings_df[
            holdings_df['个股名称'].astype(str).str.contains(keyword, case=False, na=False)
        ].copy()
    col_query, col_button = st.columns([3, 1])
    with col_query:
        query = st.text_input(
            "输入股票代码或股票名称",
            placeholder="例如：000001 或 平安银行",
            key="stock_fund_holding_query",
        ).strip()
    with col_button:
        st.write("")
        do_query = st.button("查询", type="primary", use_container_width=True, key="stock_fund_holding_search")
    if not query:
        st.caption("支持按 6 位股票代码精确查询，或按股票名称模糊查询。")
        return
    if not do_query:
        st.caption("输入后点击查询。")
        return
    query_code = normalize_stock_code(query)
    has_code = bool(re.search(r'\d', query)) and re.match(r'^\d{6}$', query_code)
    direct_result = pd.DataFrame()
    if has_code:
        with st.spinner("正在按股票代码直接查询基金持股..."):
            direct_result = fetch_stock_fund_holdings_direct(query_code)
    if has_code and direct_result.empty:
        st.warning(f"未在最新报告期基金持仓数据中找到「{query_code}」。")
        st.caption("为避免历史持仓误报，股票代码直查不再回退到本地缓存。")
        return
    if not direct_result.empty:
        result = direct_result.copy()
    elif st.session_state.all_holdings.empty:
        st.info("当前没有基金持仓缓存，正在按侧边栏条件先抓取基金池和持仓数据。")
        with st.spinner("正在准备基金池..."):
            if st.session_state.high_funds.empty:
                crawl_high_return_funds()
        if st.session_state.high_funds.empty:
            st.error("未获取到基金列表，请调低涨幅阈值或增大最大爬取页数后重试。")
            return
        with st.spinner("正在并发抓取基金持仓并筛选目标股票..."):
            crawl_all_holdings_from_list(st.session_state.high_funds)
        if st.session_state.all_holdings.empty:
            st.error("基金持仓抓取失败，请检查网络或调低持仓并发数后重试。")
            return
        df = normalize_holdings_df(st.session_state.all_holdings)
        result = _filter_stock_holdings(df, query)
    else:
        df = normalize_holdings_df(st.session_state.all_holdings)
        result = _filter_stock_holdings(df, query)
    if result.empty:
        st.warning(f"未找到「{query}」对应的基金持仓记录。可尝试先导入更完整的基金列表再查询。")
        return
    result['占基金净值比例(%)'] = pd.to_numeric(result['占基金净值比例(%)'], errors='coerce')
    result = result.sort_values('占基金净值比例(%)', ascending=False, na_position='last')
    stock_code = result['个股代码'].iloc[0]
    stock_name = result['个股名称'].iloc[0]
    fund_count = result['基金代码'].nunique()
    ratio_series = result['占基金净值比例(%)'].dropna()
    total_ratio = ratio_series.sum() if not ratio_series.empty else None
    avg_ratio = ratio_series.mean() if not ratio_series.empty else None
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("股票", f"{stock_code} {stock_name}")
    col2.metric("持有基金数", fund_count)
    col3.metric("合计持仓比", f"{total_ratio:.2f}%" if total_ratio is not None else "-")
    col4.metric("平均持仓比", f"{avg_ratio:.2f}%" if avg_ratio is not None else "-")
    preferred_cols = [
        '报告期',
        '基金代码',
        '基金名称',
        '个股代码',
        '个股名称',
        '持仓数量(万股)',
        '持股市值(万元)',
        '占基金净值比例(%)',
        '数据来源',
    ]
    display_cols = [col for col in preferred_cols if col in result.columns]
    st.dataframe(result[display_cols], use_container_width=True, hide_index=True)
    create_download_buttons(result[display_cols], f"股票_{stock_code}_基金持仓")

def render_feature_overview():
    st.markdown("""
<div class="feature-grid">
    <div class="feature-item">
        <div class="feature-title">1. 筛选强势基金</div>
        <div class="feature-desc">按月、季度、年等周期筛选涨幅靠前的基金，形成后续分析的基金池。</div>
    </div>
    <div class="feature-item">
        <div class="feature-title">2. 抓取基金持仓</div>
        <div class="feature-desc">并发获取基金股票持仓，保留完整股票代码，并过滤明显异常的页面辅助链接。</div>
    </div>
    <div class="feature-item">
        <div class="feature-title">3. 反查单股持仓</div>
        <div class="feature-desc">输入股票代码查看最新报告期中有哪些基金持有，也可基于本地持仓缓存按名称查询。</div>
    </div>
    <div class="feature-item">
        <div class="feature-title">4. 识别共同持仓</div>
        <div class="feature-desc">根据持有基金数、平均持仓比例和总持仓比例生成候选龙头股评分。</div>
    </div>
</div>
""", unsafe_allow_html=True)

def render_data_status():
    high_count = 0 if st.session_state.high_funds.empty else len(st.session_state.high_funds)
    holding_count = 0 if st.session_state.all_holdings.empty else len(st.session_state.all_holdings)
    stock_count = 0 if st.session_state.all_holdings.empty else st.session_state.all_holdings['个股代码'].nunique()
    score_count = 0 if st.session_state.stock_scores.empty else len(st.session_state.stock_scores)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("基金池", f"{high_count} 只")
    col2.metric("持仓明细", f"{holding_count} 条")
    col3.metric("覆盖个股", f"{stock_count} 只")
    col4.metric("龙头候选", f"{score_count} 只")

# ========================== 主界面 ==========================
st.markdown("**功能概览**")
render_feature_overview()
render_data_status()
st.divider()
st.header("基金持仓获取与分析")
if st.session_state.crawl_done_codes:
    st.warning(
        f"⚠️ 检测到未完成的爬取任务（已完成 {len(st.session_state.crawl_done_codes)} 只基金）。"
        "点击「从爬取结果获取持仓」可继续；点击「清空所有数据」可重新开始。"
    )
tab_crawl, tab_import, tab_download, tab_stock_query = st.tabs([
    "📈 从涨幅基金爬取",
    "📋 手动导入基金列表",
    "💾 单只基金下载",
    "🔎 单股持仓查询",
])
with tab_crawl:
    st.markdown("""
<div class="section-note">
按侧边栏的涨幅周期和阈值先生成基金池，再抓取这些基金的股票持仓。
适合从“近期表现较强的基金”反推出共同关注的行业或个股。
</div>
""", unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        if st.button("1️⃣ 爬取涨幅基金", type="primary", use_container_width=True):
            with st.spinner("爬取中..."):
                crawl_high_return_funds()
    with col2:
        if st.button("2️⃣ 从爬取结果获取持仓", type="primary", use_container_width=True):
            if st.session_state.high_funds.empty:
                st.error("请先执行步骤 1")
            else:
                crawl_all_holdings_from_list(st.session_state.high_funds)
with tab_import:
    st.markdown("""
<div class="section-note">
已有基金名单时使用这里。上传 Excel/CSV 或直接粘贴基金代码，系统会跳过涨幅筛选，直接抓取这些基金的持仓。
</div>
""", unsafe_allow_html=True)
    st.subheader("导入基金列表")
    uploaded_file = st.file_uploader("上传 Excel / CSV（必须包含 '基金代码' 列）", type=['xlsx', 'xls', 'csv'])
    st.markdown("**或直接粘贴基金代码（每行一个）**")
    manual_input = st.text_area("", height=140, placeholder="022364\n022365\n001234 易方达某某基金")
    if st.button("🚀 从导入列表获取持仓", type="primary", use_container_width=True):
        fund_list = pd.DataFrame()
        if uploaded_file is not None:
            fund_list = read_fund_file(uploaded_file)
        elif manual_input.strip():
            lines = [line.strip() for line in manual_input.split('\n') if line.strip()]
            data = []
            for line in lines:
                parts = line.split(maxsplit=1)
                code = parts[0].strip()
                name = parts[1].strip() if len(parts) > 1 else f"基金{code}"
                if code:
                    data.append({'基金代码': code, '基金名称': name})
            fund_list = pd.DataFrame(data)
        if not fund_list.empty:
            st.info(f"准备处理 {len(fund_list)} 只基金")
            crawl_all_holdings_from_list(fund_list)
with tab_download:
    st.markdown("""
<div class="section-note">
用于快速查看和下载单只基金的最新股票持仓，不会影响当前批量分析结果。
</div>
""", unsafe_allow_html=True)
    download_fund_individual_holdings()
with tab_stock_query:
    st.markdown("""
<div class="section-note">
输入 6 位股票代码时，会优先查询最新报告期基金持股；输入股票名称时，会在当前已爬取的持仓缓存中查找。
</div>
""", unsafe_allow_html=True)
    query_stock_fund_holdings()
st.divider()
st.markdown("**分析与导出**")
col1, col2, col3 = st.columns(3)
with col1:
    if st.button("3️⃣ 龙头识别评分", type="primary", use_container_width=True):
        with st.spinner("正在进行龙头评分..."):
            identify_leaders()
with col2:
    if st.button("🔄 清空所有数据", type="secondary", use_container_width=True):
        st.session_state.high_funds = pd.DataFrame()
        st.session_state.all_holdings = pd.DataFrame()
        st.session_state.stock_scores = pd.DataFrame()
        st.session_state.fund_holdings_dict = {}
        st.session_state.crawl_done_codes = set()
        st.session_state.crawl_partial = []
        st.success("已清空所有数据（含断点续爬记录）")
with col3:
    if not st.session_state.all_holdings.empty or not st.session_state.stock_scores.empty:
        export_data = {}
        if not st.session_state.all_holdings.empty:
            export_data['基金持仓明细'] = st.session_state.all_holdings
        if not st.session_state.stock_scores.empty:
            export_data['龙头股评分'] = st.session_state.stock_scores
        if export_data:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for sheet_name, df in export_data.items():
                    df.to_excel(writer, sheet_name=sheet_name[:31], index=False)
            st.download_button(
                label="📦 导出全部数据(Excel)",
                data=output.getvalue(),
                file_name=f"全部数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
if not st.session_state.all_holdings.empty:
    st.session_state.all_holdings = normalize_holdings_df(st.session_state.all_holdings)
    with st.expander("📥 基金持仓明细", expanded=False):
        st.dataframe(st.session_state.all_holdings, use_container_width=True, hide_index=True)
        st.caption(f"共 {len(st.session_state.all_holdings)} 条记录")
        st.markdown("**💾 下载当前数据**")
        create_download_buttons(st.session_state.all_holdings, "基金持仓明细")
if not st.session_state.stock_scores.empty:
    with st.expander("🐉 龙头股评分排行", expanded=True):
        cols = ['个股代码', '个股名称', '龙头评分', '持仓基金数', '平均持仓比(%)', '总持仓比(%)']
        st.dataframe(st.session_state.stock_scores[cols].head(TOP_N_STOCKS),
                     use_container_width=True, hide_index=True)
        st.markdown("**💾 下载当前数据**")
        create_download_buttons(st.session_state.stock_scores[cols].head(TOP_N_STOCKS), "龙头股排行")
st.caption("v12 修复版 | 修复 AKShare 涨跌幅解析 & 多线程 session_state 安全隐患 | 断点续爬更稳定")
