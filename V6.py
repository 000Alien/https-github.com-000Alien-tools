# streamlit run V6.2.py
import streamlit as st
import pandas as pd
import requests
import re
import time
import random
import json
import ssl
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import PoolManager
from bs4 import BeautifulSoup
import demjson3
import io

# ========================== SSL & 重试配置 ==========================
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx, **pool_kwargs)

def create_retry_session():
    session = requests.Session()
    retries = Retry(total=8, backoff_factor=1.2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", TLSAdapter())
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ========================== Streamlit 配置 ==========================
st.set_page_config(page_title="A股最强主线龙头股识别系统", page_icon="🐉", layout="wide")
st.title("🐉 A股最强主线 & 龙头股识别系统 V6.2")
st.markdown("**持仓解析 + 实时股价** | 优化爬虫 + JSON解析 + 东方财富实时行情")

# 初始化会话状态
default_state = {
    'high_funds': pd.DataFrame(),
    'all_holdings': pd.DataFrame(),
    'stock_scores': pd.DataFrame(),
    'stock_realtime': pd.DataFrame(),
    'fund_holdings_dict': {},
    'crawl_progress': {'current_page': 1, 'total_funds': 0, 'is_running': False},
    'last_crawl_time': None,
    'cache_key': None
}

for key, default_value in default_state.items():
    if key not in st.session_state:
        st.session_state[key] = default_value

# 侧边栏
st.sidebar.header("⚙️ 基础参数设置")
RETURN_THRESHOLD = st.sidebar.slider("涨幅阈值 (%)", 30, 300, 80, 10)
MAX_PAGES = st.sidebar.number_input("最大爬取页数", 1, 50, value=10)
DELAY_MIN = st.sidebar.slider("请求间隔(秒)", 1.0, 5.0, 3.5, 0.1)
TOP_N_STOCKS = st.sidebar.slider("显示 Top N 个股", 10, 100, 30)
MIN_HOLDING_FUNDS = st.sidebar.slider("龙头最少被持基金数", 1, 50, 8)

st.sidebar.divider()
st.sidebar.header("📅 爬取周期设置")
period_type = st.sidebar.selectbox(
    "涨幅统计周期",
    options=["近1周", "近1个月", "近3个月", "近6个月", "近1年", "近2年", "近3年", "今年来", "成立来", "自定义"],
    index=4
)

period_sort_map = {
    "近1周": "1w", "近1个月": "1m", "近3个月": "3m", "近6个月": "6m",
    "近1年": "lnzf", "近2年": "2nzf", "近3年": "3nzf",
    "今年来": "jnzf", "成立来": "clnf",
}

if period_type == "自定义":
    start_date = st.sidebar.date_input("开始日期", value=datetime.now() - timedelta(days=365))
    end_date = st.sidebar.date_input("结束日期", value=datetime.now())
else:
    start_date = end_date = None

USE_AKSHARE = st.sidebar.checkbox("优先使用 AKShare", value=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/'
}

# ========================== 下载功能 ==========================
def convert_df_to_csv(df):
    if df.empty:
        return None
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    return csv_buffer.getvalue().encode('utf-8-sig')

def create_download_buttons(df, title_prefix, key_prefix=""):
    if df.empty:
        return
    col1, col2 = st.columns(2)
    with col1:
        csv_data = convert_df_to_csv(df)
        if csv_data:
            st.download_button(
                label="📥 下载 CSV",
                data=csv_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{key_prefix}_csv"
            )

# ========================== JSON解析 ==========================
def safe_json_parse(json_str):
    if not json_str:
        return None
    json_str = json_str.strip()
    if json_str.startswith('var '):
        json_str = json_str.split('=', 1)[1].strip()
    if json_str.endswith(';'):
        json_str = json_str[:-1]
    try:
        fixed = re.sub(r'([{,])\s*([a-zA-Z_][\w]*)\s*:', r'\1"\2":', json_str)
        return json.loads(fixed)
    except:
        pass
    try:
        return demjson3.decode(json_str)
    except:
        pass
    try:
        safe_str = json_str.replace('null', 'None').replace('true', 'True').replace('false', 'False')
        return eval(safe_str)
    except:
        return None

# ========================== 爬取高涨幅基金 ==========================
def crawl_high_return_funds(reset=False):
    if reset:
        st.session_state.crawl_progress = {'current_page': 1, 'total_funds': 0, 'is_running': False}
        st.session_state.high_funds = pd.DataFrame()
        st.session_state.cache_key = None
        st.session_state.last_crawl_time = None

    if st.session_state.crawl_progress.get('is_running', False):
        st.warning("⚠️ 爬取任务正在运行中...")
        return st.session_state.high_funds

    if period_type == "自定义" and start_date and end_date:
        cache_key = f"custom_{start_date}_{end_date}_{RETURN_THRESHOLD}"
        period_display = f"{start_date} 至 {end_date}"
        col_name = f'{start_date.strftime("%Y%m%d")}至{end_date.strftime("%Y%m%d")}涨幅(%)'
    else:
        cache_key = f"{period_type}_{RETURN_THRESHOLD}"
        period_display = period_type
        col_name = f'{period_type}涨幅(%)'

    if (not reset and not st.session_state.high_funds.empty and st.session_state.get('cache_key') == cache_key):
        st.success(f"📦 使用缓存数据（{st.session_state.last_crawl_time}）")
        return st.session_state.high_funds

    funds = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    st.info(f"🚀 开始爬取 {period_display} 涨幅 ≥ {RETURN_THRESHOLD}% 的基金...")

    st.session_state.crawl_progress['is_running'] = True
    session = create_retry_session()
    page = 1

    with st.spinner("爬取中，请勿刷新页面..."):
        while page <= MAX_PAGES:
            try:
                if period_type == "自定义" and start_date and end_date:
                    sd = start_date.strftime('%Y-%m-%d')
                    ed = end_date.strftime('%Y-%m-%d')
                    url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc=zdf&st=desc&sd={sd}&ed={ed}&pi={page}&pn=100&v={random.random()}"
                else:
                    sort_field = period_sort_map.get(period_type, "lnzf")
                    url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc={sort_field}&st=desc&pi={page}&pn=100&v={random.random()}"

                resp = session.get(url, timeout=20)
                resp.raise_for_status()
                text = resp.text

                match = re.search(r'var\s+rankData\s*=\s*(\{.*?\});', text, re.DOTALL | re.IGNORECASE)
                if not match:
                    break

                data = safe_json_parse(match.group(1))
                if not data:
                    break

                datas = data.get('datas') or data.get('data') or []
                if not datas:
                    break

                field_map = {"近1周":4,"近1个月":5,"近3个月":6,"近6个月":7,
                            "近1年":8,"近2年":9,"近3年":10,"今年来":11,"成立来":12}
                ret_index = 6 if period_type == "自定义" else field_map.get(period_type, 8)

                for item in datas:
                    if isinstance(item, str):
                        fields = [x.strip() for x in item.split(',')]
                        if len(fields) <= ret_index:
                            continue
                        try:
                            ret_str = fields[ret_index].replace('%', '').strip()
                            ret = float(ret_str) if ret_str not in ('', '--', 'null') else 0
                            if ret >= RETURN_THRESHOLD:
                                funds.append({
                                    '基金代码': fields[0],
                                    '基金名称': fields[1],
                                    col_name: round(ret, 2),
                                    '净值': fields[2] if len(fields)>2 else '',
                                    '日涨幅': fields[3] if len(fields)>3 else ''
                                })
                        except:
                            continue

                progress = min(int(page / MAX_PAGES * 100), 100)
                progress_bar.progress(progress)
                status_text.text(f"第 {page} 页 | 已找到 {len(funds)} 只")

                if page >= int(data.get('allPages', MAX_PAGES)):
                    break
                page += 1
                time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.0))

            except Exception:
                page += 1
                continue

    st.session_state.crawl_progress['is_running'] = False
    st.session_state.last_crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.session_state.cache_key = cache_key
    df = pd.DataFrame(funds)
    st.session_state.high_funds = df

    if not df.empty:
        st.success(f"✅ 共找到 **{len(df)}** 只基金")
        st.dataframe(df[['基金代码', '基金名称', col_name]], use_container_width=True, hide_index=True)
        create_download_buttons(df, "高涨幅基金列表")
    else:
        st.warning("未找到符合条件的基金")

    return df

# ========================== 获取基金持仓 ==========================
def get_fund_holdings(fund_code: str, fund_name: str):
    session = create_retry_session()
    fund_code = str(fund_code).strip().zfill(6)

    if USE_AKSHARE:
        try:
            import akshare as ak
            df = ak.fund_portfolio_hold_em(symbol=fund_code)
            if not df.empty:
                df = df.rename(columns={'股票代码':'个股代码','股票名称':'个股名称',
                                      '占净值比例':'占基金净值比例(%)','持仓占比':'占基金净值比例(%)'})
                df['基金代码'] = fund_code
                df['基金名称'] = fund_name
                df['个股代码'] = df['个股代码'].astype(str).str.lstrip('0')
                df['占基金净值比例(%)'] = pd.to_numeric(df['占基金净值比例(%)'], errors='coerce')
                return df[['基金代码','基金名称','个股代码','个股名称','占基金净值比例(%)']].dropna()
        except:
            pass

    # 网页备用方案
    urls = [
        f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc",
        f"https://fund.eastmoney.com/f10/FundArchivesDatas.aspx?code={fund_code}&type=jjcc"
    ]
    for url in urls:
        try:
            resp = session.get(url, timeout=15)
            data = safe_json_parse(resp.text)
            if data and 'content' in data:
                soup = BeautifulSoup(data['content'], 'html.parser')
                holdings = []
                for row in soup.find_all('tr'):
                    cells = row.find_all('td')
                    if len(cells) >= 4:
                        stock_code = None
                        stock_name = cells[0].text.strip()
                        ratio = None
                        for cell in cells:
                            m = re.search(r'code=(\d{6})', cell.get_text() + str(cell))
                            if m:
                                stock_code = m.group(1)
                                break
                        if stock_code and len(cells) > 2:
                            try:
                                ratio = float(cells[2].text.strip().replace('%',''))
                            except:
                                pass
                        if stock_code and ratio and ratio > 0:
                            holdings.append({
                                '基金代码': fund_code, '基金名称': fund_name,
                                '个股代码': stock_code, '个股名称': stock_name,
                                '占基金净值比例(%)': ratio
                            })
                if holdings:
                    return pd.DataFrame(holdings)
        except:
            continue
    return pd.DataFrame()

# ========================== 批量持仓 ==========================
def crawl_all_holdings_from_list(fund_list_df):
    if fund_list_df.empty:
        st.error("基金列表为空")
        return pd.DataFrame()

    all_hold = []
    progress_bar = st.progress(0)
    status = st.empty()
    total = len(fund_list_df)

    with st.spinner(f"正在获取 {total} 只基金持仓..."):
        for i, row in fund_list_df.iterrows():
            code = str(row['基金代码']).strip().zfill(6)
            name = row.get('基金名称', f"基金{code}")
            status.text(f"[{i+1}/{total}] {code} {name}")
            hold_df = get_fund_holdings(code, name)
            if not hold_df.empty:
                all_hold.append(hold_df)
            progress_bar.progress((i+1)/total)
            time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.2))

    if all_hold:
        combined = pd.concat(all_hold, ignore_index=True).drop_duplicates(subset=['基金代码', '个股代码'])
        st.session_state.all_holdings = combined
        st.success(f"✅ 持仓获取完成！共 {len(combined)} 条记录")
        create_download_buttons(combined, "全部基金持仓")
        return combined
    return pd.DataFrame()

# ========================== 实时股价 ==========================
@st.cache_data(ttl=60)
def get_realtime_prices(stock_codes):
    if not stock_codes:
        return pd.DataFrame()
    try:
        import akshare as ak
        df = ak.stock_zh_a_spot_em()
        df = df[df['代码'].isin(stock_codes)][['代码', '名称', '最新价', '涨跌幅', '成交额', '总市值']]
        df = df.rename(columns={'代码': '个股代码', '名称': '个股名称'})
        df['总市值'] = pd.to_numeric(df['总市值'], errors='coerce')
        return df
    except Exception as e:
        st.warning(f"实时行情获取失败: {str(e)[:80]}")
        return pd.DataFrame()

def enrich_with_realtime(df_scores):
    if df_scores.empty:
        return df_scores
    codes = df_scores['个股代码'].tolist()
    rt_df = get_realtime_prices(codes)
    if not rt_df.empty:
        merged = df_scores.merge(rt_df, on='个股代码', how='left')
        st.session_state.stock_realtime = rt_df
        return merged
    return df_scores

# ========================== 龙头评分 ==========================
def identify_leaders():
    if st.session_state.all_holdings.empty:
        st.error("请先获取持仓数据")
        return

    df = st.session_state.all_holdings.copy()
    stats = df.groupby(['个股代码', '个股名称']).agg({
        '占基金净值比例(%)': ['sum', 'mean', 'count'],
        '基金代码': 'nunique'
    }).reset_index()
    stats.columns = ['个股代码', '个股名称', '总持仓比(%)', '平均持仓比(%)', '持仓记录数', '持仓基金数']
    
    stats = stats[stats['持仓基金数'] >= MIN_HOLDING_FUNDS].copy()
    if stats.empty:
        st.warning("没有足够的龙头候选股")
        return

    max_f = stats['持仓基金数'].max()
    stats['基金数得分'] = stats['持仓基金数'] / max_f * 40
    stats['平均持仓得分'] = stats['平均持仓比(%)'] / stats['平均持仓比(%)'].max() * 30
    stats['总持仓得分'] = stats['总持仓比(%)'] / stats['总持仓比(%)'].max() * 30
    stats['龙头评分'] = stats['基金数得分'] + stats['平均持仓得分'] + stats['总持仓得分']
    stats = stats.sort_values('龙头评分', ascending=False)

    st.session_state.stock_scores = stats
    st.success(f"✅ 评分完成！共识别 {len(stats)} 只候选龙头股")

# ========================== 主界面 ==========================
st.divider()
st.header("🚀 核心功能")

col1, col2, col3 = st.columns(3)
with col1: st.info(f"📅 周期：{period_type if period_type != '自定义' else f'{start_date} 至 {end_date}'}")
with col2: st.info(f"📊 阈值：≥{RETURN_THRESHOLD}%")
with col3: 
    if st.session_state.last_crawl_time:
        st.info(f"💾 上次：{st.session_state.last_crawl_time}")

c1, c2, c3, c4 = st.columns(4)
with c1:
    if st.button("1️⃣ 爬取高涨幅基金", type="primary", use_container_width=True):
        crawl_high_return_funds(reset=False)
with c2:
    if st.button("🔄 强制重新爬取", use_container_width=True):
        crawl_high_return_funds(reset=True)
with c3:
    if st.button("2️⃣ 获取持仓明细", type="primary", use_container_width=True):
        if st.session_state.high_funds.empty:
            st.error("请先执行步骤1")
        else:
            crawl_all_holdings_from_list(st.session_state.high_funds)
with c4:
    if st.button("3️⃣ 龙头评分 + 实时行情", type="primary", use_container_width=True):
        with st.spinner("正在评分并获取实时股价..."):
            identify_leaders()
            if not st.session_state.stock_scores.empty:
                st.session_state.stock_scores = enrich_with_realtime(st.session_state.stock_scores)

if not st.session_state.stock_scores.empty:
    if st.button("🔄 刷新实时股价", type="secondary"):
        with st.spinner("更新实时数据..."):
            st.session_state.stock_scores = enrich_with_realtime(st.session_state.stock_scores)
            st.rerun()

# 结果展示
st.divider()
st.subheader("📊 分析结果")

if not st.session_state.high_funds.empty:
    with st.expander("📈 高涨幅基金列表", expanded=False):
        st.dataframe(st.session_state.high_funds, use_container_width=True, hide_index=True)

if not st.session_state.all_holdings.empty:
    with st.expander("📥 全部持仓明细", expanded=False):
        st.dataframe(st.session_state.all_holdings, use_container_width=True, hide_index=True)

if not st.session_state.stock_scores.empty:
    with st.expander("🐉 龙头股评分排行（含实时行情）", expanded=True):
        df_show = st.session_state.stock_scores.copy()
        display_cols = ['个股代码', '个股名称', '龙头评分', '持仓基金数', '最新价', '涨跌幅', '总市值', '平均持仓比(%)']
        for col in ['最新价', '涨跌幅', '总市值']:
            if col in df_show.columns:
                df_show[col] = pd.to_numeric(df_show[col], errors='coerce')
        
        show_df = df_show[display_cols].head(TOP_N_STOCKS)
        if '涨跌幅' in show_df.columns:
            show_df['涨跌幅'] = show_df['涨跌幅'].apply(lambda x: f"{x:.2f}%" if pd.notna(x) else "—")
        
        st.dataframe(show_df, use_container_width=True, hide_index=True)
        create_download_buttons(show_df, "龙头股_实时行情")

st.caption("V6.2 完整版 | 实时股价集成 | AKShare 支持 | 缓存优化")
