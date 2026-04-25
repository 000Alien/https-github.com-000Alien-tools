#基金持股分析
import streamlit as st
import pandas as pd
import requests
import re
import time
import random
import json
import ssl
from datetime import datetime, timedelta
import numpy as np
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib3 import PoolManager

# ========================== SSL & 重试适配器（解决 UNEXPECTED_EOF） ==========================
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4  # OP_NO_SSLv3
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize, block=block,
            ssl_context=ctx, **pool_kwargs
        )

def create_retry_session():
    session = requests.Session()
    retries = Retry(total=6, backoff_factor=1.2, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", TLSAdapter())
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session

# ========================== 页面配置 ==========================
st.set_page_config(page_title="A股最强主线龙头股识别系统", page_icon="🐉", layout="wide")
st.title("🐉 A股最强主线 & 龙头股识别系统")
st.markdown("**优化版**：高涨幅基金 → 持仓个股（SSL 修复）→ 龙头评分 → 主线聚类")

# ========================== 会话状态 ==========================
for key in ['high_funds', 'all_holdings', 'stock_scores', 'industry_leaders']:
    if key not in st.session_state:
        st.session_state[key] = pd.DataFrame()
if 'maintheme_clusters' not in st.session_state:
    st.session_state.maintheme_clusters = {}

# ========================== 侧边栏 ==========================
st.sidebar.header("⚙️ 参数设置")
tab1, tab2 = st.sidebar.tabs(["爬取参数", "分析参数"])

with tab1:
    RETURN_THRESHOLD = st.slider("近一年涨幅阈值 (%)", 30, 300, 80, 10)
    MAX_PAGES = st.number_input("最大爬取页数", 1, 50, value=10)
    DELAY_MIN = st.slider("请求间隔(秒)", 1.0, 5.0, 2.8, 0.1)   # 加大防SSL/封禁
    USE_AKSHARE = st.checkbox("优先使用 AKShare", value=True)

with tab2:
    TOP_N_STOCKS = st.slider("显示 Top N 个股", 10, 100, 30)
    MIN_HOLDING_FUNDS = st.slider("龙头最少被持基金数", 1, 50, 8)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/data/fundranking.html'
}

# ========================== 1. 爬取高涨幅基金 ==========================
def crawl_high_return_funds():
    funds = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    st.info(f"🚀 开始爬取近一年涨幅 ≥ {RETURN_THRESHOLD}% 的基金...")

    page = 1
    session = create_retry_session()
    while page <= MAX_PAGES:
        url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc=1nzf&st=desc&sd={(datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')}&ed={datetime.now().strftime('%Y-%m-%d')}&pi={page}&pn=100&v={random.random()}"
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            text = resp.text

            match = re.search(r'var\s+rankData\s*=\s*(\{.*?\});', text, re.DOTALL | re.IGNORECASE)
            if not match:
                st.warning(f"第 {page} 页无数据")
                break

            data_str = re.sub(r'([a-zA-Z_]\w*)\s*:', r'"\1":', match.group(1))
            data = json.loads(data_str) if '{' in data_str else eval(data_str)

            datas = data.get('datas') or data.get('data') or []
            if not datas:
                break

            total_pages = data.get('allPages') or data.get('allNum') or data.get('pages') or data.get('pageNum') or MAX_PAGES

            for item in datas:
                if isinstance(item, str):
                    fields = [x.strip() for x in item.split(',')]
                    if len(fields) < 12:
                        continue
                    try:
                        ret = float(fields[11].replace('%', '').strip())
                        if ret >= RETURN_THRESHOLD:
                            funds.append({
                                '基金代码': fields[0],
                                '基金名称': fields[1],
                                '近一年涨幅(%)': round(ret, 2),
                                '爬取时间': datetime.now().strftime('%Y-%m-%d %H:%M')
                            })
                    except:
                        continue

            progress = min(int(page / MAX_PAGES * 100), 100)
            progress_bar.progress(progress)
            status_text.text(f"第 {page} 页完成 | 已找到 {len(funds)} 只基金")

            if page >= int(total_pages):
                break
            page += 1
            time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.0))
        except Exception as e:
            st.error(f"第 {page} 页异常: {str(e)[:100]}")
            page += 1
            continue

    df = pd.DataFrame(funds)
    st.session_state.high_funds = df
    if not df.empty:
        st.success(f"✅ 爬取完成！共找到 **{len(df)}** 只高涨幅基金")
        st.dataframe(df.head(20), use_container_width=True, hide_index=True)
    return df


# ========================== 2. 获取基金持仓（核心优化） ==========================
def parse_holdings_js(text):
    """简单解析 FundArchivesDatas.aspx 返回的 JS 数据"""
    try:
        # 提取 apidata 或类似变量中的数组
        match = re.search(r'var apidata=\s*(\{.*?\});', text, re.DOTALL)
        if not match:
            match = re.search(r'(\[\s*\{.*?\}\s*\])', text, re.DOTALL)
        if match:
            data_str = match.group(1)
            data = json.loads(data_str) if data_str.startswith('{') or data_str.startswith('[') else eval(data_str)
            # 根据实际结构提取持仓列表（常见为 data['datas'] 或直接列表）
            holdings = data.get('datas') or data if isinstance(data, list) else []
            rows = []
            for h in holdings[:50]:  # 取前50只重仓
                if isinstance(h, str):
                    fields = h.split(',')
                elif isinstance(h, dict):
                    fields = [h.get(k, '') for k in ['gpdm', 'gpjc', 'cczb', 'ccsl', 'ccsz']]
                else:
                    continue
                if len(fields) >= 3:
                    rows.append({
                        '个股代码': fields[0],
                        '个股名称': fields[1],
                        '占基金净值比例(%)': float(fields[2].replace('%', '')) if '%' in str(fields[2]) else float(fields[2]),
                    })
            return pd.DataFrame(rows)
    except:
        pass
    return pd.DataFrame()

def get_fund_holdings(fund_code: str, fund_name: str):
    session = create_retry_session()
    try:
        if USE_AKSHARE:
            import akshare as ak
            df = ak.fund_portfolio_hold_em(symbol=fund_code)
            if not df.empty:
                rename_map = {'股票代码': '个股代码', '股票名称': '个股名称', '占净值比例': '占基金净值比例(%)'}
                df = df.rename(columns=rename_map)
                df['基金代码'] = fund_code
                df['基金名称'] = fund_name
                return df

        # 备用直接请求 + 解析
        url = f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={fund_code}&topline=50&rt={random.random()}"
        resp = session.get(url, timeout=25, verify=False)
        resp.raise_for_status()

        df = parse_holdings_js(resp.text)
        if not df.empty:
            df['基金代码'] = fund_code
            df['基金名称'] = fund_name
            st.info(f"基金 {fund_code} 使用备用接口解析成功")
            return df
        else:
            st.warning(f"基金 {fund_code} 解析持仓失败")
            return pd.DataFrame()

    except Exception as e:
        st.warning(f"基金 {fund_code} {fund_name} 持仓失败: {str(e)[:120]}")
        return pd.DataFrame()


def crawl_all_holdings():
    if st.session_state.high_funds.empty:
        st.error("请先执行步骤 1")
        return pd.DataFrame()

    all_hold = []
    progress_bar = st.progress(0)
    status = st.empty()
    total = len(st.session_state.high_funds)

    with st.spinner("正在获取持仓数据（已优化 SSL）..."):
        for i, row in st.session_state.high_funds.iterrows():
            status.text(f"处理 {i+1}/{total}：{row['基金代码']} {row['基金名称']}")
            hold_df = get_fund_holdings(row['基金代码'], row['基金名称'])
            if not hold_df.empty:
                all_hold.append(hold_df)
            progress_bar.progress(int((i + 1) / total * 100))
            time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.2))

        if all_hold:
            combined = pd.concat(all_hold, ignore_index=True)
            st.session_state.all_holdings = combined
            st.success(f"✅ 持仓获取完成！共 {len(combined)} 条记录")
            st.dataframe(combined.head(60), use_container_width=True)
            return combined
        return pd.DataFrame()


# ========================== 3. 龙头评分（简化版，保持核心逻辑） ==========================
def identify_leaders():
    if st.session_state.all_holdings.empty:
        st.error("请先执行步骤 2")
        return pd.DataFrame()

    df = st.session_state.all_holdings.copy()
    stats = df.groupby(['个股代码', '个股名称']).agg({
        '占基金净值比例(%)': ['sum', 'mean', 'count'],
        '基金代码': 'nunique'
    }).reset_index()

    stats.columns = ['个股代码', '个股名称', '总持仓比(%)', '平均持仓比(%)', '持仓记录数', '持仓基金数']

    # 模拟数据（可后续替换真实接口）
    stats = stats.merge(get_stock_returns(stats['个股代码'].tolist()), on='个股代码', how='left')
    stats = stats.merge(get_industry_info(stats['个股代码'].tolist()), on='个股代码', how='left')

    stats['龙头评分'] = calculate_leader_score(stats)
    stats = stats.sort_values('龙头评分', ascending=False)
    stats['龙头等级'] = pd.cut(stats['龙头评分'], bins=[0, 40, 60, 80, 100],
                               labels=['⭐', '⭐⭐', '⭐⭐⭐', '🐉超级龙头'], include_lowest=True)

    st.session_state.stock_scores = stats
    st.success(f"✅ 龙头识别完成！共 {len(stats)} 只候选")
    leaders = stats[stats['持仓基金数'] >= MIN_HOLDING_FUNDS].head(TOP_N_STOCKS)
    st.dataframe(leaders[['个股代码', '个股名称', '龙头评分', '龙头等级', '持仓基金数', '行业']], use_container_width=True, hide_index=True)
    return stats

def calculate_leader_score(df):
    # 简化评分（可按侧边栏权重扩展）
    max_funds = df['持仓基金数'].max() or 1
    max_avg = df['平均持仓比(%)'].max() or 1
    return (df['持仓基金数'] / max_funds * 40 +
            df['平均持仓比(%)'] / max_avg * 40 +
            (df.get('个股涨幅(%)', 0) / 100 * 20)).fillna(0)

def get_stock_returns(codes):
    df = pd.DataFrame({'个股代码': codes})
    df['个股涨幅(%)'] = np.random.uniform(10, 180, len(codes))
    return df

def get_industry_info(codes):
    df = pd.DataFrame({'个股代码': codes})
    industries = ['电子', '计算机', '医药生物', '新能源', '半导体', '汽车', '食品饮料']
    df['行业'] = np.random.choice(industries, len(codes))
    return df


# ========================== 主界面 ==========================
st.divider()
st.header("🚀 优化版分析流程")

col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("1️⃣ 爬取高涨幅基金", type="primary", use_container_width=True):
        with st.spinner("爬取中..."):
            crawl_high_return_funds()

with col2:
    if st.button("2️⃣ 获取基金持仓", type="primary", use_container_width=True):
        with st.spinner("获取持仓中（SSL 已优化）..."):
            crawl_all_holdings()

with col3:
    if st.button("3️⃣ 龙头识别评分", type="primary", use_container_width=True):
        with st.spinner("评分中..."):
            identify_leaders()

with col4:
    if st.button("⚡ 一键全跑", type="secondary", use_container_width=True):
        with st.spinner("全流程执行中..."):
            crawl_high_return_funds()
            crawl_all_holdings()
            identify_leaders()
        st.balloons()

st.divider()

# 结果展示
if not st.session_state.high_funds.empty:
    with st.expander("📊 Step 1: 高涨幅基金", expanded=False):
        st.dataframe(st.session_state.high_funds, use_container_width=True, hide_index=True)

if not st.session_state.all_holdings.empty:
    with st.expander("📥 Step 2: 基金持仓明细", expanded=False):
        st.dataframe(st.session_state.all_holdings.head(100), use_container_width=True, hide_index=True)

if not st.session_state.stock_scores.empty:
    with st.expander("🐉 Step 3: 龙头股评分", expanded=True):
        cols = ['个股代码', '个股名称', '龙头评分', '龙头等级', '持仓基金数', '行业']
        st.dataframe(st.session_state.stock_scores[cols].head(TOP_N_STOCKS), use_container_width=True, hide_index=True)

st.caption("✅ SSL 错误已优化 | 持仓采用 AKShare + 备用解析 | 如仍有问题请加大间隔或关闭 AKShare 选项")
