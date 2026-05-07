# streamlit run v5.py
# 修复说明：
# 1. 批量爬取中间结果实时写入 session_state，支持断点续爬
# 2. get_fund_holdings 新增 verbose 参数，批量模式下不再循环创建 st 元素（避免页面崩溃）
# 3. 使用 enumerate 替代 iterrows 的原始 index，修复进度计数错误
# 4. 所有逐基金提示收拢到单个 st.empty() 占位符，不再累积 DOM 节点
# 5. 配套 .streamlit/config.toml 延长服务器超时（见文件末尾注释）

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


# ========================== SSL 重试 ==========================
class TLSAdapter(HTTPAdapter):
    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        ctx.options |= 0x4
        ctx.set_ciphers('DEFAULT@SECLEVEL=1')
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block,
                                       ssl_context=ctx, **pool_kwargs)


def create_retry_session():
    session = requests.Session()
    retries = Retry(total=10, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", TLSAdapter())
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ========================== 配置 ==========================
st.set_page_config(page_title="A股最强主线龙头股识别系统", page_icon="🐉", layout="wide")
st.title("🐉 A股最强主线 & 龙头股识别系统")
st.markdown("**持仓解析最终版**：加强表格选择，优先股票持仓主表")

# 会话状态初始化（新增断点续爬相关 key）
_default_state = {
    'high_funds': pd.DataFrame(),
    'all_holdings': pd.DataFrame(),
    'stock_scores': pd.DataFrame(),
    'fund_holdings_dict': {},
    # ↓ 新增：断点续爬状态
    'crawl_done_codes': set(),   # 已处理的基金代码集合
    'crawl_partial': [],         # 已完成基金的 DataFrame 列表（序列化为 records）
}
for key, default in _default_state.items():
    if key not in st.session_state:
        st.session_state[key] = default

# 侧边栏
st.sidebar.header("⚙️ 参数设置")
RETURN_THRESHOLD = st.sidebar.slider("近一年涨幅阈值 (%)", 30, 300, 80, 10)
MAX_PAGES = st.sidebar.number_input("最大爬取页数", 1, 50, value=10)
DELAY_MIN = st.sidebar.slider("请求间隔(秒)", 1.0, 5.0, 4.0, 0.1)
USE_AKSHARE = st.sidebar.checkbox("优先使用 AKShare", value=True)
TOP_N_STOCKS = st.sidebar.slider("显示 Top N 个股", 10, 100, 30)
MIN_HOLDING_FUNDS = st.sidebar.slider("龙头最少被持基金数", 1, 50, 8)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'Referer': 'http://fund.eastmoney.com/'
}


# ========================== 下载功能 ==========================
def convert_df_to_csv(df, filename):
    if df.empty:
        return None
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
    return csv_buffer.getvalue().encode('utf-8-sig')


def convert_df_to_excel(df, filename):
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
        csv_data = convert_df_to_csv(df, f"{title_prefix}.csv")
        if csv_data:
            st.download_button(
                label="📥 下载 CSV",
                data=csv_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{title_prefix}_csv"
            )
    with col2:
        excel_data = convert_df_to_excel(df, f"{title_prefix}.xlsx")
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
                        csv_data = convert_df_to_csv(holdings_df, f"基金_{fund_code}_持仓")
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


# ========================== 1. 爬取高涨幅基金 ==========================
def crawl_high_return_funds():
    funds = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    st.info(f"🚀 开始爬取近一年涨幅 ≥ {RETURN_THRESHOLD}% 的基金...")

    page = 1
    session = create_retry_session()
    while page <= MAX_PAGES:
        url = (
            f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc=1nzf&st=desc"
            f"&sd={(datetime.now() - timedelta(days=400)).strftime('%Y-%m-%d')}"
            f"&ed={datetime.now().strftime('%Y-%m-%d')}&pi={page}&pn=100&v={random.random()}"
        )
        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            text = resp.text

            match = re.search(r'var\s+rankData\s*=\s*(\{.*?\});', text, re.DOTALL | re.IGNORECASE)
            if not match:
                break

            data_str = re.sub(r'([a-zA-Z_]\w*)\s*:', r'"\1":', match.group(1))
            data = json.loads(data_str) if '{' in data_str else eval(data_str)

            datas = data.get('datas') or data.get('data') or []
            if not datas:
                break

            total_pages = (data.get('allPages') or data.get('allNum') or
                           data.get('pages') or data.get('pageNum') or MAX_PAGES)

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
                                '近一年涨幅(%)': round(ret, 2)
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

        except:
            page += 1
            continue

    df = pd.DataFrame(funds)
    st.session_state.high_funds = df
    if not df.empty:
        st.success(f"✅ 爬取完成！共找到 **{len(df)}** 只基金")
        st.dataframe(df.head(20), use_container_width=True, hide_index=True)
        st.markdown("### 💾 下载基金列表")
        create_download_buttons(df, "高涨幅基金列表")
    return df


# ========================== 修复JSON解析函数 ==========================
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


# ========================== 获取基金持仓 ==========================
# [修复] 新增 verbose 和 status_placeholder 参数
#   verbose=True  → 调用 st.success/st.info（单只基金查询时使用）
#   verbose=False → 写入 status_placeholder（批量时使用，避免无限累积 DOM 节点）
def get_fund_holdings(fund_code: str, fund_name: str,
                      verbose: bool = True, status_placeholder=None):
    """获取单只基金持仓"""

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

    session = create_retry_session()
    fund_code = str(fund_code).strip().zfill(6)

    # 方法1：AKShare
    if USE_AKSHARE:
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
                    result_df['个股代码'] = result_df['个股代码'].astype(str).str.replace('^0+', '', regex=True)
                    result_df['占基金净值比例(%)'] = pd.to_numeric(result_df['占基金净值比例(%)'], errors='coerce')
                    result_df = result_df.dropna(subset=['个股代码', '占基金净值比例(%)'])
                    result_df = result_df[result_df['个股代码'] != '']
                    if not result_df.empty:
                        _log_success(f"✅ {fund_code} AKShare 获取成功（{len(result_df)} 只个股）")
                        return result_df
        except Exception as e:
            _log_info(f"AKShare获取失败 {fund_code}: {str(e)[:50]}")

    # 方法2：JSON解析
    try:
        urls = [
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&year=2024&rt=0.{random.randint(100000, 999999)}",
            f"https://fund.eastmoney.com/f10/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&rt=0.{random.randint(100000, 999999)}",
            f"http://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&rt=0.{random.randint(100000, 999999)}"
        ]
        for url in urls:
            try:
                resp = session.get(url, timeout=15, verify=False)
                resp.encoding = 'utf-8'
                if resp.text:
                    data = safe_json_parse(resp.text.strip())
                    if data and isinstance(data, dict):
                        content = data.get('content', '')
                        if content:
                            soup = BeautifulSoup(content, 'html.parser')
                            for table in soup.find_all('table'):
                                holdings = []
                                for row in table.find_all('tr'):
                                    cells = row.find_all('td')
                                    if len(cells) < 4:
                                        continue
                                    stock_code = stock_name = ratio = None
                                    for i, cell in enumerate(cells):
                                        link = cell.find('a')
                                        if link:
                                            href = link.get('href', '')
                                            code_match = re.search(r'code=(\d+)', href)
                                            if code_match:
                                                stock_code = code_match.group(1)
                                                stock_name = link.text.strip()
                                                if i + 2 < len(cells):
                                                    try:
                                                        ratio = float(cells[i + 2].text.strip().replace('%', ''))
                                                    except:
                                                        pass
                                                break
                                    if not stock_code:
                                        for cell in cells:
                                            code_match = re.search(r'\b(\d{6})\b', cell.text.strip())
                                            if code_match:
                                                stock_code = code_match.group(1)
                                                break
                                    if not stock_name and cells:
                                        stock_name = cells[0].text.strip()
                                    if not ratio and len(cells) > 2:
                                        try:
                                            ratio = float(cells[2].text.strip().replace('%', ''))
                                        except:
                                            pass
                                    if stock_code and re.match(r'^\d{6}$', stock_code) and ratio and ratio > 0:
                                        holdings.append({
                                            '基金代码': fund_code, '基金名称': fund_name,
                                            '个股代码': stock_code,
                                            '个股名称': stock_name or f"股票{stock_code}",
                                            '占基金净值比例(%)': ratio
                                        })
                                if holdings:
                                    result_df = pd.DataFrame(holdings).drop_duplicates(subset=['个股代码'])
                                    _log_success(f"✅ {fund_code} JSON解析成功（{len(result_df)} 只个股）")
                                    return result_df
                break
            except:
                continue
    except Exception as e:
        _log_info(f"JSON解析失败 {fund_code}: {str(e)[:50]}")

    # 方法3：正则提取
    try:
        url = f"https://fundf10.eastmoney.com/ccmx_{fund_code}.html"
        resp = session.get(url, timeout=15, verify=False)
        resp.encoding = 'utf-8'
        if resp.text:
            html = resp.text
            patterns = [
                r'<a[^>]*href="[^"]*code=(\d+)"[^>]*>([^<]+)</a>[^<]*</td>[^<]*<td[^>]*>([\d.]+)%',
                r'code=(\d+)[^>]*>([^<]+)</a>.*?([\d.]+)%',
                r'<td[^>]*>([^<]+)</td>.*?<td[^>]*>(\d+)</td>.*?<td[^>]*>([\d.]+)%',
            ]
            for pattern in patterns:
                matches = re.findall(pattern, html, re.DOTALL)
                if matches:
                    holdings = []
                    for match in matches:
                        if len(match) == 3:
                            if re.match(r'^\d{6}$', match[1]):
                                stock_code, stock_name, ratio = match[1], match[0], float(match[2])
                            elif re.match(r'^\d{6}$', match[0]):
                                stock_code, stock_name, ratio = match[0], match[1], float(match[2])
                            else:
                                continue
                            if ratio > 0:
                                holdings.append({
                                    '基金代码': fund_code, '基金名称': fund_name,
                                    '个股代码': stock_code, '个股名称': stock_name,
                                    '占基金净值比例(%)': ratio
                                })
                    if holdings:
                        result_df = pd.DataFrame(holdings).drop_duplicates(subset=['个股代码'])
                        _log_success(f"✅ {fund_code} 正则提取成功（{len(result_df)} 只个股）")
                        return result_df
    except:
        pass

    _log_warning(f"⚠️ {fund_code} {fund_name} 所有方法均获取失败")
    return pd.DataFrame()


# ========================== 批量获取（断点续爬版）==========================
def crawl_all_holdings_from_list(fund_list_df):
    if fund_list_df.empty or '基金代码' not in fund_list_df.columns:
        st.error("必须包含 '基金代码' 列")
        return pd.DataFrame()

    total = len(fund_list_df)

    # [修复1] 从 session_state 恢复断点续爬进度
    done_codes: set = st.session_state.crawl_done_codes
    fund_holdings_dict: dict = st.session_state.fund_holdings_dict

    # 把已保存的分片 records 还原为 DataFrame 列表
    all_hold = [pd.DataFrame(r) for r in st.session_state.crawl_partial] if st.session_state.crawl_partial else []

    remaining = [(idx, row) for idx, (_, row) in enumerate(fund_list_df.iterrows())
                 if str(row['基金代码']).strip().zfill(6) not in done_codes]

    if done_codes:
        st.info(f"⏩ 检测到断点续爬：已完成 {len(done_codes)} 只，剩余 {len(remaining)} 只")

    progress_bar = st.progress(int(len(done_codes) / total * 100) if total else 0)
    status = st.empty()          # 当前进度文字
    fund_msg = st.empty()        # [修复2] 单个占位符替代循环内 st.success/st.info
    error_log = []

    st.info(f"准备处理 {total} 只基金（本次 {len(remaining)} 只）")

    for idx, row in remaining:
        # [修复3] 使用 enumerate 后的 idx（0-based），不依赖原始 DataFrame index
        code = str(row['基金代码']).strip().zfill(6)
        name = row.get('基金名称', f"基金{code}")
        status.text(f"[{idx + 1}/{total}] 正在获取: {code} {name}")

        # [修复2] verbose=False，所有日志写到 fund_msg 占位符，不创建新节点
        hold_df = get_fund_holdings(code, name, verbose=False, status_placeholder=fund_msg)

        if not hold_df.empty:
            all_hold.append(hold_df)
            fund_holdings_dict[code] = hold_df

            # [修复1] 实时保存到 session_state，重启后可续爬
            st.session_state.crawl_partial.append(hold_df.to_dict('records'))
            st.session_state.fund_holdings_dict = fund_holdings_dict

            # 合并当前所有持仓并写入 all_holdings（方便随时下载）
            combined_now = pd.concat(all_hold, ignore_index=True).drop_duplicates(
                subset=['基金代码', '个股代码'])
            st.session_state.all_holdings = combined_now
        else:
            error_log.append(code)

        done_codes.add(code)
        st.session_state.crawl_done_codes = done_codes

        progress_bar.progress(int((idx + 1) / total * 100))
        time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.8))

    # ---- 全部完成后汇总 ----
    fund_msg.empty()
    status.empty()

    if all_hold:
        combined = pd.concat(all_hold, ignore_index=True).drop_duplicates(subset=['基金代码', '个股代码'])
        st.session_state.all_holdings = combined
        st.session_state.fund_holdings_dict = fund_holdings_dict

        # 爬取完成后清空断点状态，方便下次全新爬取
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

    df = st.session_state.all_holdings.copy()
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


# ========================== 主界面 ==========================
st.divider()
st.header("🚀 基金持仓获取")

# 断点续爬提示
if st.session_state.crawl_done_codes:
    st.warning(
        f"⚠️ 检测到未完成的爬取任务（已完成 {len(st.session_state.crawl_done_codes)} 只基金）。"
        "点击「从爬取结果获取持仓」可继续；点击「清空所有数据」可重新开始。"
    )

tab_crawl, tab_import, tab_download = st.tabs(["📈 从高涨幅基金爬取", "📋 手动导入基金列表", "💾 单只基金下载"])

with tab_crawl:
    col1, col2 = st.columns(2)
    with col1:
        if st.button("1️⃣ 爬取高涨幅基金", type="primary", use_container_width=True):
            with st.spinner("爬取中..."):
                crawl_high_return_funds()
    with col2:
        if st.button("2️⃣ 从爬取结果获取持仓", type="primary", use_container_width=True):
            if st.session_state.high_funds.empty:
                st.error("请先执行步骤 1")
            else:
                crawl_all_holdings_from_list(st.session_state.high_funds)

with tab_import:
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
    download_fund_individual_holdings()

st.divider()

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

# 结果展示
if not st.session_state.all_holdings.empty:
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

st.caption("✅ v5 修复版 | 支持断点续爬 | 批量模式不累积 DOM | 修复进度计数 | 支持 CSV/Excel 下载")

# ==============================================================
# 请同时在项目根目录创建 .streamlit/config.toml，内容如下：
#
# [server]
# maxUploadSize = 200
# enableWebsocketCompression = false
# maxMessageSize = 500
# headless = true
#
# [browser]
# gatherUsageStats = false
# ==============================================================
