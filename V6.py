# streamlit run V6.py
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
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, ssl_context=ctx,
                                       **pool_kwargs)


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
st.markdown("**持仓解析最终版 V6**：支持自定义爬取周期 | 防刷新缓存 | 数据持久化")

# 初始化会话状态
default_state = {
    'high_funds': pd.DataFrame(),
    'all_holdings': pd.DataFrame(),
    'stock_scores': pd.DataFrame(),
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
DELAY_MIN = st.sidebar.slider("请求间隔(秒)", 1.0, 5.0, 4.0, 0.1)
TOP_N_STOCKS = st.sidebar.slider("显示 Top N 个股", 10, 100, 30)
MIN_HOLDING_FUNDS = st.sidebar.slider("龙头最少被持基金数", 1, 50, 8)

# ========== 爬取周期设置 ==========
st.sidebar.divider()
st.sidebar.header("📅 爬取周期设置")

period_type = st.sidebar.selectbox(
    "涨幅统计周期",
    options=["近1周", "近1个月", "近3个月", "近6个月", "近1年", "近2年", "近3年", "今年来", "成立来", "自定义"],
    index=4  # 默认近1年
)

# 东方财富网排序字段映射
period_sort_map = {
    "近1周": "1w",
    "近1个月": "1m",
    "近3个月": "3m",
    "近6个月": "6m",
    "近1年": "1nzf",
    "近2年": "2nzf",
    "近3年": "3nzf",
    "今年来": "jnzf",
    "成立来": "clnf",
}

if period_type == "自定义":
    st.sidebar.markdown("**自定义日期范围**")
    start_date = st.sidebar.date_input(
        "开始日期",
        value=datetime.now() - timedelta(days=365),
        max_value=datetime.now()
    )
    end_date = st.sidebar.date_input(
        "结束日期",
        value=datetime.now(),
        max_value=datetime.now()
    )
else:
    start_date = None
    end_date = None

USE_AKSHARE = st.sidebar.checkbox("优先使用 AKShare", value=True)

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


def create_download_buttons(df, title_prefix, key_prefix=""):
    if df.empty:
        return
    col1, col2 = st.columns(2)
    with col1:
        csv_data = convert_df_to_csv(df, f"{title_prefix}.csv")
        if csv_data:
            st.download_button(
                label="📥 下载 CSV",
                data=csv_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                key=f"{key_prefix}_{title_prefix}_csv"
            )
    with col2:
        excel_data = convert_df_to_excel(df, f"{title_prefix}.xlsx")
        if excel_data:
            st.download_button(
                label="📊 下载 Excel",
                data=excel_data,
                file_name=f"{title_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"{key_prefix}_{title_prefix}_excel"
            )


# ========================== 调试函数：查看返回的数据结构 ==========================
def debug_response_data(text):
    """调试函数：查看返回的数据结构"""
    match = re.search(r'var\s+rankData\s*=\s*(\{.*?\});', text, re.DOTALL | re.IGNORECASE)
    if match:
        data_str = match.group(1)
        # 只取前500字符用于调试
        return data_str[:500]
    return None


# ========================== 1. 爬取高涨幅基金（修复版）==========================
def crawl_high_return_funds(reset=False):
    """爬取高涨幅基金，支持断点续爬和防刷新"""
    
    # 重置进度
    if reset:
        st.session_state.crawl_progress = {'current_page': 1, 'total_funds': 0, 'is_running': False}
        st.session_state.high_funds = pd.DataFrame()
        st.session_state.cache_key = None
        st.session_state.last_crawl_time = None
    
    # 如果正在运行中，提示用户
    if st.session_state.crawl_progress.get('is_running', False):
        st.warning("⚠️ 爬取任务正在后台运行中，请稍候...")
        return st.session_state.high_funds
    
    # 生成缓存键
    if period_type == "自定义":
        cache_key = f"custom_{start_date}_{end_date}_{RETURN_THRESHOLD}"
        period_display = f"{start_date} 至 {end_date}"
        col_name = f'{start_date}至{end_date}涨幅(%)'
    else:
        cache_key = f"{period_type}_{RETURN_THRESHOLD}"
        period_display = period_type
        col_name = f'{period_type}涨幅(%)'
    
    # 检查缓存
    if (not reset and not st.session_state.high_funds.empty and 
        st.session_state.get('cache_key') == cache_key):
        st.info(f"📦 使用缓存数据（上次爬取时间：{st.session_state.last_crawl_time}）")
        st.success(f"✅ 已有 {len(st.session_state.high_funds)} 只基金数据")
        return st.session_state.high_funds
    
    funds = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    st.info(f"🚀 开始爬取{period_display}涨幅 ≥ {RETURN_THRESHOLD}% 的基金...")
    
    # 标记运行状态
    st.session_state.crawl_progress['is_running'] = True
    
    page = 1
    session = create_retry_session()
    first_page_data = None  # 用于调试
    
    with st.spinner(f"正在爬取{period_display}数据，请勿刷新页面..."):
        while page <= MAX_PAGES:
            # 构建 URL
            if period_type == "自定义" and start_date and end_date:
                sd = start_date.strftime('%Y-%m-%d')
                ed = end_date.strftime('%Y-%m-%d')
                url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc=zdf&st=desc&sd={sd}&ed={ed}&pi={page}&pn=100&v={random.random()}"
            else:
                sort_field = period_sort_map.get(period_type, "1nzf")
                url = f"http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=all&sc={sort_field}&st=desc&pi={page}&pn=100&v={random.random()}"
            
            try:
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                text = resp.text
                
                # 保存第一页数据用于调试
                if page == 1 and first_page_data is None:
                    first_page_data = text
                
                # 解析数据
                match = re.search(r'var\s+rankData\s*=\s*(\{.*?\});', text, re.DOTALL | re.IGNORECASE)
                if not match:
                    st.warning(f"第 {page} 页：未找到 rankData")
                    break
                
                data_str = match.group(1)
                
                # 尝试多种解析方式
                data = None
                try:
                    # 方式1：正则添加引号后解析
                    fixed_str = re.sub(r'([a-zA-Z_]\w*)\s*:', r'"\1":', data_str)
                    data = json.loads(fixed_str)
                except:
                    pass
                
                if data is None:
                    try:
                        # 方式2：demjson3
                        data = demjson3.decode(data_str)
                    except:
                        pass
                
                if data is None:
                    try:
                        # 方式3：eval
                        data_str_fixed = data_str.replace('null', 'None').replace('true', 'True').replace('false', 'False')
                        data = eval(data_str_fixed)
                    except:
                        pass
                
                if data is None:
                    st.warning(f"第 {page} 页：数据解析失败")
                    break
                
                # 获取 datas 数组
                datas = data.get('datas') or data.get('data') or []
                if not datas:
                    st.warning(f"第 {page} 页：无数据")
                    break
                
                total_pages = data.get('allPages') or data.get('allNum') or data.get('pages') or MAX_PAGES
                
                # 解析每一条数据
                for item in datas:
                    if isinstance(item, str):
                        fields = [x.strip() for x in item.split(',')]
                        if len(fields) < 8:
                            continue
                        
                        # 东方财富网返回字段顺序（验证后）：
                        # 0: 基金代码, 1: 基金名称, 2: 净值, 3: 日涨幅, 
                        # 4: 近1周, 5: 近1月, 6: 近3月, 7: 近6月, 
                        # 8: 近1年, 9: 近2年, 10: 近3年, 11: 今年来, 12: 成立来
                        
                        # 根据周期类型选择对应的涨幅字段
                        field_index_map = {
                            "近1周": 4,
                            "近1个月": 5,
                            "近3个月": 6,
                            "近6个月": 7,
                            "近1年": 8,
                            "近2年": 9,
                            "近3年": 10,
                            "今年来": 11,
                            "成立来": 12,
                        }
                        
                        if period_type == "自定义":
                            # 自定义周期使用的是区间涨幅，字段位置可能不同
                            # 自定义返回的数据中，涨幅可能在字段6
                            ret_index = 6
                        else:
                            ret_index = field_index_map.get(period_type, 8)
                        
                        if len(fields) <= ret_index:
                            continue
                        
                        try:
                            ret_str = fields[ret_index].replace('%', '').strip()
                            ret = float(ret_str) if ret_str else 0
                            
                            if ret >= RETURN_THRESHOLD:
                                funds.append({
                                    '基金代码': fields[0],
                                    '基金名称': fields[1],
                                    col_name: round(ret, 2),
                                    '净值': fields[2] if len(fields) > 2 else '',
                                    '日涨幅': fields[3] if len(fields) > 3 else ''
                                })
                        except (ValueError, IndexError) as e:
                            continue
                
                progress = min(int(page / MAX_PAGES * 100), 100)
                progress_bar.progress(progress)
                status_text.text(f"第 {page}/{min(total_pages, MAX_PAGES)} 页完成 | 已找到 {len(funds)} 只基金")
                
                # 保存进度
                st.session_state.crawl_progress['current_page'] = page
                st.session_state.crawl_progress['total_funds'] = len(funds)
                
                if page >= int(total_pages):
                    break
                page += 1
                time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.0))
                
            except Exception as e:
                st.error(f"第 {page} 页请求失败: {str(e)}")
                page += 1
                continue
    
    st.session_state.crawl_progress['is_running'] = False
    st.session_state.last_crawl_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    st.session_state.cache_key = cache_key
    
    df = pd.DataFrame(funds)
    st.session_state.high_funds = df
    
    if not df.empty:
        st.success(f"✅ 爬取完成！共找到 **{len(df)}** 只基金（{period_display}涨幅 ≥ {RETURN_THRESHOLD}%）")
        
        # 显示简化的表格
        display_df = df[['基金代码', '基金名称', col_name]]
        st.dataframe(display_df, use_container_width=True, hide_index=True)
        
        st.markdown("**💾 下载高涨幅基金列表**")
        create_download_buttons(df, "高涨幅基金列表", "crawl")
        
        with st.expander("📋 查看基金代码列表（可复制）"):
            codes = df['基金代码'].tolist()
            st.code(", ".join(codes), language="text")
            st.info(f"共 {len(codes)} 只基金代码")
        
        col_stat1, col_stat2, col_stat3 = st.columns(3)
        with col_stat1:
            st.metric("基金数量", len(df))
        with col_stat2:
            avg_return = df[col_name].mean()
            st.metric("平均涨幅", f"{avg_return:.1f}%")
        with col_stat3:
            max_return = df[col_name].max()
            st.metric("最大涨幅", f"{max_return:.1f}%")
    else:
        st.warning(f"未找到符合条件的基金（{period_display}涨幅 ≥ {RETURN_THRESHOLD}%）")
        
        # 显示调试信息
        with st.expander("🔧 调试信息", expanded=True):
            st.info("可能原因：")
            st.write("1. 涨幅阈值设置过高")
            st.write("2. 选择的周期内没有符合条件的基金")
            st.write("3. 数据接口字段位置需要调整")
            
            if first_page_data:
                st.text("第一页数据预览（前800字符）：")
                st.code(first_page_data[:800], language="javascript")
    
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
    return None


# ========================== 获取基金持仓 ==========================
def get_fund_holdings(fund_code: str, fund_name: str):
    session = create_retry_session()
    fund_code = str(fund_code).strip().zfill(6)
    
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
                        return result_df
        except:
            pass
    
    try:
        urls = [
            f"https://fundf10.eastmoney.com/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&topline=50&rt=0.{random.randint(100000, 999999)}",
            f"https://fund.eastmoney.com/f10/FundArchivesDatas.aspx?code={fund_code}&type=jjcc&rt=0.{random.randint(100000, 999999)}",
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
                                    if len(cells) >= 4:
                                        stock_code, stock_name, ratio = None, None, None
                                        for cell in cells:
                                            link = cell.find('a')
                                            if link and 'code=' in link.get('href', ''):
                                                stock_code = re.search(r'code=(\d+)', link['href']).group(1)
                                                stock_name = link.text.strip()
                                                break
                                        if not stock_code:
                                            for cell in cells:
                                                m = re.search(r'\b(\d{6})\b', cell.text)
                                                if m:
                                                    stock_code = m.group(1)
                                                    break
                                        if not stock_name and len(cells) > 0:
                                            stock_name = cells[0].text.strip()
                                        if len(cells) > 2:
                                            try:
                                                ratio = float(cells[2].text.strip().replace('%', ''))
                                            except:
                                                pass
                                        if stock_code and ratio and ratio > 0:
                                            holdings.append({
                                                '基金代码': fund_code, '基金名称': fund_name,
                                                '个股代码': stock_code, '个股名称': stock_name or f"股票{stock_code}",
                                                '占基金净值比例(%)': ratio
                                            })
                                if holdings:
                                    return pd.DataFrame(holdings).drop_duplicates(subset=['个股代码'])
                    break
            except:
                continue
    except:
        pass
    return pd.DataFrame()


# ========================== 批量获取 ==========================
def crawl_all_holdings_from_list(fund_list_df):
    if fund_list_df.empty or '基金代码' not in fund_list_df.columns:
        st.error("必须包含 '基金代码' 列")
        return pd.DataFrame()
    
    all_hold = []
    fund_holdings_dict = {}
    progress_bar = st.progress(0)
    status = st.empty()
    total = len(fund_list_df)
    st.info(f"准备处理 {total} 只基金")
    
    with st.spinner(f"正在获取 {total} 只基金持仓，请勿刷新页面..."):
        for i, row in fund_list_df.iterrows():
            code = str(row['基金代码']).strip().zfill(6)
            name = row.get('基金名称', f"基金{code}")
            status.text(f"[{i + 1}/{total}] {code} {name}")
            hold_df = get_fund_holdings(code, name)
            if not hold_df.empty:
                all_hold.append(hold_df)
                fund_holdings_dict[code] = hold_df
            progress_bar.progress(int((i + 1) / total * 100))
            time.sleep(random.uniform(DELAY_MIN, DELAY_MIN + 1.8))
    
    if all_hold:
        combined = pd.concat(all_hold, ignore_index=True)
        combined = combined.drop_duplicates(subset=['基金代码', '个股代码'])
        st.session_state.all_holdings = combined
        st.session_state.fund_holdings_dict = fund_holdings_dict
        st.success(f"✅ 持仓获取完成！共 {len(combined)} 条记录")
        st.dataframe(combined.head(100), use_container_width=True)
        st.markdown("**💾 下载所有基金持仓**")
        create_download_buttons(combined, "全部基金持仓汇总", "batch")
        return combined
    return pd.DataFrame()


# ========================== 文件读取 ==========================
def read_fund_file(uploaded_file):
    if uploaded_file is None:
        return pd.DataFrame()
    try:
        if uploaded_file.name.endswith('.csv'):
            for enc in ['utf-8-sig', 'gbk', 'gb2312', 'utf-8']:
                try:
                    uploaded_file.seek(0)
                    df = pd.read_csv(uploaded_file, encoding=enc)
                    st.success(f"✅ CSV读取成功")
                    return df
                except:
                    continue
            st.error("编码无法识别")
            return pd.DataFrame()
        else:
            return pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"读取失败: {str(e)}")
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
    max_funds, max_avg, max_total = stats['持仓基金数'].max(), stats['平均持仓比(%)'].max(), stats['总持仓比(%)'].max()
    stats['基金数得分'] = (stats['持仓基金数'] / max_funds * 40) if max_funds > 0 else 0
    stats['平均持仓得分'] = (stats['平均持仓比(%)'] / max_avg * 30) if max_avg > 0 else 0
    stats['总持仓得分'] = (stats['总持仓比(%)'] / max_total * 30) if max_total > 0 else 0
    stats['龙头评分'] = stats['基金数得分'] + stats['平均持仓得分'] + stats['总持仓得分']
    stats = stats.sort_values('龙头评分', ascending=False)
    st.session_state.stock_scores = stats
    st.success(f"✅ 龙头评分完成！共识别 {len(stats)} 只候选股")
    display_cols = ['个股代码', '个股名称', '龙头评分', '持仓基金数', '平均持仓比(%)', '总持仓比(%)']
    st.dataframe(stats[display_cols].head(TOP_N_STOCKS), use_container_width=True, hide_index=True)
    st.markdown("**💾 下载龙头股数据**")
    create_download_buttons(stats[display_cols].head(TOP_N_STOCKS), "龙头股评分排行", "leader")


# ========================== 主界面 ==========================
st.divider()
st.header("🚀 核心功能")

# 显示当前爬取周期和缓存状态
col_period1, col_period2, col_period3 = st.columns(3)
with col_period1:
    if period_type == "自定义":
        st.info(f"📅 周期：{start_date} 至 {end_date}")
    else:
        st.info(f"📅 周期：{period_type}")
with col_period2:
    st.info(f"📊 阈值：≥ {RETURN_THRESHOLD}%")
with col_period3:
    if st.session_state.last_crawl_time:
        st.info(f"💾 缓存：{st.session_state.last_crawl_time}")
    else:
        st.info(f"💾 状态：未缓存")

# 四个主要功能按钮
col1, col2, col3, col4 = st.columns(4)
with col1:
    if st.button("1️⃣ 爬取高涨幅基金", type="primary", use_container_width=True):
        crawl_high_return_funds(reset=False)
with col2:
    if st.button("🔄 强制重新爬取", type="secondary", use_container_width=True):
        crawl_high_return_funds(reset=True)
with col3:
    if st.button("2️⃣ 从结果获取持仓", type="primary", use_container_width=True):
        if st.session_state.high_funds.empty:
            st.error("请先执行步骤 1")
        else:
            crawl_all_holdings_from_list(st.session_state.high_funds)
with col4:
    if st.button("3️⃣ 龙头识别评分", type="primary", use_container_width=True):
        with st.spinner("评分中..."):
            identify_leaders()

# 清空按钮
st.divider()
st.subheader("🗑️ 数据管理")
col_clear1, col_clear2, col_clear3, col_clear4 = st.columns(4)
with col_clear1:
    if st.button("清空基金列表", use_container_width=True):
        st.session_state.high_funds = pd.DataFrame()
        st.session_state.crawl_progress = {'current_page': 1, 'total_funds': 0, 'is_running': False}
        st.session_state.last_crawl_time = None
        st.session_state.cache_key = None
        st.success("已清空")
with col_clear2:
    if st.button("清空持仓明细", use_container_width=True):
        st.session_state.all_holdings = pd.DataFrame()
        st.session_state.fund_holdings_dict = {}
        st.success("已清空")
with col_clear3:
    if st.button("清空评分结果", use_container_width=True):
        st.session_state.stock_scores = pd.DataFrame()
        st.success("已清空")
with col_clear4:
    if st.button("🗑️ 清空全部数据", type="secondary", use_container_width=True):
        st.session_state.high_funds = pd.DataFrame()
        st.session_state.all_holdings = pd.DataFrame()
        st.session_state.stock_scores = pd.DataFrame()
        st.session_state.fund_holdings_dict = {}
        st.session_state.crawl_progress = {'current_page': 1, 'total_funds': 0, 'is_running': False}
        st.session_state.last_crawl_time = None
        st.session_state.cache_key = None
        st.success("已清空全部数据")

# 手动导入
with st.expander("📋 手动导入基金列表", expanded=False):
    uploaded_file = st.file_uploader("上传 Excel/CSV（需含'基金代码'列）", type=['xlsx', 'xls', 'csv'])
    manual_input = st.text_area("或粘贴基金代码（每行一个）", height=100)
    if st.button("从导入列表获取持仓"):
        fund_list = pd.DataFrame()
        if uploaded_file:
            fund_list = read_fund_file(uploaded_file)
        elif manual_input.strip():
            data = []
            for line in manual_input.strip().split('\n'):
                parts = line.split(maxsplit=1)
                code = parts[0].strip()
                name = parts[1].strip() if len(parts) > 1 else f"基金{code}"
                data.append({'基金代码': code, '基金名称': name})
            fund_list = pd.DataFrame(data)
        if not fund_list.empty:
            crawl_all_holdings_from_list(fund_list)

# 单只基金下载
with st.expander("💾 单只基金下载", expanded=False):
    fund_input = st.text_input("输入基金代码", placeholder="022364")
    if st.button("获取并下载"):
        if fund_input:
            with st.spinner("获取中..."):
                holdings = get_fund_holdings(fund_input.strip().zfill(6), f"基金{fund_input}")
                if not holdings.empty:
                    st.dataframe(holdings)
                    create_download_buttons(holdings, f"基金_{fund_input}_持仓", "single")
                else:
                    st.error("未获取到数据")

# 结果展示
st.divider()
st.subheader("📊 数据结果")

if not st.session_state.high_funds.empty:
    with st.expander("📈 高涨幅基金列表", expanded=False):
        st.dataframe(st.session_state.high_funds, use_container_width=True, hide_index=True)
        create_download_buttons(st.session_state.high_funds, "高涨幅基金列表", "show_high")

if not st.session_state.all_holdings.empty:
    with st.expander("📥 基金持仓明细", expanded=True):
        st.dataframe(st.session_state.all_holdings, use_container_width=True, hide_index=True)
        create_download_buttons(st.session_state.all_holdings, "基金持仓明细", "show_hold")

if not st.session_state.stock_scores.empty:
    with st.expander("🐉 龙头股评分排行", expanded=True):
        cols = ['个股代码', '个股名称', '龙头评分', '持仓基金数', '平均持仓比(%)', '总持仓比(%)']
        st.dataframe(st.session_state.stock_scores[cols].head(TOP_N_STOCKS), use_container_width=True, hide_index=True)
        create_download_buttons(st.session_state.stock_scores[cols].head(TOP_N_STOCKS), "龙头股排行", "show_score")

st.caption("✅ V6 增强版 | 修复周期选择问题 | 支持近1周/1月/3月/6月/1年/2年/3年/今年来/成立来/自定义 | 防刷新缓存")
