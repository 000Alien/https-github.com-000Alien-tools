import yfinance as yf
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_google_genai import ChatGoogleGenerativeAI
import requests
import json

# 配置区域
TAVILY_API_KEY = "你的Tavily密钥"
GOOGLE_API_KEY = "你的Google密钥"
DINGTALK_WEBHOOK = "你的钉钉Webhook地址"


class ResourceAgent:
    def __init__(self):
        self.llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", google_api_key=GOOGLE_API_KEY)
        self.search = TavilySearchResults(api_key=TAVILY_API_KEY, k=5)

    def get_market_data(self):
        """抓取关键商品及A股关联标的价格"""
        symbols = {
            "黄金(COMEX)": "GC=F",
            "白银(COMEX)": "SI=F",
            "伦铜(LME)": "HG=F",
            "紫金矿业": "601899.SS",
            "洛阳钼业": "603993.SS",
            "金徽股份": "603132.SS"
        }
        prices = {}
        for name, sym in symbols.items():
            ticker = yf.Ticker(sym)
            prices[name] = ticker.fast_info['last_price']
        return prices

    def get_summit_news(self):
        """定向检索：2026美中峰会对有色金属、出口配额、地缘溢价的影响"""
        query = "2026 US-China Summit impacts on Copper Zinc Gold Silver trade policies and mining industry"
        return self.search.run(query)

    def analyze_and_report(self):
        prices = self.get_market_data()
        news = self.get_summit_news()

        prompt = f"""
        # 2026 有色金属专题简报 #

        【当前市价】: {prices}
        【国际动态精华】: {news}

        作为资深有色金属分析师，请结合2026年5月即将举行的美中峰会，给出以下深度分析：
        1. 宏观预期：峰会是否可能缓解出口限制？对铜、锌等工业金属的需求提振如何？
        2. 避险逻辑：地缘政治波动对金银价格的支撑位预测。
        3. 标的影响：重点点评对 A 股资源类标的（如紫金、洛钼）的情绪影响。
        请使用 Markdown 格式输出。
        """

        report = self.llm.invoke(prompt).content
        self.push_to_dingtalk(report)

    def push_to_dingtalk(self, content):
        """推送至钉钉，已处理 Windows 字符编码问题"""
        headers = {'Content-Type': 'application/json'}
        data = {
            "msgtype": "markdown",
            "markdown": {
                "title": "2026资源类深度研报",
                "text": content
            }
        }
        requests.post(DINGTALK_WEBHOOK, data=json.dumps(data), headers=headers)


if __name__ == "__main__":
    agent = ResourceAgent()
    agent.analyze_and_report()