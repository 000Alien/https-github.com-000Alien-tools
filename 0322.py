import pywencai
import pandas as pd

# 你的500只代码列表（示例，可从文件读取）
codes = ['600519', '000001', '300750', ...]  # 替换成你的完整列表，长度500

# 拼接成问财能识别的格式：代码用逗号分隔
code_str = ','.join(codes)

# 问财查询语句（核心在这里）
query_str = f"股票代码：{code_str}；所属同花顺概念"

# 执行查询
df = pywencai.get(question=query_str)

# 如果一次太多被限流，分批（每批150–200只）
# 比如：
# batches = [codes[i:i+150] for i in range(0, len(codes), 150)]
# 然后循环查询并concat

print(df.head())
df.to_excel('500只_同花顺概念.xlsx', index=False)