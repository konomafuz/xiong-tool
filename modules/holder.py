import requests
import pandas as pd
import time
import requests


def get_all_holders(chain_id, token_address, timestamp, top_n):
    """获取指定时间点的前N大持仓地址"""
    url = "https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
    
    # 准备基本参数（包含时间戳）
    params = {
        "chainId": chain_id,
        "tokenAddress": token_address,
        "t": timestamp,
        "limit": min(top_n, 100),  # 每页最多100条
        "offset": 0
    }
    
    holders = []
    page_count = 0
    max_pages = 20  # 防止无限循环
    
    try:
        while len(holders) < top_n and page_count < max_pages:
            # 发送API请求
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
            }
            response = requests.get(url, params=params, headers=headers, timeout=10)
            # 检查HTTP状态
            if response.status_code != 200:
                print(f"API响应错误: HTTP {response.status_code}")
                break
                
            # 尝试解析JSON
            try:
                json_data = response.json()
            except ValueError:
                print("JSON解析失败")
                break
                
            # 检查API返回的数据结构
            if not isinstance(json_data, dict) or not json_data:
                print("API返回了非预期的数据结构")
                break
                
            # 处理API返回的错误代码
            if json_data.get("code") != 0:
                print(f"API错误: 代码 {json_data.get('code')}, 消息: {json_data.get('msg')}")
                break
                
            # 获取持有者数据
            data = json_data.get("data", {})
            new_holders = data.get("holderRankingList", [])
            
            if not new_holders:
                print(f"没有更多持仓数据，时间戳: {timestamp}")
                break
                
            holders.extend(new_holders)
            params["offset"] += len(new_holders)
            page_count += 1
            
            # 检查是否还有更多数据
            if len(new_holders) < params["limit"]:
                break
                
            # 避免请求过快
            time.sleep(1)
            
            # 动态调整limit确保不超过top_n
            remaining = top_n - len(holders)
            if remaining < params["limit"]:
                params["limit"] = remaining
    
    except Exception as e:
        print(f"请求失败: {str(e)}")
        return pd.DataFrame()
    
    # 处理获取的数据
    if not holders:
        print(f"没有找到任何持仓数据，时间戳: {timestamp}")
        return pd.DataFrame()
    
    # 创建DataFrame并提取关键字段
    try:
        df = pd.DataFrame(holders)
        df = df[["holderWalletAddress", "holdAmount", "holdAmountPercentage"]]
        df.columns = ["address", "balance", "percentage"]
        
        # 安全转换为数值类型
        df["balance"] = pd.to_numeric(df["balance"], errors="coerce")
        
        # 确保不超过请求数量
        return df.head(top_n)
        
    except KeyError as e:
        print(f"数据字段缺失: {str(e)}")
        return pd.DataFrame()