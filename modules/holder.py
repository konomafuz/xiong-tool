import pandas as pd
import requests
import time
from utils import fetch_data


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
            response = fetch_data(url, params)
            if not response or 'data' not in response:
                break
                
            page_data = response['data']
            if not page_data:
                break
                
            holders.extend(page_data)
            
            # 如果这一页的数据少于limit，说明已经到最后一页
            if len(page_data) < params['limit']:
                break
                
            # 更新offset到下一页
            params['offset'] += params['limit']
            page_count += 1
            
            # 避免请求过快
            time.sleep(0.5)
    
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
        df["percentage"] = pd.to_numeric(df["percentage"], errors="coerce")
        
        # 确保不超过请求数量
        df = df.head(top_n)
        
        return df
        
    except KeyError as e:
        print(f"数据字段缺失: {str(e)}")
        return pd.DataFrame()

def get_multiple_snapshots(chain_id, token_address, timestamps, top_n=50):
    """获取多个时间点的持仓快照"""
    all_snapshots = []
    
    for timestamp in timestamps:
        df = get_all_holders(chain_id, token_address, timestamp, top_n)
        if not df.empty:
            df['timestamp'] = timestamp
            df['snapshot_time'] = pd.to_datetime(timestamp, unit='ms')
            all_snapshots.append(df)
        time.sleep(1)  # 避免请求过快
    
    if all_snapshots:
        return pd.concat(all_snapshots, ignore_index=True)
    else:
        return pd.DataFrame()