import time
import pandas as pd
from utils import fetch_data
import json
import requests
import pandas as pd
import time


def fetch_top_traders(token_contract_address: str, chain_id=501, max_records=1000):
    """获取指定代币的顶级盈利交易者（分页获取所有记录）"""
    all_traders = []
    page_size = 100  # 每次请求100条记录
    offset = 0
    
    while True:
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
        params = {
            "chainId": chain_id,
            "tokenContractAddress": token_contract_address,
            "offset": offset,
            "limit": page_size,
            "t": int(time.time() * 1000)  # 添加时间戳避免缓存
        }
        
        data = fetch_data(url, params)
        if not data:
            break
            
        traders = data.get('list', [])
        if not traders:
            break
            
        all_traders.extend(traders)
        offset += len(traders)
        
        # 停止条件：达到最大记录数或最后一页
        if len(traders) < page_size or len(all_traders) >= max_records:
            break
    
    return all_traders

def fetch_address_token_list(wallet_address: str, chain_id=501, max_records=1000):
    """获取指定钱包地址的所有顶级盈利代币（分页获取所有记录）"""
    all_tokens = []
    page_size = 100  # 每次请求100条记录
    offset = 0
    
    while True:
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        params = {
            "walletAddress": wallet_address,
            "chainId": chain_id,
            "isAsc": "false",
            "sortType": "1",
            "offset": offset,
            "limit": page_size,
            "t": int(time.time() * 1000)  # 添加时间戳避免缓存
        }
        
        data = fetch_data(url, params)
        if not data:
            break
            
        tokens = data.get('tokenList', [])
        if not tokens:
            break
            
        all_tokens.extend(tokens)
        offset += len(tokens)
        
        # 检查是否有下一页
        has_next = data.get('hasNext', False)
        
        # 停止条件：达到最大记录数或没有下一页
        if not has_next or len(all_tokens) >= max_records:
            break
    
    return all_tokens

def prepare_tokens_data(tokens):
    """处理代币数据并转换为DataFrame，保留所有原始字段"""
    if not tokens:
        return pd.DataFrame()
    
    # 创建一个空列表来存储处理后的数据
    processed_data = []
    
    for token in tokens:
        # 创建一个新字典来存储处理后的记录
        record = {}
        
        # 添加所有字段到记录中
        for key, value in token.items():
            # 处理特殊类型（如列表、字典）
            if isinstance(value, (list, dict)):
                record[key] = json.dumps(value)
            else:
                record[key] = value
        
        processed_data.append(record)
    
    return pd.DataFrame(processed_data)

def prepare_traders_data(traders):
    """处理交易者数据并转换为DataFrame，保留所有原始字段"""
    if not traders:
        return pd.DataFrame()
    
    # 创建一个空列表来存储处理后的数据
    processed_data = []
    
    for trader in traders:
        # 创建一个新字典来存储处理后的记录
        record = {}
        
        # 添加所有字段到记录中
        for key, value in trader.items():
            # 处理特殊类型（如列表、字典）
            if isinstance(value, (list, dict)):
                record[key] = json.dumps(value)
            else:
                record[key] = value
        
        processed_data.append(record)
    
    return pd.DataFrame(processed_data)