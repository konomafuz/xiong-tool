import time
import json
from collections import defaultdict
from utils import fetch_data


def get_token_list(address, chain_id):
    """获取目标地址交易过的代币列表（分页获取所有记录）"""
    all_tokens = []
    page_size = 100
    offset = 0
    
    while True:
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        params = {
            "walletAddress": address,
            "chainId": chain_id,
            "isAsc": "false",     # 字符串
            "sortType": "1",      # 字符串
            "offset": offset,
            "limit": page_size,
            "t": int(time.time() * 1000)
        }
        data = fetch_data(url, params)
        if not data:
            break
        
        tokens = data.get('tokenList', [])
        if not tokens:
            break
        
        all_tokens.extend(tokens)
        offset += len(tokens)
        
        # 判断是否还有下一页：如果返回条数 < 请求条数，则说明到末尾
        if len(tokens) < page_size:
            break
    
    return all_tokens

def get_first_buy(address, chain_id, token_address):
    """获取目标地址在特定代币上的最早买入交易ID"""
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/trading-history"
    params = {
        "chainId": chain_id,
        "tokenContractAddress": token_address,
        "type": "0",  # 买入交易
        "userAddressList": address,
        "limit": 1,
        "desc": "false",  # 按时间正序，获取最早的一条
        "t": int(time.time() * 1000)
    }
    
    data = fetch_data(url, params)
    if data and data.get('list'):
        return data['list'][0]['id']  # 返回第一条交易的ID
    
    return None

def get_early_traders(chain_id, token_address, first_tx_id, page_limit=5):
    """获取在目标地址之前交易该代币的所有地址"""
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/trading-history"
    early_traders = set()
    next_id = first_tx_id
    count = 0
    
    while count < page_limit:  # 限制翻页次数
        count += 1
        params = {
            "chainId": chain_id,
            "tokenContractAddress": token_address,
            "type": "0",  # 买入交易
            "desc": "true",  # 时间倒序（从最新到最早）
            "dataId": next_id,
            "limit": 100,
            "t": int(time.time() * 1000)
        }
        
        data = fetch_data(url, params)
        if not data or not data.get('list'):
            break
        
        trades = data['list']
        # 收集交易地址（排除目标地址自身）
        for trade in trades:
            if trade['userAddress'].lower() != address.lower():
                early_traders.add(trade['userAddress'])
        
        # 更新下一页起始ID（使用最后一笔交易的ID）
        if trades:
            next_id = trades[-1]['id']
    
    return early_traders

def find_smart_accounts(target_address, chain_id, max_tokens=50, page_limit=5):
    """识别潜在小号地址"""
    # 1. 获取目标地址交易过的代币列表
    tokens = get_token_list(target_address, chain_id)
    if not tokens:
        return []
    
    # 限制代币数量，避免请求过多
    tokens = tokens[:max_tokens]
    
    # 2. 统计每个地址的"提前买入"次数
    address_counter = defaultdict(int)
    
    for token in tokens:
        token_address = token.get('tokenContractAddress')
        if not token_address:
            continue
            
        # 2.1 获取目标地址在该代币的最早交易ID
        first_tx_id = get_first_buy(target_address, chain_id, token_address)
        if not first_tx_id:
            continue
        
        # 2.2 获取所有更早的交易地址
        early_traders = get_early_traders(chain_id, token_address, first_tx_id, page_limit)
        
        # 2.3 更新地址计数器
        for addr in early_traders:
            address_counter[addr] += 1
        
        time.sleep(0.5)  # API限速缓冲
    
    # 3. 按出现频率排序
    suspicious_accounts = sorted(address_counter.items(), key=lambda x: x[1], reverse=True)
    return suspicious_accounts