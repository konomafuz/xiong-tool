# parse_transactions.py
import requests
import pandas as pd
import time
from datetime import datetime

def fetch_transactions_helius(address, helius_api_key, limit=500):
    """从Helius获取地址的交易历史"""
    if not helius_api_key:
        raise ValueError("需要Helius API密钥")
    
    url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
    params = {"api-key": helius_api_key, "limit": limit}
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"获取{address}交易失败: {e}")
        return []

def parse_enhanced_transactions(address, helius_api_key, limit=500):
    """获取并解析增强交易数据"""
    url = f"https://api.helius.xyz/v0/addresses/{address}/transactions"
    params = {
        "api-key": helius_api_key,
        "limit": limit,
        "type": "ENHANCED"
    }
    
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"获取{address}增强交易失败: {e}")
        return []

def parse_transactions(raw_txs, target_token=None):
    """解析交易数据，提取swap/transfer/add/remove等事件"""
    events = []
    
    for tx in raw_txs:
        try:
            timestamp = tx.get('timestamp', 0)
            tx_hash = tx.get('signature', '')
            
            # 解析类型
            tx_type = tx.get('type', 'UNKNOWN')
            
            # 如果有增强数据，优先使用
            if 'events' in tx:
                for event in tx['events']:
                    event_type = event.get('type', 'UNKNOWN')
                    
                    if event_type == 'SWAP':
                        swap_data = event.get('swap', {})
                        events.append({
                            'timestamp': timestamp,
                            'tx_hash': tx_hash,
                            'type': 'SWAP',
                            'token_in': swap_data.get('tokenIn', {}).get('mint', ''),
                            'token_out': swap_data.get('tokenOut', {}).get('mint', ''),
                            'amount_in': swap_data.get('tokenIn', {}).get('amount', 0),
                            'amount_out': swap_data.get('tokenOut', {}).get('amount', 0),
                            'program_id': event.get('source', '')
                        })
                    
                    elif event_type == 'TOKEN_TRANSFER':
                        transfer_data = event.get('tokenTransfer', {})
                        events.append({
                            'timestamp': timestamp,
                            'tx_hash': tx_hash,
                            'type': 'TRANSFER',
                            'token_mint': transfer_data.get('mint', ''),
                            'from_address': transfer_data.get('fromUserAccount', ''),
                            'to_address': transfer_data.get('toUserAccount', ''),
                            'amount': transfer_data.get('tokenAmount', 0),
                            'program_id': event.get('source', '')
                        })
            
            # 如果没有增强数据，使用基础解析
            else:
                if 'tokenTransfers' in tx:
                    for transfer in tx['tokenTransfers']:
                        events.append({
                            'timestamp': timestamp,
                            'tx_hash': tx_hash,
                            'type': 'TRANSFER',
                            'token_mint': transfer.get('mint', ''),
                            'from_address': transfer.get('fromUserAccount', ''),
                            'to_address': transfer.get('toUserAccount', ''),
                            'amount': transfer.get('tokenAmount', 0),
                            'program_id': ''
                        })
                        
        except Exception as e:
            print(f"解析交易失败: {e}")
            continue
    
    return pd.DataFrame(events)

def analyze_address_transactions(address, helius_api_key, target_token=None):
    """分析单个地址的交易"""
    print(f"正在分析地址: {address}")
    
    # 获取交易数据
    raw_txs = fetch_transactions_helius(address, helius_api_key, 500)
    if not raw_txs:
        return pd.DataFrame()
    
    # 解析交易
    events_df = parse_transactions(raw_txs, target_token)
    
    if not events_df.empty:
        # 转换时间戳
        events_df['datetime'] = pd.to_datetime(events_df['timestamp'], unit='s')
        events_df['address'] = address
    
    return events_df

def batch_analyze_holders(holders_df, helius_api_key, target_token=None):
    """批量分析top holders的交易"""
    all_events = []
    
    for idx, row in holders_df.iterrows():
        address = row['address']
        events_df = analyze_address_transactions(address, helius_api_key, target_token)
        
        if not events_df.empty:
            all_events.append(events_df)
        
        # 避免请求过快
        time.sleep(1)
        
        if idx % 10 == 0:
            print(f"已处理 {idx + 1}/{len(holders_df)} 个地址")
    
    if all_events:
        return pd.concat(all_events, ignore_index=True)
    else:
        return pd.DataFrame()