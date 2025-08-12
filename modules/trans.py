import requests
import time
import hmac
import base64
import hashlib
import json
import pandas as pd
from datetime import datetime, timezone


# 你的 API 信息
API_KEY = 'a0b223a0-bfb7-43c5-9320-7579253aaa68'
API_SECRET = 'DE312D8D08408AF175CCE041ED075610'
API_PASSPHRASE ='kdLS.6.V8gwZu!j'
PROJECT_ID = 'xap_cb'

# ========== 工具函数 ==========
def get_iso_timestamp():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def generate_signature(timestamp, method, request_path, body=''):
    message = f"{timestamp}{method.upper()}{request_path}{body}"
    mac = hmac.new(bytes(API_SECRET, encoding='utf8'),
                   bytes(message, encoding='utf8'),
                   digestmod=hashlib.sha256)
    return base64.b64encode(mac.digest()).decode()

def flatten_transaction(tx):
    # 处理可能为空的值
    from_info = tx.get("from", [{}])
    to_info = tx.get("to", [{}])
    
    from_dict = from_info[0] if from_info else {}
    to_dict = to_info[0] if to_info else {}
    
    # 处理时间
    ts_ms = int(tx.get("txTime", "0")) if tx.get("txTime") else 0
    time_str = ""
    if ts_ms > 0:
        try:
            time_str = datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except:
            pass
    
    # 处理数量 - 确保转换为浮点数
    amount_str = tx.get("amount", "0")
    try:
        amount = float(amount_str) if amount_str else 0.0
    except (ValueError, TypeError):
        amount = 0.0
    
    # 构建结果字典，使用中文列名
    result = {
        "time": time_str,
        "txTime": ts_ms,
        "chainIndex": tx.get("chainIndex", ""),
        "txHash": tx.get("txHash", ""),
        "iType": tx.get("iType", ""),
        "methodId": tx.get("methodId", ""),
        "Nonce": tx.get("nonce", ""),
        "sender_address": from_dict.get("address", ""),
        "sender_amount": from_dict.get("amount", "0"),
        "receiver_address": to_dict.get("address", ""),
        "receiver_amount": to_dict.get("amount", "0"),
        "tokenAddress": tx.get("tokenAddress", ""),
        "coin_amount": amount,  # 已经转换为浮点数
        "symbol": tx.get("symbol", ""),
        "txFee": tx.get("txFee", ""),
        "txStatus": tx.get("txStatus", ""),
        "hitBlacklist": tx.get("hitBlacklist", False),
    }
    
    return result

# ========== 主函数：获取地址交易记录 ==========
def get_okx_transaction_df(address, chain="501", begin=None, end=None, limit=100):
    """
    查询 OKX 上某地址在某链的交易历史，并返回 pandas.DataFrame
    :param address: 钱包地址
    :param chain: 链 ID（501=Solana, 1=ETH, ...）
    :param begin: 开始时间（datetime 类型或 None）
    :param end: 结束时间（datetime 类型或 None）
    :param limit: 每页查询条数（默认100，最大100）
    :return: pd.DataFrame
    """
    base_url = "https://web3.okx.com"
    path = "/api/v5/wallet/post-transaction/transactions-by-address"
    url = base_url + path

    # 时间戳转毫秒
    begin_ms = int(begin.timestamp() * 1000) if begin else None
    end_ms = int(end.timestamp() * 1000) if end else None

    all_txs = []
    cursor = None

    while True:
        params = {
            "address": address,
            "chains": chain,
            "limit": str(limit),
        }
        if begin_ms:
            params["begin"] = str(begin_ms)
        if end_ms:
            params["end"] = str(end_ms)
        if cursor:
            params["cursor"] = cursor

        query_string = '?' + '&'.join(f"{k}={v}" for k, v in params.items())
        timestamp = get_iso_timestamp()
        sign = generate_signature(timestamp, 'GET', path + query_string)

        headers = {
            'Content-Type': 'application/json',
            'OK-ACCESS-KEY': API_KEY,
            'OK-ACCESS-SIGN': sign,
            'OK-ACCESS-TIMESTAMP': timestamp,
            'OK-ACCESS-PASSPHRASE': API_PASSPHRASE,
            'OK-ACCESS-PROJECT': PROJECT_ID
        }

        response = requests.get(url, headers=headers, params=params, timeout=15)
        response.raise_for_status()  # 检查HTTP错误
        if response.status_code != 200:
            print("请求失败:", response.status_code, response.text)
            break

        data_list = response.json().get("data", [])
        if not data_list:
            break

        txs = data_list[0].get("transactionList", [])
        all_txs.extend(txs)
        cursor = data_list[0].get("cursor")
        if not cursor or not txs:
            break

        time.sleep(1)

    if not all_txs:
        return pd.DataFrame()
    
    records = []
    for tx in all_txs:
        try:
            records.append(flatten_transaction(tx))
        except Exception as e:
            print(f"交易处理失败: {tx.get('txHash', '未知交易')} - {str(e)}")
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    
    
    # 对数量列进行预处理
    if 'coin_amount' in df.columns:
        df['coin_amount'] = pd.to_numeric(df['coin_amount'], errors='coerce').fillna(0.0)
    
    # 按时间排序（降序）
    if 'time' in df.columns:
        df.sort_values('time', ascending=False, inplace=True)
    
    return df

