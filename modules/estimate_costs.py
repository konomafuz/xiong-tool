# estimate_costs.py
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

def get_price_at_timestamp(token_id, timestamp):
    """获取指定时间点的价格"""
    try:
        # 如果是datetime对象，转换为字符串
        if isinstance(timestamp, (int, float)):
            date_obj = datetime.fromtimestamp(timestamp)
        else:
            date_obj = timestamp
            
        date_str = date_obj.strftime("%d-%m-%Y")
        
        url = f"https://api.coingecko.com/api/v3/coins/{token_id}/history"
        params = {"date": date_str}
        
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        return data['market_data']['current_price']['usd']
    except Exception as e:
        print(f"获取价格失败 {token_id} @ {timestamp}: {e}")
        return None

def get_current_price(token_id):
    """获取当前价格"""
    try:
        url = f"https://api.coingecko.com/api/v3/simple/price"
        params = {"ids": token_id, "vs_currencies": "usd"}
        
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        
        return data[token_id]['usd']
    except Exception as e:
        print(f"获取当前价格失败 {token_id}: {e}")
        return None

def estimate_transaction_costs(events_df, token_id):
    """估算交易成本"""
    if events_df.empty:
        return pd.DataFrame()
    
    cost_estimates = []
    
    # 按地址分组
    for address, group in events_df.groupby('address'):
        buy_events = []
        sell_events = []
        
        for _, event in group.iterrows():
            if event['type'] == 'SWAP':
                # 判断是买入还是卖出
                if event.get('token_out') == token_id or event.get('token_mint') == token_id:
                    buy_events.append(event)
                elif event.get('token_in') == token_id:
                    sell_events.append(event)
            
            elif event['type'] == 'TRANSFER':
                # 转入视为买入，转出视为卖出
                if event['to_address'] == address:
                    buy_events.append(event)
                elif event['from_address'] == address:
                    sell_events.append(event)
        
        # 计算平均成本
        total_cost = 0
        total_amount = 0
        
        for buy_event in buy_events:
            timestamp = buy_event['timestamp']
            amount = buy_event.get('amount', 0) or buy_event.get('amount_out', 0)
            
            if amount > 0:
                price = get_price_at_timestamp(token_id, timestamp)
                if price:
                    cost = amount * price
                    total_cost += cost
                    total_amount += amount
                
                time.sleep(0.1)  # 避免请求过快
        
        avg_cost = total_cost / total_amount if total_amount > 0 else 0
        
        cost_estimates.append({
            'address': address,
            'total_bought': total_amount,
            'total_cost_usd': total_cost,
            'avg_cost_usd': avg_cost,
            'buy_count': len(buy_events),
            'sell_count': len(sell_events)
        })
    
    return pd.DataFrame(cost_estimates)

def calculate_unrealized_pnl(holders_df, cost_estimates_df, current_price):
    """计算未实现盈亏"""
    if holders_df.empty or cost_estimates_df.empty:
        return pd.DataFrame()
    
    # 合并持仓和成本数据
    merged_df = holders_df.merge(cost_estimates_df, on='address', how='left')
    
    # 计算未实现盈亏
    merged_df['current_value_usd'] = merged_df['balance'] * current_price
    merged_df['unrealized_pnl_usd'] = merged_df['current_value_usd'] - merged_df['total_cost_usd']
    merged_df['unrealized_pnl_pct'] = (merged_df['unrealized_pnl_usd'] / merged_df['total_cost_usd'] * 100).fillna(0)
    merged_df['multiplier'] = (merged_df['current_value_usd'] / merged_df['total_cost_usd']).fillna(0)
    
    return merged_df

def analyze_holder_profitability(holders_df, events_df, token_id):
    """分析持有者盈利能力"""
    print("正在估算成本...")
    cost_estimates = estimate_transaction_costs(events_df, token_id)
    
    print("正在获取当前价格...")
    current_price = get_current_price(token_id)
    
    if current_price is None:
        print("无法获取当前价格，跳过盈亏计算")
        return cost_estimates
    
    print("正在计算盈亏...")
    pnl_df = calculate_unrealized_pnl(holders_df, cost_estimates, current_price)
    
    return pnl_df