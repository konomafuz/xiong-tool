import time
import json
from utils import fetch_data_robust

def fetch_top_traders(token_address, chain_id="501", limit=100):
    """获取指定代币的顶级盈利交易者"""
    print(f"🔍 开始获取TOP交易者，代币: {token_address}, 链: {chain_id}")
    
    # 使用正确的API URL
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
    
    params = {
        "chainId": chain_id,
        "tokenContractAddress": token_address,
        "t": int(time.time() * 1000)
    }
    
    try:
        print(f"📊 请求URL: {url}")
        print(f"📊 请求参数: {params}")
        
        response = fetch_data_robust(url, params, max_retries=3, timeout=20)
        
        if not response:
            print(f"❌ 请求失败：无响应")
            return []
            
        if response.get('code') != 0:
            print(f"❌ 请求失败：{response}")
            return []
        
        data = response.get('data', {})
        traders = data.get('list', [])
        
        if not traders:
            print(f"📝 API返回空数据")
            print(f"📝 完整响应: {json.dumps(response, indent=2, ensure_ascii=False)}")
            return []
            
        print(f"✅ 获取到 {len(traders)} 个交易者")
        
        # 限制返回数量
        return traders[:limit]
        
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        import traceback
        print(f"📝 异常详情: {traceback.format_exc()}")
        return []

def fetch_address_token_list(wallet_address: str, chain_id=501, max_records=1000):
    """获取地址的代币列表"""
    print(f"🔍 获取地址代币列表: {wallet_address[:8]}...")
    
    # 使用正确的API URL
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
    
    params = {
        "walletAddress": wallet_address,
        "chainId": chain_id,
        "isAsc": False,  # 降序排列
        "sortType": 1,   # 按盈利排序
        "offset": 0,     # 偏移量
        "limit": min(max_records, 100),  # 限制每次请求数量
        "t": int(time.time() * 1000)
    }
    
    all_tokens = []
    
    try:
        # 分页获取所有代币
        while len(all_tokens) < max_records:
            current_limit = min(100, max_records - len(all_tokens))
            params['limit'] = current_limit
            params['offset'] = len(all_tokens)
            
            print(f"📊 请求第 {params['offset'] // 100 + 1} 页，偏移: {params['offset']}, 限制: {current_limit}")
            
            response = fetch_data_robust(url, params, max_retries=3, timeout=20)
            
            if not response:
                print(f"❌ 请求失败：无响应")
                break
                
            if response.get('code') != 0:
                print(f"❌ 请求失败：{response}")
                break
            
            data = response.get('data', {})
            tokens = data.get('list', [])
            
            if not tokens:
                print(f"📝 第 {params['offset'] // 100 + 1} 页无数据，停止获取")
                break
                
            print(f"✅ 第 {params['offset'] // 100 + 1} 页获取到 {len(tokens)} 个代币")
            all_tokens.extend(tokens)
            
            # 检查是否还有更多数据
            total = data.get('total', 0)
            if len(all_tokens) >= total or len(tokens) < current_limit:
                print(f"📋 已获取所有数据，总计: {len(all_tokens)}")
                break
                
            time.sleep(0.3)  # 避免API限制
            
        print(f"🎯 最终获取到 {len(all_tokens)} 个代币")
        return all_tokens
        
    except Exception as e:
        print(f"❌ 请求异常: {e}")
        import traceback
        print(f"📝 异常详情: {traceback.format_exc()}")
        return []

def prepare_tokens_data(tokens):
    """处理代币数据"""
    if not tokens:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        processed_data = []
        for token in tokens:
            processed_token = {
                'tokenAddress': token.get('tokenContractAddress', ''),
                'tokenSymbol': token.get('tokenSymbol', ''),
                'tokenName': token.get('tokenName', ''),
                'totalPnl': float(token.get('totalPnl', 0)),
                'totalPnlPercentage': float(token.get('totalPnlPercentage', 0)),
                'realizedPnl': float(token.get('realizedPnl', 0)),
                'unrealizedPnl': float(token.get('unrealizedPnl', 0)),
                'winRate': float(token.get('winRate', 0)),
                'winCount': int(token.get('winCount', 0)),
                'lossCount': int(token.get('lossCount', 0)),
                'totalCount': int(token.get('totalCount', 0)),
                'buyValue': float(token.get('buyValue', 0)),
                'sellValue': float(token.get('sellValue', 0)),
                'holdValue': float(token.get('holdValue', 0)),
                'holdAmount': float(token.get('holdAmount', 0)),
                'avgBuyPrice': float(token.get('avgBuyPrice', 0)),
                'avgSellPrice': float(token.get('avgSellPrice', 0)),
                'currentPrice': float(token.get('currentPrice', 0)),
                'chain': token.get('chainId', ''),
                'logoUrl': token.get('logoUrl', ''),
                'firstBuyTime': token.get('firstBuyTime', ''),
                'lastTradeTime': token.get('lastTradeTime', ''),
                'roi': float(token.get('roi', 0))
            }
            processed_data.append(processed_token)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ 代币数据转换完成，共 {len(df)} 条记录")
        return df
        
    except Exception as e:
        print(f"❌ 代币数据处理失败: {e}")
        import pandas as pd
        return pd.DataFrame()

def prepare_traders_data(traders):
    """处理交易者数据，转换为DataFrame格式"""
    if not traders:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        def safe_extract_tags(tag_data):
            """安全提取标签"""
            tags = []
            
            if not tag_data:
                return tags
            
            try:
                if isinstance(tag_data, list):
                    for item in tag_data:
                        if isinstance(item, str):
                            tags.append(item)
                        elif isinstance(item, list):
                            # 处理嵌套列表，如 [["suspectedPhishingWallet"]]
                            for nested_item in item:
                                if isinstance(nested_item, str):
                                    tags.append(nested_item)
                        elif isinstance(item, dict):
                            if 'k' in item:
                                tags.append(str(item['k']))
                            elif 'name' in item:
                                tags.append(str(item['name']))
                        else:
                            tags.append(str(item))
                elif isinstance(tag_data, dict):
                    if 'k' in tag_data:
                        tags.append(str(tag_data['k']))
                    elif 'name' in tag_data:
                        tags.append(str(tag_data['name']))
                else:
                    tags.append(str(tag_data))
            except Exception as e:
                print(f"⚠️ 标签提取异常: {e}")
                tags = []
            
            return list(set([tag for tag in tags if tag and tag.strip()]))
        
        def safe_get_float(data, key, default=0.0):
            """安全获取float值"""
            try:
                value = data.get(key, default)
                return float(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        def safe_get_int(data, key, default=0):
            """安全获取int值"""
            try:
                value = data.get(key, default)
                return int(value) if value is not None else default
            except (ValueError, TypeError):
                return default
        
        # 展开数据结构
        processed_data = []
        for i, trader in enumerate(traders):
            # 打印调试信息
            if i == 0:
                print(f"🔍 第一个交易者的所有字段: {list(trader.keys())}")
            
            # 提取钱包地址
            wallet_address = trader.get('holderWalletAddress', '')
            
            # 处理标签 - 同时处理 tagList 和 t 字段
            tag_list = trader.get('tagList', [])
            t_list = trader.get('t', [])
            
            all_tags = []
            all_tags.extend(safe_extract_tags(tag_list))
            
            # 处理 t 字段中的标签
            if t_list:
                for t_item in t_list:
                    if isinstance(t_item, dict) and 'k' in t_item:
                        all_tags.append(t_item['k'])
            
            # 去重
            tags = list(set(all_tags))
            
            # 计算一些衍生字段
            buy_count = safe_get_int(trader, 'buyCount')
            sell_count = safe_get_int(trader, 'sellCount')
            total_count = buy_count + sell_count
            
            # 计算胜率 (这里简化为有盈利即为胜)
            total_profit = safe_get_float(trader, 'totalProfit')
            realized_profit = safe_get_float(trader, 'realizedProfit')
            win_rate = safe_get_float(trader, 'totalProfitPercentage')  # 使用总盈利百分比作为胜率指标
            
            # 构建处理后的数据 - 根据实际API字段映射
            processed_trader = {
                'walletAddress': wallet_address,
                'holderWalletAddress': wallet_address,
                
                # 核心财务数据
                'totalPnl': total_profit,  # 使用 totalProfit
                'realizedProfit': realized_profit,
                'unrealizedProfit': safe_get_float(trader, 'unrealizedProfit'),
                'totalProfitPercentage': safe_get_float(trader, 'totalProfitPercentage'),
                'realizedProfitPercentage': safe_get_float(trader, 'realizedProfitPercentage'),
                'unrealizedProfitPercentage': safe_get_float(trader, 'unrealizedProfitPercentage'),
                
                # 交易统计
                'buyCount': buy_count,
                'sellCount': sell_count,
                'totalCount': total_count,
                'winCount': max(1, buy_count) if total_profit > 0 else 0,  # 简化计算
                'lossCount': max(1, sell_count) if total_profit <= 0 else 0,  # 简化计算
                
                # 价格和数量
                'buyValue': safe_get_float(trader, 'buyValue'),
                'sellValue': safe_get_float(trader, 'sellValue'),
                'holdAmount': safe_get_float(trader, 'holdAmount'),
                'holdVolume': safe_get_float(trader, 'holdVolume'),
                'holdAmountPercentage': safe_get_float(trader, 'holdAmountPercentage'),
                
                # 平均价格
                'boughtAvgPrice': safe_get_float(trader, 'boughtAvgPrice'),
                'soldAvgPrice': safe_get_float(trader, 'soldAvgPrice'),
                'holdAvgPrice': safe_get_float(trader, 'holdAvgPrice'),
                
                # 计算字段
                'winRate': min(100, max(0, win_rate)),  # 限制在0-100之间
                'avgProfit': realized_profit / max(1, buy_count) if buy_count > 0 else 0,
                'avgLoss': 0,  # API中没有提供亏损数据
                'maxProfit': realized_profit,  # 简化为已实现盈利
                'maxLoss': 0,  # API中没有提供
                'roi': safe_get_float(trader, 'totalProfitPercentage'),
                'profitFactor': 0,  # API中没有提供
                'sharpeRatio': 0,  # API中没有提供
                
                # 时间信息
                'lastTradeTime': trader.get('lastTradeTime', ''),
                'holdCreateTime': trader.get('holdCreateTime', ''),
                'holdingTime': safe_get_int(trader, 'holdingTime'),
                
                # 其他信息
                'chainId': trader.get('chainId', ''),
                'tokenContractAddress': trader.get('tokenContractAddress', ''),
                'explorerUrl': trader.get('explorerUrl', ''),
                'nativeTokenBalance': safe_get_float(trader, 'nativeTokenBalance'),
                
                # 排名和标签
                'rank': i + 1,  # 按顺序排名
                'tags': ', '.join(tags),
                'tagList': tags
            }
            
            processed_data.append(processed_trader)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ 交易者数据转换完成，共 {len(df)} 条记录")
        
        # 打印转换后的数据预览
        if not df.empty:
            print(f"📊 转换后的数据预览:")
            print(f"  总盈利范围: {df['totalPnl'].min():.2f} 至 {df['totalPnl'].max():.2f}")
            print(f"  平均盈利: {df['totalPnl'].mean():.2f}")
            print(f"  盈利地址数: {(df['totalPnl'] > 0).sum()}")
            
            # 显示前几条记录的关键字段
            print(f"📋 前3条记录预览:")
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                print(f"  #{i+1}: {row['walletAddress'][:8]}... - 盈利: ${row['totalPnl']:.2f} ({row['totalProfitPercentage']:.1f}%)")
        
        return df
        
    except Exception as e:
        print(f"❌ 交易者数据处理失败: {e}")
        import traceback
        print(f"📝 异常详情: {traceback.format_exc()}")
        import pandas as pd
        return pd.DataFrame()

# 测试函数
def test_fetch_top_traders():
    """测试获取TOP交易者功能"""
    print("🧪 测试获取TOP交易者...")
    
    # 使用提供的代币地址进行测试
    test_token = "HtTYHz1Kf3rrQo6AqDLmss7gq5WrkWAaXn3tupUZbonk"
    test_chain = "501"
    
    traders = fetch_top_traders(test_token, test_chain, 10)
    
    if traders:
        print(f"✅ 测试成功！获取到 {len(traders)} 个交易者")
        print(f"📋 第一个交易者信息:")
        first_trader = traders[0]
        for key, value in first_trader.items():
            print(f"  {key}: {value}")
        
        # 测试数据处理
        df = prepare_traders_data(traders)
        print(f"📊 DataFrame形状: {df.shape}")
        print(f"📊 DataFrame列: {list(df.columns)}")
        
    else:
        print("❌ 测试失败！未获取到交易者数据")

def test_fetch_address_tokens():
    """测试获取地址代币列表功能"""
    print("\n🧪 测试获取地址代币列表...")
    
    # 使用示例地址进行测试
    test_address = "38tAutsiZWaJ8MsMJVgKCx4U5LHwZM2g6StmQpKLRqz6"
    test_chain = "501"
    
    tokens = fetch_address_token_list(test_address, test_chain, 50)
    
    if tokens:
        print(f"✅ 测试成功！获取到 {len(tokens)} 个代币")
        print(f"📋 第一个代币信息:")
        first_token = tokens[0]
        for key, value in first_token.items():
            print(f"  {key}: {value}")
        
        # 测试数据处理
        df = prepare_tokens_data(tokens)
        print(f"📊 DataFrame形状: {df.shape}")
        print(f"📊 DataFrame列: {list(df.columns)}")
        
    else:
        print("❌ 测试失败！未获取到代币数据")

if __name__ == "__main__":
    test_fetch_top_traders()
    test_fetch_address_tokens()