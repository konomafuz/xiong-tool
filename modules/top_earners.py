import time
import json
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils import fetch_data_robust

class TopEarnersTracker:
    def __init__(self):
        self.max_workers = 2  # Render 环境限制并发数
        self.request_delay = 0.8  # 增加请求间隔
        self.max_timeout = 20  # 单个请求最大超时
        
    def fetch_top_traders_optimized(self, token_address, chain_id="501", limit=50):
        """优化的获取TOP交易者方法，适配 Render 环境"""
        print(f"🔍 开始获取TOP交易者，代币: {token_address[:8]}..., 链: {chain_id}")
        
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
        
        params = {
            "chainId": chain_id,
            "tokenContractAddress": token_address,
            "t": int(time.time() * 1000)
        }
        
        try:
            # 使用较短超时和重试
            response = fetch_data_robust(
                url, params, 
                max_retries=2, 
                timeout=self.max_timeout,
                backoff_factor=0.5
            )
            
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
                return []
                
            print(f"✅ 获取到 {len(traders)} 个交易者")
            
            # 限制返回数量并手动清理内存
            result = traders[:limit]
            del traders, data, response
            gc.collect()
            
            return result
            
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            return []
    
    def fetch_address_token_list_optimized(self, wallet_address: str, chain_id=501, max_records=100):
        """优化的获取地址代币列表，分批获取并控制内存"""
        print(f"🔍 获取地址代币列表: {wallet_address[:8]}...")
        
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        
        all_tokens = []
        batch_size = 50  # 减小批次大小
        max_batches = (max_records + batch_size - 1) // batch_size
        max_batches = min(max_batches, 5)  # 最多5批，避免超时
        
        try:
            for batch in range(max_batches):
                offset = batch * batch_size
                current_limit = min(batch_size, max_records - len(all_tokens))
                
                if current_limit <= 0:
                    break
                
                params = {
                    "walletAddress": wallet_address,
                    "chainId": chain_id,
                    "isAsc": False,
                    "sortType": 1,
                    "offset": offset,
                    "limit": current_limit,
                    "t": int(time.time() * 1000)
                }
                
                print(f"📊 请求第 {batch + 1}/{max_batches} 批，偏移: {offset}, 限制: {current_limit}")
                
                # 添加延迟避免限流
                if batch > 0:
                    time.sleep(self.request_delay)
                
                response = fetch_data_robust(
                    url, params, 
                    max_retries=2, 
                    timeout=self.max_timeout,
                    backoff_factor=0.5
                )
                
                if not response or response.get('code') != 0:
                    print(f"❌ 第 {batch + 1} 批请求失败")
                    break
                
                data = response.get('data', {})
                tokens = data.get('list', [])
                
                if not tokens:
                    print(f"📝 第 {batch + 1} 批无数据，停止获取")
                    break
                
                print(f"✅ 第 {batch + 1} 批获取到 {len(tokens)} 个代币")
                all_tokens.extend(tokens)
                
                # 内存清理
                del tokens, data, response
                
                # 检查是否达到目标数量或API返回的总数
                total = data.get('total', 0) if 'data' in locals() else 0
                if len(all_tokens) >= max_records or len(all_tokens) >= total:
                    break
            
            print(f"🎯 最终获取到 {len(all_tokens)} 个代币")
            
            # 最终内存清理
            gc.collect()
            return all_tokens
            
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            return []

# 更新便捷函数，移除硬编码限制
def fetch_top_traders(token_address, chain_id="501", limit=50):
    """获取指定代币的顶级盈利交易者 - 移除硬编码限制"""
    tracker = TopEarnersTracker()
    # 移除 min(limit, 50) 限制，允许用户选择的数量
    return tracker.fetch_top_traders_optimized(token_address, chain_id, limit)

def fetch_address_token_list(wallet_address: str, chain_id=501, max_records=100):
    """获取地址的代币列表 - 移除硬编码限制"""
    tracker = TopEarnersTracker()
    # 移除 min(max_records, 100) 限制
    return tracker.fetch_address_token_list_optimized(wallet_address, chain_id, max_records)

def prepare_traders_data(traders):
    """处理交易者数据，转换为DataFrame格式 - 内存优化版本"""
    if not traders:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        def safe_extract_tags(tag_data):
            """安全提取标签 - 简化版本"""
            if not tag_data:
                return []
            
            tags = []
            try:
                if isinstance(tag_data, list):
                    for item in tag_data:
                        if isinstance(item, str) and item.strip():
                            tags.append(item.strip())
                        elif isinstance(item, dict) and 'k' in item:
                            tags.append(str(item['k']).strip())
                elif isinstance(tag_data, dict) and 'k' in tag_data:
                    tags.append(str(tag_data['k']).strip())
            except:
                pass
            
            return list(set([tag for tag in tags if tag]))
        
        def safe_get_float(data, key, default=0.0):
            try:
                value = data.get(key, default)
                return float(value) if value is not None else default
            except:
                return default
        
        def safe_get_int(data, key, default=0):
            try:
                value = data.get(key, default)
                return int(value) if value is not None else default
            except:
                return default
        
        # 只处理必要的字段，减少内存使用
        processed_data = []
        for i, trader in enumerate(traders):
            wallet_address = trader.get('holderWalletAddress', '')
            
            # 简化标签处理
            tag_list = trader.get('tagList', [])
            t_list = trader.get('t', [])
            all_tags = safe_extract_tags(tag_list) + safe_extract_tags(t_list)
            tags = list(set(all_tags))[:5]  # 最多保留5个标签
            
            # 核心数据
            total_profit = safe_get_float(trader, 'totalProfit')
            buy_count = safe_get_int(trader, 'buyCount')
            sell_count = safe_get_int(trader, 'sellCount')
            
            # 构建精简的数据结构
            processed_trader = {
                'walletAddress': wallet_address,
                'totalPnl': total_profit,
                'realizedProfit': safe_get_float(trader, 'realizedProfit'),
                'totalProfitPercentage': safe_get_float(trader, 'totalProfitPercentage'),
                'buyCount': buy_count,
                'sellCount': sell_count,
                'totalCount': buy_count + sell_count,
                'holdAmount': safe_get_float(trader, 'holdAmount'),
                'buyValue': safe_get_float(trader, 'buyValue'),
                'sellValue': safe_get_float(trader, 'sellValue'),
                'winRate': min(100, max(0, safe_get_float(trader, 'totalProfitPercentage'))),
                'roi': safe_get_float(trader, 'totalProfitPercentage'),
                'rank': i + 1,
                'tags': ', '.join(tags[:3]),  # 最多显示3个标签
                'chainId': trader.get('chainId', ''),
                'lastTradeTime': trader.get('lastTradeTime', '')
            }
            
            processed_data.append(processed_trader)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ 交易者数据转换完成，共 {len(df)} 条记录")
        
        # 清理内存
        del processed_data, traders
        gc.collect()
        
        return df
        
    except Exception as e:
        print(f"❌ 交易者数据处理失败: {e}")
        import pandas as pd
        return pd.DataFrame()

def prepare_tokens_data(tokens):
    """处理代币数据 - 内存优化版本"""
    if not tokens:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        # 只处理必要字段
        processed_data = []
        for token in tokens:
            processed_token = {
                'tokenAddress': token.get('tokenContractAddress', ''),
                'tokenSymbol': token.get('tokenSymbol', ''),
                'tokenName': token.get('tokenName', '')[:50],  # 限制长度
                'totalPnl': float(token.get('totalPnl', 0)),
                'totalPnlPercentage': float(token.get('totalPnlPercentage', 0)),
                'realizedPnl': float(token.get('realizedPnl', 0)),
                'winRate': float(token.get('winRate', 0)),
                'totalCount': int(token.get('totalCount', 0)),
                'buyValue': float(token.get('buyValue', 0)),
                'sellValue': float(token.get('sellValue', 0)),
                'holdValue': float(token.get('holdValue', 0)),
                'currentPrice': float(token.get('currentPrice', 0)),
                'roi': float(token.get('roi', 0))
            }
            processed_data.append(processed_token)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ 代币数据转换完成，共 {len(df)} 条记录")
        
        # 清理内存
        del processed_data, tokens
        gc.collect()
        
        return df
        
    except Exception as e:
        print(f"❌ 代币数据处理失败: {e}")
        import pandas as pd
        return pd.DataFrame()

# 简化的测试函数
def test_fetch_top_traders():
    """测试获取TOP交易者功能 - 简化版本"""
    print("🧪 测试获取TOP交易者...")
    
    test_token = "HtTYHz1Kf3rrQo6AqDLmss7gq5WrkWAaXn3tupUZbonk"
    test_chain = "501"
    
    traders = fetch_top_traders(test_token, test_chain, 5)  # 只测试5个
    
    if traders:
        print(f"✅ 测试成功！获取到 {len(traders)} 个交易者")
        df = prepare_traders_data(traders)
        print(f"📊 DataFrame形状: {df.shape}")
    else:
        print("❌ 测试失败！")

if __name__ == "__main__":
    test_fetch_top_traders()