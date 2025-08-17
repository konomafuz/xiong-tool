import time
import json
import requests
import pandas as pd
from utils import fetch_data_robust

class TopEarnersTracker:
    def __init__(self):
        self.max_workers = 2  # Render 环境限制并发数
        self.request_delay = 1.0  # 稍微增加请求间隔
        self.max_timeout = 25  # 增加单个请求超时
        
        # 支持的链配置
        self.supported_chains = {
            "1": "以太坊 (Ethereum)",
            "501": "Solana",
            "56": "BNB Chain",
            "137": "Polygon"
        }
        
    def fetch_top_traders_optimized(self, token_address, chain_id="501", limit=50):
        """简化的获取TOP交易者方法"""
        print(f"🔍 获取TOP交易者: {token_address[:8]}... 链:{chain_id} 数量:{limit}")
        
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
        params = {
            "chainId": str(chain_id),
            "tokenContractAddress": token_address,
            "t": int(time.time() * 1000)
        }
        
        try:
            response = fetch_data_robust(url, params, max_retries=2, timeout=self.max_timeout)
            
            if not response or response.get('code') != 0:
                print(f"❌ 请求失败: {response}")
                return []
            
            traders = response.get('data', {}).get('list', [])[:limit]
            print(f"✅ 获取到 {len(traders)} 个交易者")
            return traders
            
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            return []
    
    def fetch_address_token_list_optimized(self, wallet_address: str, chain_id=501, max_records=100):
        """优化的获取地址代币列表"""
        chain_info = self.supported_chains.get(str(chain_id), {"name": "Unknown", "symbol": "?"})
        print(f"🔍 获取地址代币列表")
        print(f"   地址: {wallet_address[:8]}...")
        print(f"   链: {chain_info['name']} (ID: {chain_id})")
        print(f"   目标数量: {max_records}")
        
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        
        # 减少批次数量，每次请求更多数据
        batch_size = min(200, max_records)
        max_batches = max(1, (max_records + batch_size - 1) // batch_size)
        max_batches = min(max_batches, 3)
        
        all_tokens = []
        
        try:
            for batch in range(max_batches):
                offset = batch * batch_size
                remaining = max_records - len(all_tokens)
                current_limit = min(batch_size, remaining)
                
                if current_limit <= 0:
                    break
                
                params = {
                    "walletAddress": wallet_address,
                    "chainId": str(chain_id),
                    "isAsc": False,
                    "sortType": 1,
                    "offset": offset,
                    "limit": current_limit,
                    "t": int(time.time() * 1000)
                }
                
                print(f"📊 第 {batch + 1}/{max_batches} 次请求，偏移: {offset}, 数量: {current_limit}")
                
                # 只在多次请求时添加延迟
                if batch > 0:
                    time.sleep(self.request_delay)
                
                # 调整超时
                timeout = 30 if str(chain_id) == "1" else self.max_timeout
                
                response = fetch_data_robust(
                    url, params, 
                    max_retries=2,
                    timeout=timeout
                )
                
                if not response or response.get('code') != 0:
                    print(f"❌ 第 {batch + 1} 次请求失败")
                    if response:
                        print(f"🔍 错误响应: {response}")
                    break
                
                data = response.get('data', {})
                tokens = data.get('tokenList', [])
                
                if not tokens:
                    print(f"📝 第 {batch + 1} 次请求无数据，停止获取")
                    break
                
                print(f"✅ 第 {batch + 1} 次请求获取到 {len(tokens)} 个代币")
                all_tokens.extend(tokens)
                
                # 保存当前批次的 tokens 数量，用于后续判断
                current_tokens_count = len(tokens)
                
                # 内存清理
                del tokens, data, response
                
                # 检查是否达到目标数量
                if len(all_tokens) >= max_records:
                    print(f"🎯 已达到目标数量 {max_records}，停止请求")
                    break
                
                # 如果这次返回的数据少于请求的，说明已经没有更多数据了
                if current_tokens_count < current_limit:
                    print(f"📝 返回数据量({current_tokens_count}) < 请求量({current_limit})，无更多数据")
                    break
            
            # 确保不超过目标数量
            result = all_tokens[:max_records]
            print(f"🎯 最终获取到 {len(result)} 个代币（共 {max_batches} 次请求）")
            
            # 最终内存清理
            gc.collect()
            return result
            
        except Exception as e:
            print(f"❌ 请求异常: {e}")
            import traceback
            print(f"🔍 详细错误: {traceback.format_exc()}")
            return []

# 更新便捷函数
def fetch_top_traders(token_address, chain_id="501", limit=50):
    """获取指定代币的顶级盈利交易者 - 支持分页"""
    tracker = TopEarnersTracker()
    return tracker.fetch_top_traders_optimized(token_address, str(chain_id), limit)

def fetch_address_token_list(wallet_address: str, chain_id=501, max_records=100):
    """获取地址的代币列表"""
    tracker = TopEarnersTracker()
    return tracker.fetch_address_token_list_optimized(wallet_address, str(chain_id), max_records)

def prepare_traders_data(traders, chain_id="501"):
    """处理交易者数据，转换为DataFrame格式 - 修复字段映射"""
    if not traders:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        # 链信息
        chain_names = {
            "1": "ETH", "56": "BSC", "137": "MATIC", "501": "SOL",
            "42161": "ARB", "10": "OP", "8453": "BASE", "43114": "AVAX"
        }
        chain_name = chain_names.get(str(chain_id), f"Chain{chain_id}")
        
        def safe_extract_tags(tag_data):
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
                        elif isinstance(item, list):
                            # 处理嵌套列表 [["whales"]]
                            for subitem in item:
                                if isinstance(subitem, str):
                                    tags.append(subitem.strip())
                elif isinstance(tag_data, dict) and 'k' in tag_data:
                    tags.append(str(tag_data['k']).strip())
            except:
                pass
            
            return list(set([tag for tag in tags if tag]))
        
        def safe_get_float(data, key, default=0.0):
            try:
                value = data.get(key, default)
                if isinstance(value, str):
                    return float(value) if value else default
                return float(value) if value is not None else default
            except:
                return default
        
        def safe_get_int(data, key, default=0):
            try:
                value = data.get(key, default)
                if isinstance(value, str):
                    return int(float(value)) if value else default
                return int(value) if value is not None else default
            except:
                return default
        
        processed_data = []
        for i, trader in enumerate(traders):
            wallet_address = trader.get('holderWalletAddress', '')
            
            # 修复标签处理 - 处理嵌套结构
            tag_list = trader.get('tagList', [])
            t_list = trader.get('t', [])
            all_tags = safe_extract_tags(tag_list) + safe_extract_tags(t_list)
            tags = list(set(all_tags))[:3]
            
            # 修复核心数据字段映射
            total_profit = safe_get_float(trader, 'totalProfit')
            buy_count = safe_get_int(trader, 'buyCount')
            sell_count = safe_get_int(trader, 'sellCount')
            
            processed_trader = {
                'walletAddress': wallet_address,
                'totalPnl': total_profit,
                'realizedProfit': safe_get_float(trader, 'realizedProfit'),
                'unrealizedProfit': safe_get_float(trader, 'unrealizedProfit'),
                'totalProfitPercentage': safe_get_float(trader, 'totalProfitPercentage'),
                'realizedProfitPercentage': safe_get_float(trader, 'realizedProfitPercentage'),
                'unrealizedProfitPercentage': safe_get_float(trader, 'unrealizedProfitPercentage'),
                'buyCount': buy_count,
                'sellCount': sell_count,
                'totalCount': buy_count + sell_count,
                'holdAmount': safe_get_float(trader, 'holdAmount'),
                'holdAmountPercentage': safe_get_float(trader, 'holdAmountPercentage'),
                'buyValue': safe_get_float(trader, 'buyValue'),
                'sellValue': safe_get_float(trader, 'sellValue'),
                'boughtAvgPrice': safe_get_float(trader, 'boughtAvgPrice'),
                'soldAvgPrice': safe_get_float(trader, 'soldAvgPrice'),
                'holdAvgPrice': safe_get_float(trader, 'holdAvgPrice'),
                'winRate': min(100, max(0, safe_get_float(trader, 'totalProfitPercentage'))),
                'roi': safe_get_float(trader, 'totalProfitPercentage'),
                'rank': i + 1,
                'tags': ', '.join(tags),
                'chainId': str(chain_id),
                'chainName': chain_name,
                'lastTradeTime': trader.get('lastTradeTime', ''),
                'holdingTime': trader.get('holdingTime', ''),
                'nativeTokenBalance': safe_get_float(trader, 'nativeTokenBalance'),
                'explorerUrl': trader.get('explorerUrl', '')
            }
            
            processed_data.append(processed_trader)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ {chain_name} 链交易者数据转换完成，共 {len(df)} 条记录")
        
        # 调试：打印前几行数据
        if not df.empty:
            print(f"🔍 前3名交易者:")
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                print(f"   {i+1}. {row['walletAddress'][:10]}... - 总利润: ${row['totalPnl']:,.2f} - ROI: {row['roi']:.2f}%")
        
        # 清理内存
        del processed_data, traders
        gc.collect()
        
        return df
        
    except Exception as e:
        print(f"❌ 交易者数据处理失败: {e}")
        import traceback
        print(f"🔍 详细错误: {traceback.format_exc()}")
        import pandas as pd
        return pd.DataFrame()

def prepare_tokens_data(tokens, chain_id="501"):
    """处理代币数据，转换为DataFrame格式 - 支持多链"""
    if not tokens:
        import pandas as pd
        return pd.DataFrame()
    
    try:
        import pandas as pd
        
        # 链信息
        chain_names = {
            "1": "ETH", "56": "BSC", "137": "MATIC", "501": "SOL",
            "42161": "ARB", "10": "OP", "8453": "BASE", "43114": "AVAX"
        }
        chain_name = chain_names.get(str(chain_id), f"Chain{chain_id}")
        
        # 批量处理，减少循环开销
        processed_data = []
        for token in tokens:
            processed_token = {
                'tokenAddress': token.get('tokenContractAddress', ''),
                'tokenSymbol': token.get('tokenSymbol', ''),
                'tokenName': token.get('tokenName', '')[:30],  # 限制长度
                'totalPnl': float(token.get('totalPnl', 0)),
                'totalPnlPercentage': float(token.get('totalPnlPercentage', 0)),
                'realizedPnl': float(token.get('realizedPnl', 0)),
                'winRate': float(token.get('winRate', 0)),
                'totalCount': int(token.get('totalCount', 0)),
                'buyValue': float(token.get('buyValue', 0)),
                'sellValue': float(token.get('sellValue', 0)),
                'holdValue': float(token.get('holdValue', 0)),
                'currentPrice': float(token.get('currentPrice', 0)),
                'roi': float(token.get('roi', 0)),
                'chainId': str(chain_id),
                'chainName': chain_name
            }
            processed_data.append(processed_token)
        
        df = pd.DataFrame(processed_data)
        print(f"✅ {chain_name} 链代币数据转换完成，共 {len(df)} 条记录")
        
        # 清理内存
        del processed_data, tokens
        gc.collect()
        
        return df
        
    except Exception as e:
        print(f"❌ 代币数据处理失败: {e}")
        import pandas as pd
        return pd.DataFrame()

# 测试函数
def test_fetch_top_traders():
    """测试获取TOP交易者功能 - 调试版本"""
    print("🧪 测试获取TOP交易者（调试模式）...")
    
    # 使用你提供的具体参数
    test_token = "0xdd3b11ef34cd511a2da159034a05fcb94d806686"
    test_chain = "1"
    
    print(f"\n🔍 ETH链测试")
    print(f"代币地址: {test_token}")
    print(f"链ID: {test_chain}")
    
    # 测试获取50个交易者
    traders = fetch_top_traders(test_token, test_chain, 50)
    
    if traders:
        print(f"✅ 测试成功！获取到 {len(traders)} 个交易者")
        # 打印第一个交易者的信息验证数据
        if len(traders) > 0:
            first_trader = traders[0]
            print(f"🔍 第一个交易者:")
            print(f"   地址: {first_trader.get('holderWalletAddress', 'N/A')[:10]}...")
            print(f"   总利润: ${float(first_trader.get('totalProfit', 0)):,.2f}")
            print(f"   ROI: {float(first_trader.get('totalProfitPercentage', 0)):.2f}%")
            
        # 测试数据处理
        df = prepare_traders_data(traders, test_chain)
        if not df.empty:
            print(f"📊 DataFrame处理成功，形状: {df.shape}")
            print(f"📈 前3名地址:")
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                print(f"   {i+1}. {row['walletAddress'][:10]}... - ROI: {row['roi']:.2f}%")
    else:
        print("❌ 测试失败！")

if __name__ == "__main__":
    test_fetch_top_traders()