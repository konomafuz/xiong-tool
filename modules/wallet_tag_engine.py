import json
import time
from datetime import datetime, timedelta
from utils import fetch_data_robust

class WalletTagEngine:
    def __init__(self, config_path=None):
        """初始化标签引擎"""
        if config_path is None:
            config_path = "config/wallet_tags_config.json"
        
        # 更新的标签配置
        default_config = {
            "tags": {
                "单一币钱包": {"emoji": "", "short": "单币", "group": "none", "priority": 1},
                "高频交易者": {"emoji": "", "short": "高频", "group": "交易频率", "priority": 3},
                "低频交易者": {"emoji": "", "short": "低频", "group": "交易频率", "priority": 5},
                "休眠交易者": {"emoji": "", "short": "休眠", "group": "交易频率", "priority": 6},
                "新兴聪明交易者": {"emoji": "🧠", "short": "聪明", "group": "none", "priority": 7},
                "暴击小子": {"emoji": "💥", "short": "暴击", "group": "none", "priority": 8},
                "钓鱼钱包": {"emoji": "🎣", "short": "钓鱼", "group": "none", "priority": 10},
                "新钱包": {"emoji": "🆕", "short": "新", "group": "none", "priority": 2},
                "狙击钱包": {"emoji": "🎯", "short": "狙击", "group": "none", "priority": 9},
                "波段圣手": {"emoji": "📈", "short": "波段", "group": "none", "priority": 4},
                "做市商": {"emoji": "🔄", "short": "做市", "group": "none", "priority": 11}
            },
            "exclusive_groups": {
                "交易频率": ["高频交易者", "低频交易者", "休眠交易者"]
            }
        }
        
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
        except:
            print(f"⚠️ 配置文件 {config_path} 不存在，使用默认配置")
            self.config = default_config
        
        self.tags_config = self.config.get('tags', default_config['tags'])
        self.exclusive_groups = self.config.get('exclusive_groups', default_config['exclusive_groups'])
    
    def fetch_wallet_tokens(self, wallet_address, chain_id="501"):
        """获取钱包代币数据"""
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        
        params = {
            "walletAddress": wallet_address,
            "chainId": chain_id,
            "isAsc": False,
            "sortType": 1,
            "offset": 0,
            "limit": 100,
            "t": int(time.time() * 1000)
        }
        
        try:
            response = fetch_data_robust(url, params, max_retries=3, timeout=20)
            
            if response and response.get('code') == 0:
                tokens = response.get('data', {}).get('tokenList', [])
                print(f"✅ 获取到 {len(tokens)} 个代币")
                return tokens
            else:
                print(f"❌ 获取代币失败")
                return []
                
        except Exception as e:
            print(f"❌ 代币请求异常: {e}")
            return []
    
    def fetch_wallet_profile_multi_period(self, wallet_address, chain_id="501"):
        """获取多个时间窗口的钱包profile数据（只要7D和30D）"""
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/wallet-profile/summary"
        
        # periodType: 3=7D, 4=30D
        periods = {
            '7d': 3,
            '30d': 4
        }
        
        profile_data = {}
        
        for period_name, period_type in periods.items():
            params = {
                "periodType": period_type,
                "chainId": chain_id,
                "walletAddress": wallet_address,
                "t": int(time.time() * 1000)
            }
            
            try:
                response = fetch_data_robust(url, params, max_retries=3, timeout=20)
                
                if response and response.get('code') == 0:
                    data = response.get('data', {})
                    profile_data[period_name] = {
                        'win_rate': float(data.get('totalWinRate', 0)),
                        'total_pnl': float(data.get('totalPnl', 0)),
                        'total_roi': float(data.get('totalPnlRoi', 0)),
                        'total_tx_buy': int(data.get('totalTxsBuy', 0)),
                        'total_tx_sell': int(data.get('totalTxsSell', 0)),
                        'total_tx': int(data.get('totalTxsBuy', 0)) + int(data.get('totalTxsSell', 0))
                    }
                    print(f"✅ 获取{period_name}数据成功")
                else:
                    print(f"❌ 获取{period_name}数据失败")
                    profile_data[period_name] = self._get_empty_profile()
                
                # 避免API限制
                time.sleep(0.5)
                    
            except Exception as e:
                print(f"❌ {period_name}请求异常: {e}")
                profile_data[period_name] = self._get_empty_profile()
        
        return profile_data
    
    def _get_empty_profile(self):
        """返回空的profile数据"""
        return {
            'win_rate': 0,
            'total_pnl': 0,
            'total_roi': 0,
            'total_tx_buy': 0,
            'total_tx_sell': 0,
            'total_tx': 0
        }
    
    def calculate_wallet_stats(self, tokens_data, profile_data_multi):
        """计算钱包统计数据"""
        try:
            # 代币相关统计
            token_balances = []
            zero_buy_count = 0
            max_single_token_pnl = 0
            effective_token_count = 0  # 有效代币数量（>100刀）
            high_profit_tokens = 0  # 高收益代币数量（>10k刀）
            total_balance_usd = 0  # 总持仓价值
            
            for token in tokens_data:
                balance = float(token.get('balanceUsd', 0))
                
                # 计算有效持仓（>100刀）
                if balance > 100:
                    total_balance_usd += balance
                    effective_token_count += 1
                
                token_balances.append(balance)
                
                # 钓鱼检测：有余额但没有买入记录
                if balance > 0 and int(token.get('totalTxBuy', 0)) == 0:
                    zero_buy_count += 1
                
                # 单个代币最大收益和高收益代币统计
                token_pnl = float(token.get('totalPnl', 0))
                if token_pnl > max_single_token_pnl:
                    max_single_token_pnl = token_pnl
                
                if token_pnl > 10000:  # 收益超过1万刀
                    high_profit_tokens += 1
            
            positive_balances = [b for b in token_balances if b > 0]
            
            # 计算持仓时间
            holding_times = []
            for token in tokens_data:
                holding_time = int(token.get('holdingTime', 0))
                if holding_time > 0:
                    days_ago = (time.time() - holding_time) / (24 * 3600)
                    holding_times.append(days_ago)
            
            # 计算是否为大额持仓（>0.1M）
            balance_millions = total_balance_usd / 1000000
            has_large_holding = balance_millions >= 0.1
            
            # 确定星星等级
            star_level = ""
            if high_profit_tokens >= 10:
                star_level = "🌟"  # 金色星星
            elif high_profit_tokens >= 5:
                star_level = "⭐"  # 银色星星
            
            # 基础统计
            stats = {
                # 基础数据
                'total_balance_usd': total_balance_usd,
                'effective_token_count': effective_token_count,
                'token_count': len(positive_balances),  # 兼容性
                'max_token_ratio': max(positive_balances) / sum(positive_balances) if positive_balances else 0,
                
                # 持仓相关
                'balance_millions': balance_millions,
                'has_large_holding': has_large_holding,
                'high_profit_tokens': high_profit_tokens,
                'star_level': star_level,
                
                # 7D和30D数据
                'win_rates': {
                    '7d': profile_data_multi.get('7d', {}).get('win_rate', 0),
                    '30d': profile_data_multi.get('30d', {}).get('win_rate', 0)
                },
                'total_pnls': {
                    '7d': profile_data_multi.get('7d', {}).get('total_pnl', 0),
                    '30d': profile_data_multi.get('30d', {}).get('total_pnl', 0)
                },
                'total_rois': {
                    '7d': profile_data_multi.get('7d', {}).get('total_roi', 0),
                    '30d': profile_data_multi.get('30d', {}).get('total_roi', 0)
                },
                'total_transactions': {
                    '7d': profile_data_multi.get('7d', {}).get('total_tx', 0),
                    '30d': profile_data_multi.get('30d', {}).get('total_tx', 0)
                },
                
                # 特殊检测数据
                'zero_buy_token_count': zero_buy_count,
                'wallet_age_days': min(holding_times) if holding_times else 0,
                'max_single_token_pnl': max_single_token_pnl,
                
                # 兼容性数据（使用30d数据）
                'total_transactions_legacy': profile_data_multi.get('30d', {}).get('total_tx', 0),
                'win_rate_legacy': profile_data_multi.get('30d', {}).get('win_rate', 0),
                'total_pnl_legacy': profile_data_multi.get('30d', {}).get('total_pnl', 0),
                'total_roi_legacy': profile_data_multi.get('30d', {}).get('total_roi', 0),
                
                # 原始数据
                'tokens_count': len(tokens_data),
                'has_profile': len(profile_data_multi) > 0
            }
            
            return stats
            
        except Exception as e:
            print(f"❌ 计算统计数据失败: {e}")
            return self._get_empty_stats()
    
    def _get_empty_stats(self):
        """返回空的统计数据"""
        return {
            'total_balance_usd': 0,
            'effective_token_count': 0,
            'token_count': 0,
            'balance_millions': 0,
            'has_large_holding': False,
            'high_profit_tokens': 0,
            'star_level': "",
            'win_rates': {'7d': 0, '30d': 0},
            'total_pnls': {'7d': 0, '30d': 0},
            'total_rois': {'7d': 0, '30d': 0},
            'total_transactions': {'7d': 0, '30d': 0},
            'total_transactions_legacy': 0,
            'win_rate_legacy': 0,
            'total_pnl_legacy': 0,
            'total_roi_legacy': 0,
            'zero_buy_token_count': 0,
            'wallet_age_days': 0,
            'max_single_token_pnl': 0
        }
    
    def identify_tags_enhanced(self, stats):
        """增强的标签识别逻辑"""
        detected_tags = []
        
        try:
            # 1. 持仓特征（基于有效代币数量）
            if stats['effective_token_count'] <= 3:
                detected_tags.append("单一币钱包")
            
            # 2. 交易频率（基于7d数据，互斥）
            tx_7d = stats['total_transactions'].get('7d', 0)
            tx_30d = stats['total_transactions'].get('30d', 0)
            
            if tx_30d == 0:
                detected_tags.append("休眠交易者")
            elif tx_7d >= 200:
                detected_tags.append("高频交易者")
            else:
                detected_tags.append("低频交易者")
            
            # 3. 聪明交易者（基于7d胜率）
            win_rate_7d = stats['win_rates'].get('7d', 0)
            if win_rate_7d >= 50:
                detected_tags.append("新兴聪明交易者")
            
            # 4. 暴击小子（7d胜率<30%，但单个币大于1w刀收益）
            if win_rate_7d < 30 and stats['max_single_token_pnl'] > 10000:
                detected_tags.append("暴击小子")
            
            # 5. 钓鱼钱包
            if stats['zero_buy_token_count'] > 0:
                detected_tags.append("钓鱼钱包")
            
            # 6. 新钱包
            if stats['wallet_age_days'] <= 30 and stats['wallet_age_days'] > 0:
                detected_tags.append("新钱包")
            
            # 7. 波段圣手（多个时间窗口都盈利且胜率高）
            if (stats['total_pnls'].get('7d', 0) > 0 and 
                stats['total_pnls'].get('30d', 0) > 0 and
                win_rate_7d >= 60):
                detected_tags.append("波段圣手")
            
            return detected_tags
            
        except Exception as e:
            print(f"❌ 标签识别失败: {e}")
            return ["未知钱包"]
    
    def analyze_wallet(self, wallet_address, chain_id="501"):
        """分析单个钱包"""
        print(f"🔍 开始分析钱包: {wallet_address[:8]}...")
        
        try:
            # 获取数据
            tokens_data = self.fetch_wallet_tokens(wallet_address, chain_id)
            profile_data_multi = self.fetch_wallet_profile_multi_period(wallet_address, chain_id)
            
            # 计算统计数据
            stats = self.calculate_wallet_stats(tokens_data, profile_data_multi)
            
            # 识别标签
            raw_tags = self.identify_tags_enhanced(stats)
            detected_tags = self.filter_exclusive_tags(raw_tags)
            
            # 构建返回结果
            result = {
                'address': wallet_address,
                'chain_id': chain_id,
                'detected_tags': detected_tags,
                'stats': stats,
                'wallet_data': stats,  # 兼容性
                'tag_details': {tag: self.tags_config.get(tag, {"emoji": "🏷️", "short": tag}) for tag in detected_tags},
                'tokens_data': tokens_data[:10] if tokens_data else [],
                'profile_data_multi': profile_data_multi
            }
            
            print(f"✅ 分析完成: {len(detected_tags)} 个标签 {detected_tags}")
            print(f"💰 持仓: ${stats['total_balance_usd']:.0f} ({stats['effective_token_count']}币)")
            if stats['star_level']:
                print(f"⭐ 高收益代币: {stats['high_profit_tokens']}个 {stats['star_level']}")
            
            return result
            
        except Exception as e:
            print(f"❌ 钱包分析失败: {e}")
            import traceback
            print(f"📝 详细错误: {traceback.format_exc()}")
            
            # 返回错误结果
            return {
                'address': wallet_address,
                'chain_id': chain_id,
                'detected_tags': [],
                'stats': self._get_empty_stats(),
                'wallet_data': self._get_empty_stats(),
                'tag_details': {},
                'error': str(e)
            }
    
    def filter_exclusive_tags(self, tags):
        """处理互斥标签"""
        filtered_tags = []
        excluded_groups = set()
        
        # 按优先级排序
        sorted_tags = sorted(tags, key=lambda t: self.tags_config.get(t, {}).get('priority', 5))
        
        for tag in sorted_tags:
            tag_config = self.tags_config.get(tag, {})
            tag_group = tag_config.get('group', 'none')
            
            if tag_group != 'none' and tag_group in excluded_groups:
                continue  # 跳过同组的后续标签
            
            filtered_tags.append(tag)
            if tag_group != 'none':
                excluded_groups.add(tag_group)
        
        return filtered_tags
    
    def batch_analyze(self, addresses, chain_id="501"):
        """批量分析钱包"""
        results = []
        
        for i, address in enumerate(addresses):
            print(f"\n🔍 分析钱包 {i+1}/{len(addresses)}: {address[:8]}...")
            
            result = self.analyze_wallet(address.strip(), chain_id)
            results.append(result)
            
            # 避免API限制
            if i < len(addresses) - 1:  # 最后一个不需要等待
                time.sleep(2)  # 增加等待时间，因为要请求多个API
        
        print(f"\n✅ 批量分析完成！共处理 {len(results)} 个钱包")
        return results

# 测试函数
def test_tag_engine():
    """测试标签引擎"""
    engine = WalletTagEngine()
    
    test_addresses = [
        "38tAutsiZWaJ8MsMJVgKCx4U5LHwZM2g6StmQpKLRqz6",
        "3bKhBxxTuCWSiV1jmDUB9yxDWSej67yh1tS3Sk2j4rdQ"
    ]
    
    results = engine.batch_analyze(test_addresses, "501")
    
    for result in results:
        print(f"\n📊 钱包: {result['address'][:8]}...")
        print(f"🏷️ 标签: {result['detected_tags']}")
        
        # 显示关键数据
        data = result.get('wallet_data', {})
        print(f"💰 持仓价值: ${data.get('total_balance_usd', 0):.2f}")
        print(f"🔢 代币数量: {data.get('token_count', 0)}")
        print(f"📈 交易次数: {data.get('total_transactions', 0)}")
        print(f"🎯 30天胜率: {data.get('win_rate_30d', 0):.1f}%")

if __name__ == "__main__":
    test_tag_engine()