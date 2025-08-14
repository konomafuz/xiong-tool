import requests
import time
import json
from datetime import datetime, timedelta
from utils import fetch_data_robust


def fetch_wallet_profile(chain_id, wallet_address, period_type=5):
    """获取钱包profile信息 - 使用更健壮的请求方法"""
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/wallet-profile/summary"
    
    params = {
        "periodType": period_type,
        "chainId": chain_id,
        "walletAddress": wallet_address,
        "t": int(time.time() * 1000)
    }
    
    try:
        response = fetch_data_robust(url, params, max_retries=3, timeout=20)
        
        if response and response.get('code') == 0:
            print(f"✅ 钱包 {wallet_address[:8]}... profile获取成功")
            return response.get('data', {})
        else:
            print(f"❌ 钱包 {wallet_address[:8]}... profile获取失败: {response}")
            return None
            
    except Exception as e:
        print(f"❌ 钱包 {wallet_address[:8]}... 请求异常: {e}")
        return None


def fetch_gmgn_wallet_stat(wallet_address, period="30d"):
    """获取GMGN钱包统计信息 - 使用更健壮的请求方法"""
    url = f"https://gmgn.ai/api/v1/wallet_stat/sol/{wallet_address}/{period}"
    
    params = {
        "device_id": "7204d77c-bdbe-44d1-b086-fbd07d171727",
        "client_id": "gmgn_web_20250808-2102-c0815f7",
        "from_app": "gmgn",
        "app_ver": "20250808-2102-c0815f7",
        "tz_name": "Asia/Shanghai",
        "tz_offset": "28800",
        "app_lang": "zh-CN",
        "fp_did": "c941f0f7b449fb59fb32d2bf260f16a2",
        "os": "web",
        "period": period
    }
    
    try:
        # 对GMGN使用标准requests，因为它通常更稳定
        response = requests.get(url, params=params, timeout=15)
        data = response.json()
        
        if data.get('code') == 0:
            print(f"✅ GMGN {wallet_address[:8]}... 获取成功")
            return data.get('data', {})
        else:
            print(f"❌ GMGN {wallet_address[:8]}... 获取失败: {data}")
            return None
            
    except Exception as e:
        print(f"❌ GMGN {wallet_address[:8]}... 请求失败: {e}")
        return None


def is_smart_wallet(wallet_data_1m, wallet_data_3m, smart_criteria):
    """判断是否为聪明钱包"""
    if not wallet_data_1m or not wallet_data_3m:
        return False, "数据获取失败"
    
    # 1. 检查胜率
    win_rate_1m = float(wallet_data_1m.get("totalWinRate", 0))
    win_rate_3m = float(wallet_data_3m.get("totalWinRate", 0))
    
    if win_rate_1m < smart_criteria["win_rate_1m"]:
        return False, f"1月胜率{win_rate_1m:.0f}%不足{smart_criteria['win_rate_1m']}%"
    
    if win_rate_3m < smart_criteria["win_rate_3m"]: 
        return False, f"3月胜率{win_rate_3m:.0f}%不足{smart_criteria['win_rate_3m']}%"
    
    # 2. 检查顶级代币盈利
    top_tokens = wallet_data_3m.get("topTokens", [])
    max_profit = 0
    if top_tokens:
        profits = [float(token.get("pnl", 0)) for token in top_tokens]
        max_profit = max(profits) if profits else 0
    
    if max_profit < smart_criteria["min_profit"]:
        return False, f"最大单币盈利{max_profit:.0f}不足{smart_criteria['min_profit']}"
    
    # 3. 检查收益率分布
    distribution = wallet_data_3m.get("newWinRateDistribution", [0, 0, 0, 0])
    if len(distribution) >= 2:
        high_return = distribution[0]  # >500%
        medium_return = distribution[1]  # 0-500%
        
        condition1 = high_return > smart_criteria["high_return_min"]
        condition2 = medium_return > smart_criteria["medium_return_min"]
        
        if not (condition1 or condition2):
            return False, f"收益分布不符合: >500%({high_return})≤{smart_criteria['high_return_min']} 且 0-500%({medium_return})≤{smart_criteria['medium_return_min']}"
    
    return True, f"✅聪明钱包: 月{win_rate_1m:.0f}%-季{win_rate_3m:.0f}%-最大盈利{max_profit:.0f}"


def is_conspiracy_wallet(wallet_data_3m, conspiracy_criteria, token_name):
    """判断是否为阴谋钱包"""
    if not wallet_data_3m:
        return False, "数据获取失败"
    
    # 检查历史PnL数据
    pnl_list = wallet_data_3m.get("datePnlList", [])
    
    if not pnl_list:
        return False, "无PnL历史数据"
    
    # 计算N天前的时间戳
    days_ago = conspiracy_criteria["empty_days"]
    cutoff_timestamp = int((datetime.now() - timedelta(days=days_ago)).timestamp() * 1000)
    
    # 检查N天前是否都为空（profit为0）
    old_profits = []
    for item in pnl_list:
        timestamp = item.get("timestamp", 0)
        profit = float(item.get("profit", 0))
        
        if timestamp < cutoff_timestamp:
            old_profits.append(profit)
    
    # 如果N天前的记录都是0或接近0，认为是新钱包
    if old_profits and all(abs(profit) < 1 for profit in old_profits):
        return True, f"新-{token_name}-top?"
    
    return False, f"{days_ago}天前有交易记录"


def verify_with_gmgn(wallet_address, smart_criteria):
    """使用GMGN验证聪明钱包"""
    # 获取30天和all数据
    data_30d = fetch_gmgn_wallet_stat(wallet_address, "30d")
    data_all = fetch_gmgn_wallet_stat(wallet_address, "all")
    
    if not data_30d or not data_all:
        return False, "GMGN数据获取失败", None
    
    # 检查胜率
    win_rate_30d = data_30d.get("winrate", 0) * 100  # 转换为百分比
    win_rate_all = data_all.get("winrate", 0) * 100
    
    if win_rate_30d < smart_criteria["gmgn_win_rate_1m"]:
        return False, f"GMGN月胜率{win_rate_30d:.0f}%不足{smart_criteria['gmgn_win_rate_1m']}%", None
    
    if win_rate_all < smart_criteria["gmgn_win_rate_all"]:
        return False, f"GMGN总胜率{win_rate_all:.0f}%不足{smart_criteria['gmgn_win_rate_all']}%", None
    
    # 检查Twitter信息
    twitter_name = data_all.get("twitter_name", "")
    follow_count = data_all.get("follow_count", 0)
    
    twitter_info = {
        "has_twitter": bool(twitter_name),
        "twitter_name": twitter_name,
        "follow_count": follow_count,
        "is_influencer": bool(twitter_name) and follow_count > smart_criteria["min_followers"]
    }
    
    return True, f"GMGN验证通过: 月{win_rate_30d:.0f}%-总{win_rate_all:.0f}%", twitter_info


def generate_wallet_remark(wallet_address, wallet_type, okx_data_1m, okx_data_3m, token_name, twitter_info=None):
    """生成钱包备注"""
    wallet_prefix = wallet_address[:4]
    
    if wallet_type == "conspiracy":
        return f"新-{token_name}-top?"
    
    elif wallet_type == "smart":
        win_rate_1m = int(float(okx_data_1m.get("totalWinRate", 0)))
        win_rate_3m = int(float(okx_data_3m.get("totalWinRate", 0)))
        
        # 基础格式
        base_remark = f"{wallet_prefix}-月{win_rate_1m}%-季{win_rate_3m}%"
        
        # 如果有Twitter且粉丝足够多
        if twitter_info and twitter_info["is_influencer"]:
            twitter_name = twitter_info["twitter_name"]
            follow_count = twitter_info["follow_count"]
            return f"{twitter_name}-月{win_rate_1m}%-季{win_rate_3m}%-{follow_count}关注"
        
        # GMGN验证过的加※标记
        elif twitter_info is not None:  # 说明经过了GMGN验证
            return f"※{base_remark}"
        
        return base_remark
    
    return f"{wallet_prefix}-未知"


def analyze_wallets(chain_id, token_address, token_name, limit=300, 
                   smart_criteria=None, conspiracy_criteria=None):
    """分析钱包类型 - 使用更健壮的网络请求"""
    # 默认筛选条件
    if smart_criteria is None:
        smart_criteria = {
            "win_rate_1m": 35,           # 1月胜率阈值
            "win_rate_3m": 30,           # 3月胜率阈值
            "min_profit": 10000,         # 最小单币盈利(刀)
            "high_return_min": 1,        # >500%收益数量
            "medium_return_min": 2,      # 0-500%收益数量
            "gmgn_win_rate_1m": 35,      # GMGN月胜率阈值
            "gmgn_win_rate_all": 30,     # GMGN总胜率阈值
            "min_followers": 100         # 最小关注数
        }
    
    if conspiracy_criteria is None:
        conspiracy_criteria = {
            "empty_days": 10             # 空白天数
        }
    
    print(f"🎯 开始分析钱包...")
    print(f"📊 分析数量: {limit}")
    print(f"💎 聪明钱包条件: 月胜率>{smart_criteria['win_rate_1m']}%, 季胜率>{smart_criteria['win_rate_3m']}%")
    print(f"🐟 阴谋钱包条件: {conspiracy_criteria['empty_days']}天前无交易")
    
    # 1. 获取Top Holders
    from modules.gmgn import fetch_top_holders
    holders = fetch_top_holders(chain_id, token_address, limit=limit)
    
    if not holders:
        raise Exception("获取Holders失败")
    
    print(f"✅ 获取到 {len(holders)} 个Holders")
    
    results = {
        "smart_wallets": [],
        "conspiracy_wallets": [],
        "failed_wallets": [],
        "stats": {
            "total_analyzed": 0,
            "smart_count": 0,
            "conspiracy_count": 0,
            "failed_count": 0
        }
    }
    
    # 2. 逐个分析钱包
    for i, holder in enumerate(holders):
        wallet_address = holder.get("address")
        if not wallet_address:
            continue
            
        print(f"\n📋 分析钱包 {i+1}/{len(holders)}: {wallet_address[:8]}...")
        results["stats"]["total_analyzed"] += 1
        
        try:
            # 获取OKX数据 - 增加延迟避免频率限制
            wallet_data_1m = fetch_wallet_profile(chain_id, wallet_address, period_type=4)  # 1个月
            time.sleep(1)  # 增加延迟
            
            wallet_data_3m = fetch_wallet_profile(chain_id, wallet_address, period_type=5)  # 3个月
            time.sleep(1)  # 增加延迟
            
            # 判断阴谋钱包
            is_conspiracy, conspiracy_reason = is_conspiracy_wallet(
                wallet_data_3m, conspiracy_criteria, token_name
            )
            
            if is_conspiracy:
                remark = generate_wallet_remark(
                    wallet_address, "conspiracy", wallet_data_1m, wallet_data_3m, token_name
                )
                
                results["conspiracy_wallets"].append({
                    "address": wallet_address,
                    "remark": remark,
                    "reason": conspiracy_reason,
                    "emoji": holder.get("emoji", ""),
                    "type": "conspiracy"
                })
                results["stats"]["conspiracy_count"] += 1
                print(f"🐟 阴谋钱包: {remark}")
                continue
            
            # 判断聪明钱包
            is_smart, smart_reason = is_smart_wallet(
                wallet_data_1m, wallet_data_3m, smart_criteria
            )
            
            if is_smart:
                print(f"💎 初步判定为聪明钱包: {smart_reason}")
                
                # GMGN二次验证
                gmgn_verified, gmgn_reason, twitter_info = verify_with_gmgn(
                    wallet_address, smart_criteria
                )
                
                if gmgn_verified:
                    remark = generate_wallet_remark(
                        wallet_address, "smart", wallet_data_1m, wallet_data_3m, 
                        token_name, twitter_info
                    )
                    
                    results["smart_wallets"].append({
                        "address": wallet_address,
                        "remark": remark,
                        "reason": f"{smart_reason} + {gmgn_reason}",
                        "emoji": holder.get("emoji", ""),
                        "type": "smart",
                        "twitter_info": twitter_info,
                        "okx_1m": wallet_data_1m,
                        "okx_3m": wallet_data_3m
                    })
                    results["stats"]["smart_count"] += 1
                    print(f"💎 聪明钱包确认: {remark}")
                else:
                    print(f"❌ GMGN验证失败: {gmgn_reason}")
                    results["failed_wallets"].append({
                        "address": wallet_address,
                        "reason": f"GMGN验证失败: {gmgn_reason}",
                        "type": "smart_failed"
                    })
                    results["stats"]["failed_count"] += 1
            else:
                print(f"⚪ 普通钱包: {smart_reason}")
                results["failed_wallets"].append({
                    "address": wallet_address,
                    "reason": smart_reason,
                    "type": "normal"
                })
                results["stats"]["failed_count"] += 1
                
        except Exception as e:
            print(f"❌ 分析失败: {e}")
            results["failed_wallets"].append({
                "address": wallet_address,
                "reason": f"分析异常: {str(e)}",
                "type": "error"
            })
            results["stats"]["failed_count"] += 1
    
    # 3. 输出统计结果
    stats = results["stats"]
    print(f"\n🎉 分析完成!")
    print(f"📊 总计分析: {stats['total_analyzed']} 个钱包")
    print(f"💎 聪明钱包: {stats['smart_count']} 个")
    print(f"🐟 阴谋钱包: {stats['conspiracy_count']} 个") 
    print(f"⚪ 其他钱包: {stats['failed_count']} 个")
    
    return results


# 测试函数
def test_wallet_analysis():
    """测试钱包分析功能"""
    print("🧪 测试钱包分析功能...")
    
    # 测试单个钱包
    test_address = "3bKhBxxTuCWSiV1jmDUB9yxDWSej67yh1tS3Sk2j4rdQ"
    
    print(f"\n1. 测试OKX API:")
    data_1m = fetch_wallet_profile("501", test_address, period_type=4)
    data_3m = fetch_wallet_profile("501", test_address, period_type=5)
    
    if data_1m:
        print(f"✅ 1月数据获取成功")
        print(f"  胜率: {data_1m.get('totalWinRate')}%")
    
    if data_3m:
        print(f"✅ 3月数据获取成功") 
        print(f"  胜率: {data_3m.get('totalWinRate')}%")
        print(f"  顶级代币数: {len(data_3m.get('topTokens', []))}")
    
    print(f"\n2. 测试GMGN API:")
    gmgn_30d = fetch_gmgn_wallet_stat(test_address, "30d")
    gmgn_all = fetch_gmgn_wallet_stat(test_address, "all")
    
    if gmgn_30d:
        print(f"✅ GMGN 30天数据获取成功")
        print(f"  胜率: {gmgn_30d.get('winrate', 0) * 100:.1f}%")
    
    if gmgn_all:
        print(f"✅ GMGN 全部数据获取成功")
        print(f"  胜率: {gmgn_all.get('winrate', 0) * 100:.1f}%")
        print(f"  Twitter: {gmgn_all.get('twitter_name', 'N/A')}")


if __name__ == "__main__":
    test_wallet_analysis()