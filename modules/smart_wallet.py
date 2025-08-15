import requests
import time
import json
from datetime import datetime, timedelta
from utils import fetch_data_robust

def parse_address_input(address_text):
    """解析地址输入，支持带标记的地址
    
    Args:
        address_text: 地址文本，可能包含标记
    
    Returns:
        tuple: (地址, 原始标记或None)
    """
    address_text = address_text.strip()
    
    if ':' in address_text:
        # 格式: 地址:标记
        parts = address_text.split(':', 1)
        address = parts[0].strip()
        existing_label = parts[1].strip()
        return address, existing_label
    else:
        # 纯地址
        return address_text, None

def fetch_wallet_profile(chain_id, wallet_address, period_type=5):
    """获取钱包profile信息
    
    Args:
        chain_id: 链ID (501=Solana, 1=Ethereum)
        wallet_address: 钱包地址
        period_type: 时间周期 (1=1日, 2=3日, 3=7日, 4=1月, 5=3月)
    """
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

def generate_smart_wallet_remark(wallet_address, wallet_data, existing_label=None):
    """生成聪明钱包标记
    
    Args:
        wallet_address: 钱包地址
        wallet_data: 钱包数据 (来自wallet_profile API)
        existing_label: 已有标记 (可选)
    
    Returns:
        str: 生成的标记
    """
    try:
        # 获取1月和3月的胜率
        win_rate_1m = "0"
        win_rate_3m = "0"
        
        # 从winRateList中获取胜率信息
        win_rate_list = wallet_data.get('winRateList', [])
        if len(win_rate_list) >= 4:  # 确保有足够的数据
            win_rate_1m = win_rate_list[3]  # 1月胜率 (索引3)
        if len(win_rate_list) >= 5:
            win_rate_3m = win_rate_list[4]  # 3月胜率 (索引4)
        
        # 获取总盈利
        total_pnl = wallet_data.get('totalPnl', 0)
        
        # 生成基础标记：钱包前4位-月胜率-季胜率-盈利
        wallet_prefix = wallet_address[:4]
        
        # 格式化盈利数值
        if total_pnl >= 1000000:
            pnl_str = f"{total_pnl/1000000:.1f}M"
        elif total_pnl >= 1000:
            pnl_str = f"{total_pnl/1000:.1f}k"
        else:
            pnl_str = f"{total_pnl:.0f}"
        
        base_remark = f"{wallet_prefix}-月{win_rate_1m}-季{win_rate_3m}-{pnl_str}"
        # 如果有已存在的标记，添加到后面
        if existing_label:
            final_remark = f"{base_remark}-{existing_label}"
        else:
            final_remark = base_remark
        
        return final_remark
        
    except Exception as e:
        print(f"❌ 生成标记失败: {e}")
        # 备用标记
        base_backup = f"{wallet_address[:4]}-智能"
        if existing_label:
            return f"{base_backup}-{existing_label}"
        else:
            return base_backup

def is_smart_wallet(wallet_data, criteria):
    """判断是否为聪明钱包
    
    Args:
        wallet_data: 钱包数据
        criteria: 筛选条件字典
    
    Returns:
        tuple: (是否为聪明钱包, 判断原因, emoji)
    """
    if not wallet_data:
        return False, "无法获取钱包数据", "❓"
    
    try:
        # 获取关键指标
        total_win_rate = float(wallet_data.get('totalWinRate', 0))
        win_rate_list = wallet_data.get('winRateList', [])
        
        # 获取1月和3月胜率
        win_rate_1m = 0
        win_rate_3m = 0
        
        if len(win_rate_list) >= 4:
            win_rate_1m = float(win_rate_list[3])
        if len(win_rate_list) >= 5:
            win_rate_3m = float(win_rate_list[4])
        
        # 获取收益分布
        new_win_rate_distribution = wallet_data.get('newWinRateDistribution', [0, 0, 0, 0])
        
        # 高收益项目数量 (>500% 和 100-500%)
        high_return_count = new_win_rate_distribution[3] if len(new_win_rate_distribution) > 3 else 0
        medium_return_count = new_win_rate_distribution[2] if len(new_win_rate_distribution) > 2 else 0
        
        # 总PnL
        total_pnl = float(wallet_data.get('totalPnl', 0))
        
        # 应用筛选条件
        reasons = []
        
        # 胜率检查
        if win_rate_1m >= criteria['win_rate_1m']:
            reasons.append(f"1月胜率{win_rate_1m}%")
        elif win_rate_3m >= criteria['win_rate_3m']:
            reasons.append(f"3月胜率{win_rate_3m}%")
        else:
            return False, f"胜率不达标(1月:{win_rate_1m}%, 3月:{win_rate_3m}%)", "📉"
        
        # 盈利检查
        if total_pnl < criteria['min_profit']:
            return False, f"总盈利不达标({total_pnl:.0f} USD)", "💸"
        
        reasons.append(f"总盈利{total_pnl:.0f}USD")
        
        # 高收益检查
        has_high_return = (
            high_return_count >= criteria['high_return_min'] or 
            medium_return_count >= criteria['medium_return_min']
        )
        
        if has_high_return:
            if high_return_count >= criteria['high_return_min']:
                reasons.append(f"{high_return_count}个>500%项目")
                emoji = "🚀"
            else:
                reasons.append(f"{medium_return_count}个高收益项目")
                emoji = "💎"
        else:
            return False, f"高收益项目不足(>500%:{high_return_count}, 100-500%:{medium_return_count})", "🔍"
        
        return True, " | ".join(reasons), emoji
        
    except Exception as e:
        print(f"❌ 判断聪明钱包异常: {e}")
        return False, f"数据异常: {str(e)}", "❓"

def analyze_address_list(address_list_text, chain_id="501"):
    """分析地址列表，生成智能标记
    
    Args:
        address_list_text: 地址列表文本（每行一个地址，可能包含标记）
        chain_id: 链ID
    
    Returns:
        list: 分析结果列表
    """
    results = []
    
    # 解析地址列表
    addresses_with_labels = []
    for line in address_list_text.split('\n'):
        line = line.strip()
        if line:
            address, existing_label = parse_address_input(line)
            if address and len(address) > 20:  # 简单验证地址长度
                addresses_with_labels.append((address, existing_label))
    
    print(f"🔍 解析到 {len(addresses_with_labels)} 个有效地址")
    
    for i, (address, existing_label) in enumerate(addresses_with_labels):
        print(f"\n🔍 分析地址 {i+1}/{len(addresses_with_labels)}: {address[:8]}...")
        if existing_label:
            print(f"📋 检测到已有标记: {existing_label}")
        
        try:
            # 获取钱包数据
            wallet_data = fetch_wallet_profile(chain_id, address, period_type=5)  # 3月数据
            
            if not wallet_data:
                remark = f"{address[:4]}-无数据"
                if existing_label:
                    remark += f"-{existing_label}"
                
                results.append({
                    "address": address,
                    "remark": remark,
                    "emoji": "❓",
                    "reason": "无法获取钱包数据",
                    "twitter_info": None,
                    "existing_label": existing_label
                })
                continue
            
            # 生成标记（包含已有标记）
            remark = generate_smart_wallet_remark(address, wallet_data, existing_label)
            
            # 简单的智能判断（基于胜率）
            total_win_rate = float(wallet_data.get('totalWinRate', 0))
            total_pnl = float(wallet_data.get('totalPnl', 0))
            
            if total_win_rate >= 30 and total_pnl >= 10000:
                emoji = "🚀"
                reason = f"胜率{total_win_rate}% | 盈利{total_pnl:.0f}USD"
            elif total_win_rate >= 20 and total_pnl >= 5000:
                emoji = "💎"
                reason = f"胜率{total_win_rate}% | 盈利{total_pnl:.0f}USD"
            elif total_win_rate >= 10:
                emoji = "🔍"
                reason = f"胜率{total_win_rate}% | 盈利{total_pnl:.0f}USD"
            else:
                emoji = "⚪"
                reason = f"胜率{total_win_rate}% | 盈利{total_pnl:.0f}USD"
            
            results.append({
                "address": address,
                "remark": remark,
                "emoji": emoji,
                "reason": reason,
                "twitter_info": None,  # 已删除Twitter功能
                "existing_label": existing_label
            })
            
            print(f"✅ 生成标记: {remark}")
            
            # 延迟避免频率限制
            time.sleep(0.5)
            
        except Exception as e:
            print(f"❌ 分析地址失败: {e}")
            remark = f"{address[:4]}-错误"
            if existing_label:
                remark += f"-{existing_label}"
            
            results.append({
                "address": address,
                "remark": remark,
                "emoji": "❌",
                "reason": f"分析失败: {str(e)}",
                "twitter_info": None,
                "existing_label": existing_label
            })
    
    return results

def discover_smart_wallets(token_address, criteria, chain_id="501", limit=300):
    """发现聪明钱包（原有的发现功能）
    
    Args:
        token_address: 代币地址
        criteria: 筛选条件
        chain_id: 链ID
        limit: 分析数量限制
    
    Returns:
        dict: 分析结果
    """
    # 这里应该调用原有的获取持仓者和交易者的逻辑
    # 暂时返回空结果，需要根据实际情况实现
    print(f"🔍 开始发现模式分析，代币: {token_address}, 限制: {limit}")
    
    # TODO: 实现获取Top Holders和Top Traders的逻辑
    # TODO: 实现聪明钱包筛选逻辑
    
    return {
        "smart_wallets": [],
        "stats": {
            "total_analyzed": 0,
            "smart_count": 0,
            "failed_count": 0
        }
    }

# 测试函数
def test_smart_wallet_analysis():
    """测试聪明钱包分析功能"""
    print("🧪 测试聪明钱包分析功能...")
    
    # 测试地址列表（包含带标记的地址）
    test_address_text = """E2rdM9Esp6YWygHzgYz1UKzMTkZonVU4LYK8fQYnYo9h
suqh5sHtr8HyJ7q8scBimULPkPpA557prMG47xCHQfK
CyaE1VxvBrahnPWkqm5VsdCvyS2QmNht2UFrKJHga54o:NYAN-盈利46.04k
52kPsnhTpjHZucGUQy4wSk8E3EWYc8wtF9V94RUQGkemWtt3i8dUtsA4P:NYAN-盈利18.62k"""
    
    # 分析地址列表
    results = analyze_address_list(test_address_text, "501")
    
    print(f"\n📊 分析完成，共处理 {len(results)} 个地址")
    for result in results:
        print(f"地址: {result['address'][:8]}...")
        print(f"标记: {result['remark']}")
        print(f"原因: {result['reason']}")
        print(f"已有标记: {result['existing_label'] or '无'}")
        print("-" * 50)

if __name__ == "__main__":
    test_smart_wallet_analysis()