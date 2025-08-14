import requests
import time
from datetime import datetime, timedelta
from utils import fetch_data_robust

def fetch_top_holders(chain_id, token_address, limit=100):
    """获取Top Holders"""
    url = f"https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
    params = {
        "chainId": chain_id,
        "tokenAddress": token_address,
        "t": int(time.time() * 1000),
        "limit": min(limit, 100),  # API单页限制100
        "offset": 0
    }
    
    all_holders = []
    
    try:
        while len(all_holders) < limit:
            print(f"🔍 获取 Holders 第 {params['offset']//params['limit'] + 1} 页...")
            
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            
            if not data or "data" not in data or "holderRankingList" not in data["data"]:
                print(f"❌ Holders API 响应异常: {data}")
                break
                
            holder_list = data["data"]["holderRankingList"]
            if not holder_list:
                print("✅ Holders 数据获取完毕")
                break
            
            all_holders.extend(holder_list)
            
            # 如果已够数量或本页不足，停止
            if len(all_holders) >= limit or len(holder_list) < params['limit']:
                break
                
            # 下一页
            params['offset'] += params['limit']
            time.sleep(0.5)
        
        # 处理数据
        holders = []
        for item in all_holders[:limit]:  # 限制数量
            # 获取标签和emoji
            tags = item.get("tagList", [])
            tags_flat = []
            if tags:
                for tag_group in tags:
                    if isinstance(tag_group, list):
                        tags_flat.extend(tag_group)
                    else:
                        tags_flat.append(tag_group)
            
            emoji = ""
            if "suspectedPhishingWallet" in tags_flat and "diamondHands" not in tags_flat:
                emoji = "🐟"
            elif "diamondHands" in tags_flat and "suspectedPhishingWallet" not in tags_flat:
                emoji = "💎"
            elif "suspectedPhishingWallet" in tags_flat and "diamondHands" in tags_flat:
                emoji = "🐠"
            
            holders.append({
                "address": item.get("holderWalletAddress"),
                "holdAmountPercentage": item.get("holdAmountPercentage", "0"),
                "emoji": emoji,
                "holdAmount": item.get("holdAmount", "0")
            })
        
        print(f"✅ 成功获取 {len(holders)} 个 Holders")
        return holders
        
    except Exception as e:
        print(f"❌ 获取Holders失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def fetch_top_traders(chain_id, token_address, limit=100):
    """获取Top Traders"""
    url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
    params = {
        "chainId": chain_id,
        "tokenContractAddress": token_address,
        "t": int(time.time() * 1000),
        "limit": min(limit, 100),
        "offset": 0
    }
    
    all_traders = []
    
    try:
        while len(all_traders) < limit:
            print(f"🔍 获取 Traders 第 {params['offset']//params['limit'] + 1} 页...")
            
            resp = requests.get(url, params=params, timeout=10)
            data = resp.json()
            
            if not data or data.get('code') != 0:
                print(f"❌ Traders API 响应异常: {data}")
                break
                
            trader_list = data.get('data', {}).get('list', [])
            if not trader_list:
                print("✅ Traders 数据获取完毕")
                break
            
            all_traders.extend(trader_list)
            
            if len(all_traders) >= limit or len(trader_list) < params['limit']:
                break
                
            params['offset'] += params['limit']
            time.sleep(0.5)
        
        # 处理数据
        traders = []
        for item in all_traders[:limit]:
            # 获取标签和emoji
            tags = item.get("tagList", [])
            tags_flat = []
            if tags:
                for tag_group in tags:
                    if isinstance(tag_group, list):
                        tags_flat.extend(tag_group)
                    else:
                        tags_flat.append(tag_group)
            
            emoji = ""
            if "suspectedPhishingWallet" in tags_flat and "diamondHands" not in tags_flat:
                emoji = "🐟"
            elif "diamondHands" in tags_flat and "suspectedPhishingWallet" not in tags_flat:
                emoji = "💎"
            elif "suspectedPhishingWallet" in tags_flat and "diamondHands" in tags_flat:
                emoji = "🐠"
            
            # 处理利润，转换为k单位
            realized_profit = float(item.get("realizedProfit", 0))
            profit_k = round(realized_profit / 1000, 1)
            
            traders.append({
                "address": item.get("holderWalletAddress"),
                "realizedProfit": profit_k,
                "emoji": emoji,
                "originalProfit": realized_profit
            })
        
        print(f"✅ 成功获取 {len(traders)} 个 Traders")
        return traders
        
    except Exception as e:
        print(f"❌ 获取Traders失败: {e}")
        import traceback
        traceback.print_exc()
        return []


def fetch_wallet_profile(chain_id, wallet_address, period_type=5):
    """获取钱包profile信息"""
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


def check_conspiracy_wallet(wallet_address, chain_id="501", days_before=10):
    """检查是否为阴谋钱包
    
    Args:
        wallet_address: 钱包地址
        chain_id: 链ID，默认501(Solana)
        days_before: 检查多少天前的数据，默认10天
    
    Returns:
        bool: True表示是阴谋钱包，False表示不是
    """
    print(f"🔍 检查钱包 {wallet_address[:8]}... 是否为阴谋钱包")
    
    # 获取3个月的钱包数据
    wallet_data = fetch_wallet_profile(chain_id, wallet_address, period_type=5)
    
    if not wallet_data:
        print(f"❌ 无法获取钱包数据")
        return False
    
    # 获取历史PnL数据
    date_pnl_list = wallet_data.get("datePnlList", [])
    
    if not date_pnl_list:
        print(f"❌ 无历史PnL数据")
        return False
    
    # 计算N天前的时间戳
    cutoff_timestamp = int((datetime.now() - timedelta(days=days_before)).timestamp() * 1000)
    
    print(f"📅 检查 {days_before} 天前 ({datetime.fromtimestamp(cutoff_timestamp/1000).strftime('%Y-%m-%d')}) 的数据")
    
    # 检查N天前的所有profit是否都为0
    old_records = []
    for item in date_pnl_list:
        timestamp = item.get("timestamp", 0)
        profit = float(item.get("profit", 0))
        
        if timestamp < cutoff_timestamp:
            old_records.append(profit)
    
    if not old_records:
        print(f"💡 没有 {days_before} 天前的数据，可能是新钱包")
        return True
    
    # 如果所有旧记录的profit都是0或接近0，认为是阴谋钱包
    all_zero = all(abs(profit) < 1 for profit in old_records)
    
    if all_zero:
        print(f"🐟 确认为阴谋钱包: {days_before}天前的{len(old_records)}条记录profit都为0")
        return True
    else:
        print(f"✅ 非阴谋钱包: {days_before}天前有非零交易记录")
        return False


def generate_remark_name(ca_name, holder_data=None, trader_data=None, is_conspiracy=False):
    """生成备注名称
    
    规则:
    1. 只在Holders: {name}-持{percentage}%
    2. 只在Traders: {name}-盈{profit}k  
    3. 既在Holders又在Traders: {name}-持{percentage}%-盈{profit}k
    4. 如果是阴谋钱包: 新-{name}-...
    """
    # 基础名称部分
    base_parts = [ca_name]
    
    # 持仓信息
    if holder_data:
        percentage = float(holder_data.get("holdAmountPercentage", 0))
        # 格式化：去掉多余的0，保留必要的小数位
        if percentage == int(percentage):
            base_parts.append(f"持{int(percentage)}%")
        else:
            base_parts.append(f"持{percentage:.1f}%")
    
    # 盈利信息  
    if trader_data:
        profit_k = trader_data.get("realizedProfit", 0)
        # 格式化：如果是整数就不显示小数点
        if profit_k == int(profit_k):
            base_parts.append(f"盈{int(profit_k)}k")
        else:
            base_parts.append(f"盈{profit_k}k")
    
    base_name = "-".join(base_parts)
    
    # 如果是阴谋钱包，在前面加"新-"
    if is_conspiracy:
        return f"新-{base_name}"
    else:
        return base_name


def merge_and_format(holders, traders, ca_name):
    """合并 holders 和 traders 数据并格式化"""
    result = []
    address_map = {}
    
    print(f"🔄 开始合并数据...")
    print(f"  - Holders: {len(holders) if holders else 0}")
    print(f"  - Traders: {len(traders) if traders else 0}")
    
    # 处理 holders
    if holders:
        for holder in holders:
            addr = holder.get("address")
            if addr:
                address_map[addr] = {
                    "address": addr,
                    "emoji": holder.get("emoji", ""),
                    "holder_data": holder,
                    "trader_data": None
                }
    
    # 处理 traders，如果地址已存在则合并，否则新增
    if traders:
        for trader in traders:
            addr = trader.get("address")
            if addr:
                if addr in address_map:
                    # 地址已存在，添加trader信息
                    address_map[addr]["trader_data"] = trader
                    # 更新emoji（trader的emoji可能更准确）
                    if trader.get("emoji"):
                        address_map[addr]["emoji"] = trader.get("emoji")
                else:
                    # 新地址，只有trader信息
                    address_map[addr] = {
                        "address": addr,
                        "emoji": trader.get("emoji", ""),
                        "holder_data": None,
                        "trader_data": trader
                    }
    
    # 生成最终结果
    for item in address_map.values():
        addr = item["address"]
        emoji = item["emoji"]
        holder_data = item["holder_data"]
        trader_data = item["trader_data"]
        
        # 生成备注名称
        name = generate_remark_name(ca_name, holder_data, trader_data)
        
        result.append({
            "address": addr,
            "emoji": emoji,
            "name": name
        })
    
    # 统计信息
    only_holders = len([r for r in result if '持' in r['name'] and '盈' not in r['name']])
    only_traders = len([r for r in result if '盈' in r['name'] and '持' not in r['name']])
    both = len([r for r in result if '持' in r['name'] and '盈' in r['name']])
    
    print(f"✅ 数据合并完成:")
    print(f"  - 总地址数: {len(result)}")
    print(f"  - 仅Holders: {only_holders}")
    print(f"  - 仅Traders: {only_traders}")
    print(f"  - 既是Holder又是Trader: {both}")
    
    return result


def generate_address_remarks(
    ca_address, 
    ca_name, 
    top_holders_count=20, 
    top_traders_count=20,
    conspiracy_check=False,
    conspiracy_days=10
):
    """生成地址备注
    
    Args:
        ca_address: CA地址
        ca_name: CA名称
        top_holders_count: 获取持有者数量
        top_traders_count: 获取交易者数量
        conspiracy_check: 是否进行阴谋钱包检查
        conspiracy_days: 阴谋钱包检查天数
    
    Returns:
        dict: {
            "normal_remarks": [{"address": "", "remark": ""}],  # 普通备注
            "conspiracy_remarks": [{"address": "", "remark": ""}]  # 阴谋钱包备注
        }
    """
    print(f"\n🚀 开始处理CA: {ca_name} ({ca_address})")
    
    # 获取数据
    holders_data = fetch_top_holders("501", ca_address, top_holders_count)
    traders_data = fetch_top_traders("501", ca_address, top_traders_count)
    
    # 合并数据
    address_map = {}
    
    # 处理 holders
    if holders_data:
        for holder in holders_data:
            addr = holder.get("address")
            if addr:
                address_map[addr] = {
                    "holder": holder,
                    "trader": None
                }
    
    # 处理 traders
    if traders_data:
        for trader in traders_data:
            addr = trader.get("address")
            if addr:
                if addr in address_map:
                    address_map[addr]["trader"] = trader
                else:
                    address_map[addr] = {
                        "holder": None,
                        "trader": trader
                    }
    
    normal_remarks = []
    conspiracy_remarks = []
    
    for address, data in address_map.items():
        holder_data = data.get("holder")
        trader_data = data.get("trader")
        
        # 检查是否为阴谋钱包
        is_conspiracy = False
        if conspiracy_check:
            try:
                is_conspiracy = check_conspiracy_wallet(address, chain_id="501", days_before=conspiracy_days)
            except Exception as e:
                print(f"❌ 检查钱包 {address[:8]}... 阴谋状态失败: {e}")
                is_conspiracy = False
        
        # 生成备注
        remark = generate_remark_name(ca_name, holder_data, trader_data, is_conspiracy)
        
        # 分类存储
        remark_data = {"address": address, "remark": remark}
        
        if is_conspiracy:
            conspiracy_remarks.append(remark_data)
        else:
            normal_remarks.append(remark_data)
    
    print(f"\n✅ 处理完成！")
    print(f"📊 普通地址: {len(normal_remarks)} 个")
    print(f"🐟 阴谋钱包: {len(conspiracy_remarks)} 个")
    
    return {
        "normal_remarks": normal_remarks,
        "conspiracy_remarks": conspiracy_remarks
    }


# 测试函数
def test_functions():
    """测试所有功能"""
    print("🧪 开始测试...")
    
    # 测试备注生成
    print("\n1. 测试备注生成规则:")
    test_cases = [
        {
            "name": "仅Holder",
            "holder_data": {"holdAmountPercentage": "2.5"},
            "trader_data": None,
            "expected": "USDT-持2.5%"
        },
        {
            "name": "仅Trader", 
            "holder_data": None,
            "trader_data": {"realizedProfit": 150.0},
            "expected": "USDT-盈150k"
        },
        {
            "name": "既是Holder又是Trader",
            "holder_data": {"holdAmountPercentage": "2.5"},
            "trader_data": {"realizedProfit": 150.5},
            "expected": "USDT-持2.5%-盈150.5k"
        }
    ]
    
    for case in test_cases:
        result = generate_remark_name("USDT", case["holder_data"], case["trader_data"])
        status = "✅" if result == case["expected"] else "❌"
        print(f"  {status} {case['name']}: {result}")
    
    # 测试API
    print("\n2. 测试API获取:")
    holders = fetch_top_holders("501", "6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk", limit=3)
    traders = fetch_top_traders("501", "6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk", limit=3)
    
    if holders or traders:
        result = merge_and_format(holders, traders, "USDT")
        print(f"\n✅ API测试成功! 生成 {len(result)} 条备注数据")
        
        # 显示结果
        for i, item in enumerate(result[:5]):
            print(f"  {i+1}. {item['address'][:8]}...{item['address'][-6:]} -> {item['emoji']} {item['name']}")
    else:
        print("❌ API测试失败")


if __name__ == "__main__":
    test_functions()