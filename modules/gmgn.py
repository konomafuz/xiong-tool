import requests
import time

def fetch_top_holders(chain_id, token_address):
    url = f"https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
    params = {
        "chainId": chain_id,
        "tokenAddress": token_address,
        "t": int(time.time() * 1000)
    }
    resp = requests.get(url, params=params, timeout=10)
    try:
        data = resp.json()
    except Exception:
        raise Exception("接口返回不是JSON格式")
    if not data or "data" not in data or "holderRankingList" not in data["data"]:
        raise Exception(f"接口返回异常: {data}")
    holders = []
    for item in data["data"]["holderRankingList"]:
        if not item.get("boughtAmount"):
            continue
        tags = item.get("tagList", [])
        tags_flat = [tag for sub in tags for tag in sub]
        emoji = ""
        if "suspectedPhishingWallet" in tags_flat and "diamondHands" not in tags_flat:
            emoji = "🐟"
        elif "diamondHands" in tags_flat and "suspectedPhishingWallet" not in tags_flat:
            emoji = "💎"
        elif "suspectedPhishingWallet" in tags_flat and "diamondHands" in tags_flat:
            emoji = "🐠"
        holders.append({
            "address": item.get("holderWalletAddress"),
            "pic": emoji,
            "name": None
        })
    return holders

def fetch_top_traders(chain_id, token_address):
    url = f"https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
    params = {
        "chainId": chain_id,
        "tokenContractAddress": token_address,
        "t": int(time.time() * 1000)
    }
    resp = requests.get(url, params=params, timeout=10)
    try:
        data = resp.json()
    except Exception:
        raise Exception("接口返回不是JSON格式")
    if not data or "data" not in data or "list" not in data["data"]:
        raise Exception(f"接口返回异常: {data}")
    traders = []
    for item in data["data"]["list"]:
        tags = item.get("tagList", [])
        tags_flat = [tag for sub in tags for tag in sub]
        emoji = ""
        if "suspectedPhishingWallet" in tags_flat and "diamondHands" not in tags_flat:
            emoji = "🐟"
        elif "diamondHands" in tags_flat and "suspectedPhishingWallet" not in tags_flat:
            emoji = "💎"
        elif "suspectedPhishingWallet" in tags_flat and "diamondHands" in tags_flat:
            emoji = "🐠"
        profit_k = round(float(item.get("realizedProfit", 0)) / 1000, 2)
        traders.append({
            "address": item.get("holderWalletAddress"),
            "pic": emoji,
            "realizedProfit": profit_k,
            "name": None
        })
    return traders

def merge_and_format(holders, traders, ca_name):
    addr_map = {}
    # holders
    for idx, h in enumerate(holders, 1):
        addr = h["address"]
        if not addr:
            continue
        addr_map[addr] = {
            "address": addr,
            "emoji": h["pic"],
            "name": f"{ca_name}-top{idx}"
        }
    # traders
    for idx, t in enumerate(traders, 1):
        addr = t["address"]
        if not addr:
            continue
        if addr in addr_map:
            addr_map[addr]["name"] = f"{ca_name}-盈利{t['realizedProfit']}k-top{idx}"
            addr_map[addr]["emoji"] = t["pic"]
        else:
            addr_map[addr] = {
                "address": addr,
                "emoji": t["pic"],
                "name": f"{ca_name}-盈利{t['realizedProfit']}k"
            }
    return list(addr_map.values())