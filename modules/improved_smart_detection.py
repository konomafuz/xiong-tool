#!/usr/bin/env python3
"""
改进的智能小号检测算法
基于持仓相似性和交易模式分析
"""

import time
import math
from collections import defaultdict, Counter
from utils import fetch_data_robust

class ImprovedSmartAccountDetector:
    def __init__(self):
        self.similarity_threshold = 0.3  # 相似度阈值
        self.min_common_tokens = 3       # 最少共同代币数
        
    def get_token_holders(self, chain_id, token_address, limit=100):
        """获取代币的持有者列表"""
        url = "https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
        params = {
            "chainId": chain_id,
            "tokenAddress": token_address,
            "limit": limit,
            "offset": 0,
            "t": int(time.time() * 1000)
        }
        
        # 如果是ETH链，添加额外参数
        if str(chain_id) == "1":
            params["currentUserWalletAddress"] = "0x63291f7d06ea0a17306c5e48779baae289865e99"
        
        response = fetch_data_robust(url, params, max_retries=2, timeout=10)
        
        if response and "data" in response and response["data"]:
            holder_list = response["data"].get("holderRankingList", [])
            return [holder.get("holderWalletAddress") for holder in holder_list if holder.get("holderWalletAddress")]
        
        return []
    
    def get_wallet_tokens(self, wallet_address, chain_id):
        """获取钱包的代币持仓列表"""
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/token-list"
        params = {
            "walletAddress": wallet_address,
            "chainId": chain_id,
            "isAsc": "false",
            "sortType": "1",
            "filterEmptyBalance": "false",
            "offset": 0,
            "limit": 100,
            "t": int(time.time() * 1000)
        }
        
        # 如果是ETH链，添加额外参数
        if str(chain_id) == "1":
            params["currentUserWalletAddress"] = "0x63291f7d06ea0a17306c5e48779baae289865e99"
        
        response = fetch_data_robust(url, params, max_retries=2, timeout=15)
        
        if response and "data" in response and response["data"]:
            token_list = response["data"].get("tokenList", [])
            return [token.get("tokenContractAddress") for token in token_list if token.get("tokenContractAddress")]
        
        return []
    
    def calculate_portfolio_similarity(self, tokens1, tokens2):
        """计算两个投资组合的相似度"""
        if not tokens1 or not tokens2:
            return 0
        
        set1 = set(tokens1)
        set2 = set(tokens2)
        
        # 计算Jaccard相似度
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        if union == 0:
            return 0
        
        return intersection / union
    
    def detect_by_portfolio_similarity(self, target_address, chain_id, max_tokens=20):
        """方案1: 基于投资组合相似性检测小号"""
        print(f"🔍 方案1: 分析 {target_address[:8]}... 的投资组合相似性")
        
        # 1. 获取目标地址的代币列表
        target_tokens = self.get_wallet_tokens(target_address, chain_id)
        if not target_tokens:
            print("❌ 目标地址没有代币持仓数据")
            return []
        
        print(f"🎯 目标地址持有 {len(target_tokens)} 个代币")
        
        # 2. 对于目标地址持有的每个代币，获取其他持有者
        candidate_addresses = set()
        
        for i, token_addr in enumerate(target_tokens[:max_tokens]):
            print(f"📊 分析代币 {i+1}/{min(len(target_tokens), max_tokens)}: {token_addr[:8]}...")
            
            holders = self.get_token_holders(chain_id, token_addr, limit=50)
            candidate_addresses.update(holders)
            
            time.sleep(0.5)  # API限速
        
        # 移除目标地址自身
        candidate_addresses.discard(target_address)
        print(f"🔍 发现 {len(candidate_addresses)} 个候选地址")
        
        # 3. 计算每个候选地址与目标地址的相似度
        similarity_scores = []
        
        for i, candidate in enumerate(list(candidate_addresses)[:50]):  # 限制分析数量
            if i % 10 == 0:
                print(f"📈 进度: {i+1}/50")
            
            candidate_tokens = self.get_wallet_tokens(candidate, chain_id)
            similarity = self.calculate_portfolio_similarity(target_tokens, candidate_tokens)
            
            if similarity >= self.similarity_threshold:
                common_tokens = len(set(target_tokens) & set(candidate_tokens))
                if common_tokens >= self.min_common_tokens:
                    similarity_scores.append((candidate, similarity, common_tokens))
            
            time.sleep(0.3)  # API限速
        
        # 4. 按相似度排序
        similarity_scores.sort(key=lambda x: (x[1], x[2]), reverse=True)
        
        return similarity_scores
    
    def detect_by_top_traders_overlap(self, target_address, chain_id):
        """方案2: 基于盈利榜重叠检测"""
        print(f"🔍 方案2: 分析盈利榜重叠")
        
        # 获取目标地址交易过的代币
        target_tokens = self.get_wallet_tokens(target_address, chain_id)
        if not target_tokens:
            return []
        
        overlap_counter = defaultdict(int)
        
        # 对每个代币查询盈利榜
        for token_addr in target_tokens[:10]:  # 限制数量
            top_traders = self.get_top_traders_for_token(chain_id, token_addr)
            
            for trader_addr in top_traders:
                if trader_addr != target_address:
                    overlap_counter[trader_addr] += 1
            
            time.sleep(0.5)
        
        # 返回出现频率高的地址
        frequent_addresses = [(addr, count) for addr, count in overlap_counter.items() if count >= 3]
        frequent_addresses.sort(key=lambda x: x[1], reverse=True)
        
        return frequent_addresses
    
    def get_top_traders_for_token(self, chain_id, token_address):
        """获取代币的盈利榜地址"""
        url = "https://web3.okx.com/priapi/v1/dx/market/v2/pnl/top-trader/ranking-list"
        params = {
            "chainId": chain_id,
            "tokenContractAddress": token_address,
            "limit": 50,
            "offset": 0,
            "t": int(time.time() * 1000)
        }
        
        # 如果是ETH链，添加额外参数
        if str(chain_id) == "1":
            params["currentUserWalletAddress"] = "0x63291f7d06ea0a17306c5e48779baae289865e99"
        
        response = fetch_data_robust(url, params, max_retries=2, timeout=15)
        
        if response and response.get('code') == 0:
            trader_list = response.get('data', {}).get('list', [])
            return [trader.get('holderWalletAddress') for trader in trader_list if trader.get('holderWalletAddress')]
        
        return []
    
    def comprehensive_detection(self, target_address, chain_id):
        """综合检测方法"""
        print(f"\n🚀 开始综合检测小号: {target_address[:8]}...")
        
        results = {
            'portfolio_similarity': [],
            'trader_overlap': [],
            'combined_score': []
        }
        
        # 方案1: 投资组合相似性
        try:
            results['portfolio_similarity'] = self.detect_by_portfolio_similarity(target_address, chain_id)
            print(f"✅ 投资组合相似性检测完成，发现 {len(results['portfolio_similarity'])} 个疑似小号")
        except Exception as e:
            print(f"❌ 投资组合相似性检测失败: {e}")
        
        # 方案2: 盈利榜重叠
        try:
            results['trader_overlap'] = self.detect_by_top_traders_overlap(target_address, chain_id)
            print(f"✅ 盈利榜重叠检测完成，发现 {len(results['trader_overlap'])} 个疑似小号")
        except Exception as e:
            print(f"❌ 盈利榜重叠检测失败: {e}")
        
        # 综合评分
        combined_addresses = {}
        
        # 投资组合相似性权重
        for addr, similarity, common_tokens in results['portfolio_similarity']:
            score = similarity * 0.7 + (common_tokens / 20) * 0.3  # 相似度70%权重，共同代币30%权重
            combined_addresses[addr] = combined_addresses.get(addr, 0) + score
        
        # 盈利榜重叠权重
        for addr, overlap_count in results['trader_overlap']:
            score = min(overlap_count / 10, 1.0) * 0.5  # 重叠次数归一化，50%权重
            combined_addresses[addr] = combined_addresses.get(addr, 0) + score
        
        # 综合排序
        results['combined_score'] = sorted(combined_addresses.items(), key=lambda x: x[1], reverse=True)
        
        return results

# 测试函数
def test_improved_detection():
    detector = ImprovedSmartAccountDetector()
    
    # 测试地址
    test_cases = [
        ("38tAutsiZWaJ8MsMJVgKCx4U5LHwZM2g6StmQpKLRqz6", "501"),  # Solana
        ("0x424de83e135d0be9a4b6b1268b04bcd4d92f7c98", "1"),      # Ethereum
    ]
    
    for target_address, chain_id in test_cases:
        chain_name = "Solana" if chain_id == "501" else "Ethereum"
        print(f"\n{'='*60}")
        print(f"🔍 测试 {chain_name} 链地址: {target_address}")
        print(f"{'='*60}")
        
        try:
            results = detector.comprehensive_detection(target_address, chain_id)
            
            print(f"\n📊 检测结果汇总:")
            print(f"投资组合相似性: {len(results['portfolio_similarity'])} 个")
            print(f"盈利榜重叠: {len(results['trader_overlap'])} 个")
            print(f"综合评分: {len(results['combined_score'])} 个")
            
            # 显示前5个结果
            if results['combined_score']:
                print(f"\n🏆 综合评分 TOP 5:")
                for i, (addr, score) in enumerate(results['combined_score'][:5]):
                    print(f"  {i+1}. {addr[:8]}...{addr[-6:]} (评分: {score:.3f})")
            
        except Exception as e:
            print(f"❌ 检测失败: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_improved_detection()
