import pandas as pd
import requests
import time
import sys
import os
import urllib3

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def fetch_okx_data(url, params=None, timeout=15):
    """专门用于OKX API的请求函数"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.okx.com/',
        'Origin': 'https://www.okx.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin'
    }
    
    try:
        print(f"🌐 发送请求到: {url}")
        print(f"📝 请求参数: {params}")
        
        # 确保直接请求 OKX API
        response = requests.get(
            url, 
            params=params, 
            headers=headers, 
            timeout=timeout,
            verify=True  # 使用SSL验证
        )
        
        print(f"📊 响应状态码: {response.status_code}")
        print(f"🔗 实际请求URL: {response.url}")
        
        response.raise_for_status()
        
        result = response.json()
        print(f"✅ 响应code: {result.get('code')}")
        print(f"📄 响应消息: {result.get('msg', 'N/A')}")
        
        return result
        
    except requests.exceptions.RequestException as e:
        print(f"❌ 网络请求失败: {e}")
        return None
    except ValueError as e:
        print(f"❌ JSON解析失败: {e}")
        print(f"原始响应: {response.text[:500] if 'response' in locals() else 'N/A'}")
        return None
    except Exception as e:
        print(f"❌ 未知错误: {e}")
        return None


def get_all_holders(chain_id, token_address, timestamp=None, top_n=100):
    """获取指定时间点的前N大持仓地址"""
    # 确保使用正确的 OKX API URL
    url = "https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
    
    # 验证参数
    if not chain_id or not token_address:
        print("❌ 参数错误: chain_id 和 token_address 不能为空")
        return pd.DataFrame()
    
    # 准备参数
    params = {
        "chainId": str(chain_id),
        "tokenAddress": token_address,
        "limit": min(top_n, 100),
        "offset": 0
    }
    
    # 添加时间戳
    if timestamp:
        params["t"] = int(timestamp)
    else:
        params["t"] = int(time.time() * 1000)
    
    print(f"🎯 开始获取持仓数据...")
    print(f"🔗 链ID: {chain_id}")
    print(f"💰 代币地址: {token_address}")
    print(f"📊 目标数量: {top_n}")
    
    all_holders = []
    page_count = 0
    max_pages = 20
    
    try:
        while len(all_holders) < top_n and page_count < max_pages:
            print(f"\n📄 正在请求第 {page_count + 1} 页，已获取 {len(all_holders)} 条数据")
            
            # 使用专门的请求函数
            response = fetch_okx_data(url, params)
            
            if not response:
                print("❌ API响应为空，停止请求")
                break
            
            # 检查响应状态
            response_code = response.get('code')
            if response_code != 0:
                error_msg = response.get('error_message') or response.get('msg') or 'Unknown error'
                print(f"❌ API返回错误: code={response_code}, message={error_msg}")
                break
                
            # 获取持仓列表
            data_obj = response.get('data', {})
            holder_list = data_obj.get('holderRankingList', [])
            
            if not holder_list:
                print("⚠️ holderRankingList 为空")
                # 打印完整响应以便调试
                print(f"完整响应: {response}")
                break
            
            print(f"✅ 本页获取到 {len(holder_list)} 条数据")
            all_holders.extend(holder_list)
            
            # 检查是否已到最后一页
            if len(holder_list) < params['limit']:
                print("🏁 已到最后一页")
                break
                
            # 更新offset到下一页
            params['offset'] += params['limit']
            page_count += 1
            
            # 避免请求过快
            print("⏳ 等待0.8秒...")
            time.sleep(0.8)
    
    except Exception as e:
        print(f"❌ 获取数据时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()
    
    # 检查结果
    if not all_holders:
        print(f"❌ 没有获取到任何持仓数据")
        print(f"📝 请求参数: chainId={chain_id}, tokenAddress={token_address}")
        return pd.DataFrame()
    
    print(f"🎉 总共获取到 {len(all_holders)} 条持仓数据")
    
    # 处理数据
    try:
        df = pd.DataFrame(all_holders)
        print(f"📋 原始数据字段: {df.columns.tolist()}")
        
        # 显示第一条数据
        if len(df) > 0:
            print(f"📊 数据样例:")
            sample_data = df.iloc[0].to_dict()
            for key, value in list(sample_data.items())[:5]:  # 只显示前5个字段
                print(f"  {key}: {value}")
        
        # 检查必要字段
        required_fields = ['holderWalletAddress', 'holdAmount', 'holdAmountPercentage']
        missing_fields = [field for field in required_fields if field not in df.columns]
        
        if missing_fields:
            print(f"⚠️ 缺少必要字段: {missing_fields}")
            print(f"📋 可用字段: {df.columns.tolist()}")
            return df  # 返回原始数据
        
        # 处理数据
        df_processed = df[required_fields].copy()
        df_processed.columns = ['address', 'balance', 'percentage']
        
        # 数据类型转换
        df_processed['balance'] = pd.to_numeric(df_processed['balance'], errors='coerce')
        df_processed['percentage'] = pd.to_numeric(df_processed['percentage'], errors='coerce')
        
        # 添加额外字段
        extra_fields = {
            'chainId': 'chain_id',
            'explorerUrl': 'explorer_url',
            'holdCreateTime': 'hold_create_time'
        }
        
        for original_field, new_field in extra_fields.items():
            if original_field in df.columns:
                df_processed[new_field] = df[original_field]
        
        # 限制数量并排序
        df_processed = df_processed.head(top_n)
        df_processed = df_processed.sort_values('percentage', ascending=False).reset_index(drop=True)
        
        print(f"✅ 数据处理完成!")
        print(f"📊 前5名持仓地址:")
        print(df_processed[['address', 'balance', 'percentage']].head())
        
        return df_processed
        
    except Exception as e:
        print(f"❌ 数据处理失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()


def test_connection():
    """测试网络连接"""
    print("🔍 测试网络连接...")
    
    try:
        # 测试基本连接
        response = requests.get("https://www.okx.com", timeout=10)
        print(f"✅ OKX主站连接正常: {response.status_code}")
        
        # 测试API连接
        api_url = "https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
        params = {
            "chainId": "501",
            "tokenAddress": "6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk",
            "t": int(time.time() * 1000),
            "limit": 1,
            "offset": 0
        }
        
        response = fetch_okx_data(api_url, params)
        if response and response.get('code') == 0:
            print("✅ API连接测试成功")
            return True
        else:
            print("❌ API连接测试失败")
            return False
            
    except Exception as e:
        print(f"❌ 连接测试失败: {e}")
        return False


if __name__ == "__main__":
    print("🚀 开始测试 OKX Holders API...")
    
    # 先测试连接
    if not test_connection():
        print("❌ 网络连接有问题，请检查网络设置")
        exit(1)
    
    # 测试获取数据
    print("\n" + "="*60)
    print("🧪 测试数据获取功能")
    print("="*60)
    
    test_params = {
        "chain_id": "501",
        "token_address": "6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk",
        "top_n": 10
    }
    
    print(f"📝 测试参数: {test_params}")
    
    df = get_all_holders(**test_params)
    
    if not df.empty:
        print(f"\n🎉 测试成功！获取到 {len(df)} 条持仓数据")
        
        # 导出CSV
        csv_filename = f"holders_test_{int(time.time())}.csv"
        df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        print(f"💾 数据已导出到: {csv_filename}")
        
        # 显示统计信息
        print(f"\n📊 数据统计:")
        print(f"  总持仓地址数: {len(df)}")
        print(f"  平均持仓比例: {df['percentage'].mean():.4f}%")
        print(f"  最大持仓比例: {df['percentage'].max():.4f}%")
        
    else:
        print("❌ 测试失败，未获取到数据")
        print("\n🔍 请检查:")
        print("  1. 网络连接是否正常")
        print("  2. 代币地址是否正确")
        print("  3. 是否被API限制访问")