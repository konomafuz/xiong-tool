import requests
import time

def simple_test():
    url = "https://www.okx.com/priapi/v1/dx/market/v2/holders/ranking-list"
    
    params = {
        "chainId": "501",
        "tokenAddress": "6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk",  # USDT
        "t": int(time.time() * 1000),
        "limit": 5,
        "offset": 0
    }
    
    print("发送请求...")
    print(f"URL: {url}")
    print(f"参数: {params}")
    
    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"状态码: {response.status_code}")
        print(f"响应内容: {response.text[:1000]}...")
        
        if response.status_code == 200:
            data = response.json()
            print(f"JSON结构: {list(data.keys()) if isinstance(data, dict) else '非字典'}")
        
    except Exception as e:
        print(f"请求失败: {e}")

if __name__ == "__main__":
    simple_test()