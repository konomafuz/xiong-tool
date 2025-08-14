import requests
import time
import ssl
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import json
import pandas as pd
from io import BytesIO
from collections import defaultdict

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def create_robust_session():
    """创建一个更健壮的requests session"""
    session = requests.Session()
    
    # 配置重试策略
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    # 设置请求头
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site'
    })
    
    return session

def fetch_data_robust(url, params=None, max_retries=3, timeout=15):
    """更健壮的数据获取函数"""
    session = create_robust_session()
    
    for attempt in range(max_retries):
        try:
            print(f"🔄 请求 {url[:50]}... (尝试 {attempt + 1}/{max_retries})")
            
            response = session.get(
                url, 
                params=params, 
                timeout=timeout,
                verify=False,  # 暂时禁用SSL验证
                allow_redirects=True
            )
            
            response.raise_for_status()
            
            # 检查响应内容
            if response.content:
                data = response.json()
                print(f"✅ 请求成功")
                return data
            else:
                print(f"❌ 响应内容为空")
                continue
                
        except requests.exceptions.SSLError as e:
            print(f"❌ SSL错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # 指数退避
                continue
            else:
                print("🔄 尝试使用不同的SSL配置...")
                # 最后一次尝试：使用更宽松的SSL设置
                try:
                    import ssl
                    ssl_context = ssl.create_default_context()
                    ssl_context.check_hostname = False
                    ssl_context.verify_mode = ssl.CERT_NONE
                    
                    with requests.Session() as fallback_session:
                        fallback_session.verify = False
                        fallback_response = fallback_session.get(
                            url, 
                            params=params, 
                            timeout=timeout
                        )
                        fallback_response.raise_for_status()
                        return fallback_response.json()
                        
                except Exception as fallback_error:
                    print(f"❌ 备用方案也失败: {fallback_error}")
                    return None
                    
        except requests.exceptions.ConnectionError as e:
            print(f"❌ 连接错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
                
        except requests.exceptions.Timeout as e:
            print(f"❌ 请求超时 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
                
        except requests.exceptions.RequestException as e:
            print(f"❌ 请求异常 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
                
        except ValueError as e:
            print(f"❌ JSON解析错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
                continue
                
        except Exception as e:
            print(f"❌ 未知错误 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
    
    print(f"❌ 所有重试均失败")
    return None

# 保持原有的fetch_data函数以向后兼容
def fetch_data(url, params=None):
    """原有的fetch_data函数，现在调用更健壮的版本"""
    return fetch_data_robust(url, params, max_retries=3, timeout=15)

def export_to_excel(df, filename_prefix):
    """将DataFrame导出为Excel"""
    if df.empty:
        return None
        
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    
    return send_file(
        output,
        as_attachment=True,
        download_name=f"{filename_prefix}_{int(time.time())}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )