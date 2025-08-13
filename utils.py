import requests
import time
import json
import pandas as pd
from io import BytesIO
from collections import defaultdict

def fetch_data(url, params=None, method='GET', json_data=None, max_retries=3, timeout=10):
    """通用数据获取函数，带错误处理和重试机制"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    retries = max_retries
    for attempt in range(retries):
        try:
            if method == 'GET':
                response = requests.get(
                    url, 
                    headers=headers,
                    params=params,
                    timeout=timeout,
                    verify=False
                )
            else:  # POST
                response = requests.post(
                    url, 
                    headers=headers,
                    json=json_data,
                    timeout=timeout,
                    verify=False
                )
                
            response.raise_for_status()
            json_data = response.json()
            
            if json_data.get('code') != 0:
                error_msg = json_data.get('msg', '未知错误')
                print(f"API返回错误: {error_msg}")
                return None
            
            return json_data.get('data', {})
            
        except requests.exceptions.RequestException as e:
            print(f"请求失败 (尝试 {attempt+1}/{retries}): {e}")
            time.sleep(2 * (attempt + 1))  # 递增延时
        except ValueError as e:
            print(f"JSON解析失败: {e}")
            return None
    return None

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