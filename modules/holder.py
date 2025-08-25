import pandas as pd
import requests
import time
import sys
import os
import urllib3
import datetime
import json
import sqlite3
import threading
import schedule
from pathlib import Path
from typing import Dict, List, Optional
import logging

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HolderCollectionTask:
    """Holder数据采集任务类"""
    
    def __init__(self, task_id: str, token_address: str, token_symbol: str, 
                 chain: str, interval_hours: int, max_records: int = 1000,
                 description: str = ""):
        self.task_id = task_id
        self.token_address = token_address
        self.token_symbol = token_symbol
        self.chain = chain
        self.interval_hours = interval_hours
        self.max_records = max_records
        self.description = description
        self.created_at = datetime.datetime.now()
        self.last_run = None
        self.next_run = None
        self.status = "active"  # active, paused, stopped
        self.total_collections = 0
        self.last_error = None
        
    def to_dict(self):
        """转换为字典格式"""
        return {
            'task_id': self.task_id,
            'token_address': self.token_address,
            'token_symbol': self.token_symbol,
            'chain': self.chain,
            'interval_hours': self.interval_hours,
            'max_records': self.max_records,
            'description': self.description,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'last_run': self.last_run.isoformat() if self.last_run else None,
            'next_run': self.next_run.isoformat() if self.next_run else None,
            'status': self.status,
            'total_collections': self.total_collections,
            'last_error': self.last_error
        }
    
    @classmethod
    def from_dict(cls, data: Dict):
        """从字典创建任务对象"""
        task = cls(
            task_id=data['task_id'],
            token_address=data['token_address'],
            token_symbol=data['token_symbol'],
            chain=data['chain'],
            interval_hours=data['interval_hours'],
            max_records=data.get('max_records', 1000),
            description=data.get('description', "")
        )
        
        if data.get('created_at'):
            task.created_at = datetime.datetime.fromisoformat(data['created_at'])
        if data.get('last_run'):
            task.last_run = datetime.datetime.fromisoformat(data['last_run'])
        if data.get('next_run'):
            task.next_run = datetime.datetime.fromisoformat(data['next_run'])
            
        task.status = data.get('status', 'active')
        task.total_collections = data.get('total_collections', 0)
        task.last_error = data.get('last_error')
        
        return task


class HolderDataCollector:
    """Holder数据自动采集器"""
    
    def __init__(self, db_path: str = "holders_snapshots.db"):
        self.db_path = db_path
        self.tasks: Dict[str, HolderCollectionTask] = {}
        self.scheduler_thread = None
        self.is_running = False
        self.init_database()
        self.load_tasks()
        
    def init_database(self):
        """初始化数据库"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 创建任务表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS collection_tasks (
                task_id TEXT PRIMARY KEY,
                token_address TEXT NOT NULL,
                token_symbol TEXT NOT NULL,
                chain TEXT NOT NULL,
                interval_hours INTEGER NOT NULL,
                max_records INTEGER DEFAULT 1000,
                description TEXT,
                created_at TIMESTAMP,
                last_run TIMESTAMP,
                next_run TIMESTAMP,
                status TEXT DEFAULT 'active',
                total_collections INTEGER DEFAULT 0,
                last_error TEXT
            )
        ''')
        
        # 创建快照数据表
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS holder_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                snapshot_time TIMESTAMP NOT NULL,
                holder_address TEXT NOT NULL,
                token_address TEXT NOT NULL,
                balance TEXT NOT NULL,
                percentage REAL,
                rank_position INTEGER,
                value_usd REAL,
                FOREIGN KEY (task_id) REFERENCES collection_tasks (task_id)
            )
        ''')
        
        # 创建索引
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshots_task_time 
            ON holder_snapshots (task_id, snapshot_time)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_snapshots_address 
            ON holder_snapshots (holder_address)
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"✅ 数据库初始化完成: {self.db_path}")
    
    def add_task(self, task: HolderCollectionTask) -> bool:
        """添加采集任务"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查任务是否已存在
            cursor.execute("SELECT task_id FROM collection_tasks WHERE task_id = ?", (task.task_id,))
            if cursor.fetchone():
                logger.warning(f"⚠️ 任务 {task.task_id} 已存在")
                return False
            
            # 插入任务
            cursor.execute('''
                INSERT INTO collection_tasks 
                (task_id, token_address, token_symbol, chain, interval_hours, max_records, 
                 description, created_at, status, total_collections)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                task.task_id, task.token_address, task.token_symbol, task.chain,
                task.interval_hours, task.max_records, task.description,
                task.created_at, task.status, task.total_collections
            ))
            
            conn.commit()
            conn.close()
            
            # 添加到内存
            self.tasks[task.task_id] = task
            
            # 安排定时任务
            self.schedule_task(task)
            
            logger.info(f"✅ 成功添加采集任务: {task.task_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 添加任务失败: {e}")
            return False
    
    def remove_task(self, task_id: str) -> bool:
        """删除采集任务"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 删除任务（但保留历史数据）
            cursor.execute("DELETE FROM collection_tasks WHERE task_id = ?", (task_id,))
            conn.commit()
            conn.close()
            
            # 从内存中移除
            if task_id in self.tasks:
                del self.tasks[task_id]
            
            # 取消定时任务
            schedule.clear(task_id)
            
            logger.info(f"✅ 成功删除任务: {task_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 删除任务失败: {e}")
            return False
    
    def pause_task(self, task_id: str) -> bool:
        """暂停任务"""
        return self.update_task_status(task_id, "paused")
    
    def resume_task(self, task_id: str) -> bool:
        """恢复任务"""
        return self.update_task_status(task_id, "active")
    
    def update_task_status(self, task_id: str, status: str) -> bool:
        """更新任务状态"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute(
                "UPDATE collection_tasks SET status = ? WHERE task_id = ?",
                (status, task_id)
            )
            conn.commit()
            conn.close()
            
            if task_id in self.tasks:
                self.tasks[task_id].status = status
            
            logger.info(f"✅ 任务 {task_id} 状态更新为: {status}")
            return True
            
        except Exception as e:
            logger.error(f"❌ 更新任务状态失败: {e}")
            return False
    
    def load_tasks(self):
        """从数据库加载任务"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT * FROM collection_tasks")
            rows = cursor.fetchall()
            columns = [description[0] for description in cursor.description]
            
            for row in rows:
                task_data = dict(zip(columns, row))
                task = HolderCollectionTask.from_dict(task_data)
                self.tasks[task.task_id] = task
                
                # 为活跃任务安排定时执行
                if task.status == 'active':
                    self.schedule_task(task)
            
            conn.close()
            logger.info(f"✅ 加载了 {len(self.tasks)} 个采集任务")
            
        except Exception as e:
            logger.error(f"❌ 加载任务失败: {e}")
    
    def schedule_task(self, task: HolderCollectionTask):
        """安排定时任务"""
        if task.status != 'active':
            return
        
        # 清除现有的同名任务
        schedule.clear(task.task_id)
            
        # 根据间隔时间安排任务
        if task.interval_hours == 1:
            job = schedule.every().hour.do(self.run_collection, task.task_id).tag(task.task_id)
        elif task.interval_hours == 4:
            job = schedule.every(4).hours.do(self.run_collection, task.task_id).tag(task.task_id)
        elif task.interval_hours == 12:
            job = schedule.every(12).hours.do(self.run_collection, task.task_id).tag(task.task_id)
        elif task.interval_hours == 24:
            job = schedule.every(24).hours.do(self.run_collection, task.task_id).tag(task.task_id)
        else:
            # 自定义间隔
            job = schedule.every(task.interval_hours).hours.do(self.run_collection, task.task_id).tag(task.task_id)
        
        # 从schedule获取实际的下次运行时间
        task.next_run = job.next_run
        logger.info(f"📅 任务 {task.task_id} 已安排，下次运行: {task.next_run}")
    
    def run_collection(self, task_id: str):
        """执行数据采集"""
        if task_id not in self.tasks:
            logger.warning(f"⚠️ 任务 {task_id} 不存在")
            return
        
        task = self.tasks[task_id]
        if task.status != 'active':
            logger.info(f"⏸️ 任务 {task_id} 状态为 {task.status}，跳过执行")
            return
        
        logger.info(f"🚀 开始执行采集任务: {task_id}")
        
        try:
            # 获取holder数据
            holders_data = self.fetch_holders_data(task)
            
            if holders_data:
                # 保存到数据库
                self.save_snapshot(task_id, holders_data)
                
                # 更新任务状态
                task.last_run = datetime.datetime.now()
                task.total_collections += 1
                task.last_error = None
                # 从schedule获取更新后的下次运行时间
                self.update_task_next_run_from_schedule(task_id)
                
                self.update_task_in_db(task)
                
                logger.info(f"✅ 任务 {task_id} 执行完成，采集 {len(holders_data)} 条记录")
            else:
                # 即使未获取到数据也要更新时间，确保任务继续调度
                task.last_run = datetime.datetime.now()
                task.next_run = task.last_run + datetime.timedelta(hours=task.interval_hours)
                task.last_error = "未获取到数据"
                self.update_task_in_db(task)
                logger.warning(f"⚠️ 任务 {task_id} 未获取到数据")
                
        except Exception as e:
            error_msg = str(e)
            task.last_error = error_msg
            # 即使失败也要更新last_run和next_run，确保任务能继续调度
            task.last_run = datetime.datetime.now()
            task.next_run = task.last_run + datetime.timedelta(hours=task.interval_hours)
            self.update_task_in_db(task)
            logger.error(f"❌ 任务 {task_id} 执行失败: {error_msg}")
    
    def fetch_holders_data(self, task: HolderCollectionTask) -> List[Dict]:
        """获取holder数据 - 使用现有的get_all_holders函数"""
        try:
            # 调用原有的数据获取函数
            df = get_all_holders(
                chain_id=task.chain,
                token_address=task.token_address,
                timestamp=None,  # 使用当前时间
                top_n=task.max_records
            )
            
            if df.empty:
                logger.warning(f"⚠️ 未获取到 {task.task_id} 的holder数据")
                return []
            
            # 转换为字典列表
            holders_list = []
            for _, row in df.iterrows():
                holder_data = {
                    'address': row.get('address', ''),
                    'balance': row.get('balance', ''),
                    'percentage': row.get('percentage', 0),
                    'value_usd': row.get('value_usd', 0),
                    'rank': row.get('rank', 0)
                }
                holders_list.append(holder_data)
            
            logger.info(f"✅ 获取到 {len(holders_list)} 条holder数据")
            return holders_list
            
        except Exception as e:
            logger.error(f"❌ 获取holder数据失败: {e}")
            return []
    
    def save_snapshot(self, task_id: str, holders_data: List[Dict]):
        """保存快照数据到数据库"""
        if not holders_data:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        snapshot_time = datetime.datetime.now()
        task = self.tasks[task_id]
        
        # 批量插入数据
        insert_data = []
        for rank, holder in enumerate(holders_data, 1):
            insert_data.append((
                task_id,
                snapshot_time,
                holder.get('address', ''),
                task.token_address,
                holder.get('balance', ''),
                holder.get('percentage', 0),
                rank,
                holder.get('value_usd', 0)
            ))
        
        cursor.executemany('''
            INSERT INTO holder_snapshots 
            (task_id, snapshot_time, holder_address, token_address, balance, 
             percentage, rank_position, value_usd)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', insert_data)
        
        conn.commit()
        conn.close()
        
        logger.info(f"💾 保存快照数据: {len(insert_data)} 条记录")
    
    def update_task_in_db(self, task: HolderCollectionTask):
        """更新数据库中的任务信息"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE collection_tasks 
                SET last_run = ?, next_run = ?, total_collections = ?, last_error = ?
                WHERE task_id = ?
            ''', (
                task.last_run, task.next_run, task.total_collections, 
                task.last_error, task.task_id
            ))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ 更新任务信息失败: {e}")
    
    def get_tasks(self) -> List[Dict]:
        """获取所有任务列表"""
        return [task.to_dict() for task in self.tasks.values()]
    
    def get_task_snapshots(self, task_id: str, limit: int = 100) -> List[Dict]:
        """获取任务的快照数据"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT snapshot_time, holder_address, balance, percentage, rank_position, value_usd
                FROM holder_snapshots 
                WHERE task_id = ?
                ORDER BY snapshot_time DESC, rank_position ASC
                LIMIT ?
            ''', (task_id, limit))
            
            rows = cursor.fetchall()
            columns = ['snapshot_time', 'holder_address', 'balance', 'percentage', 'rank_position', 'value_usd']
            
            result = []
            for row in rows:
                result.append(dict(zip(columns, row)))
            
            conn.close()
            return result
            
        except Exception as e:
            logger.error(f"❌ 获取快照数据失败: {e}")
            return []
    
    def export_task_data(self, task_id: str, output_path: str = None) -> str:
        """导出任务数据为CSV"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # 获取所有快照数据
            df = pd.read_sql_query('''
                SELECT task_id, snapshot_time, holder_address, token_address, 
                       balance, percentage, rank_position, value_usd
                FROM holder_snapshots 
                WHERE task_id = ?
                ORDER BY snapshot_time DESC, rank_position ASC
            ''', conn, params=(task_id,))
            
            conn.close()
            
            if output_path is None:
                output_path = f"holder_data_{task_id}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"📁 数据已导出: {output_path}")
            return output_path
            
        except Exception as e:
            logger.error(f"❌ 导出数据失败: {e}")
            return None
    
    def start_scheduler(self):
        """启动定时调度器"""
        if self.is_running:
            logger.warning("⚠️ 调度器已在运行")
            return
        
        self.is_running = True
        
        def run_scheduler():
            logger.info("🔄 定时调度器启动")
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
            logger.info("⏹️ 定时调度器停止")
        
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        logger.info("✅ 定时调度器已启动")
    
    def stop_scheduler(self):
        """停止定时调度器"""
        self.is_running = False
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        logger.info("⏹️ 定时调度器已停止")


# 以下保留原有的数据获取函数
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
        
        # 检查参数中是否包含时间戳，并格式化显示
        if params and ('timestamp' in params or 't' in params):
            ts = params.get('timestamp', params.get('t'))
            if ts:
                dt_str = datetime.datetime.fromtimestamp(int(ts)/1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"⏰ 请求时间点: {dt_str}")
        
        # 构建完整URL(便于调试)
        from urllib.parse import urlencode
        full_url = f"{url}?{urlencode(params or {})}"
        print(f"🔗 完整URL: {full_url}")
        
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
    
    # 添加时间戳 - 尝试不同的参数名称
    if timestamp:
        # OKX API可能使用不同的参数名称获取历史数据
        # 尝试不同的参数组合
        params["timestamp"] = int(timestamp)  # 常见参数名
        params["snapshotTime"] = int(timestamp)  # 另一种可能的参数名
        params["historyTimestamp"] = int(timestamp)  # 另一种可能的参数名
        params["time"] = int(timestamp)  # 简化的参数名
        params["t"] = int(timestamp)  # 原始参数，保留兼容性
        
        # 增加日志以便调试
        dt_str = datetime.datetime.fromtimestamp(timestamp/1000).strftime('%Y-%m-%d %H:%M:%S')
        print(f"📆 查询历史时间点: {dt_str} (timestamp: {timestamp})")
    else:
        current_time = int(time.time() * 1000)
        params["t"] = current_time
    
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
                print(f"完整响应: {json.dumps(response, indent=2)[:1000]}")
                break
            
            # 检查响应中是否包含时间戳或日期信息，用于验证API是否真的返回了历史数据
            response_time = data_obj.get('timestamp') or data_obj.get('snapshotTime') or params.get('timestamp')
            if response_time:
                resp_time_str = datetime.datetime.fromtimestamp(int(response_time)/1000).strftime('%Y-%m-%d %H:%M:%S')
                print(f"📅 API返回数据时间点: {resp_time_str}")
                
                # 检查返回的时间与请求的时间是否匹配
                requested_time = params.get('timestamp') or params.get('t')
                if requested_time and abs(int(response_time) - int(requested_time)) > 86400000:  # 24小时毫秒数
                    print(f"⚠️ 警告: API返回的时间与请求的时间相差超过24小时!")
                    print(f"📊 请求时间: {datetime.datetime.fromtimestamp(int(requested_time)/1000).strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"📊 返回时间: {resp_time_str}")
            else:
                print("⚠️ 警告: API响应中没有时间戳信息，无法验证是否返回了历史数据")
            
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



# 全局采集器实例
_global_collector = None

def get_collector() -> HolderDataCollector:
    """获取全局采集器实例"""
    global _global_collector
    if _global_collector is None:
        _global_collector = HolderDataCollector()
    return _global_collector

def create_collection_task(task_id: str, token_address: str, token_symbol: str, 
                          chain: str, interval_hours: int, max_records: int = 1000,
                          description: str = "") -> bool:
    """创建新的采集任务"""
    collector = get_collector()
    
    task = HolderCollectionTask(
        task_id=task_id,
        token_address=token_address,
        token_symbol=token_symbol,
        chain=chain,
        interval_hours=interval_hours,
        max_records=max_records,
        description=description
    )
    
    return collector.add_task(task)

def start_collection_service():
    """启动采集服务"""
    collector = get_collector()
    collector.start_scheduler()
    logger.info("🚀 Holder数据采集服务已启动")

def stop_collection_service():
    """停止采集服务"""
    global _global_collector
    if _global_collector:
        _global_collector.stop_scheduler()
    logger.info("⏹️ Holder数据采集服务已停止")

def list_collection_tasks() -> List[Dict]:
    """列出所有采集任务"""
    collector = get_collector()
    return collector.get_tasks()

def get_task_data(task_id: str, limit: int = 100) -> List[Dict]:
    """获取任务的采集数据"""
    collector = get_collector()
    return collector.get_task_snapshots(task_id, limit)

def export_task_data_csv(task_id: str, output_path: str = None) -> str:
    """导出任务数据为CSV"""
    collector = get_collector()
    return collector.export_task_data(task_id, output_path)

def get_all_tasks_summary() -> List[Dict]:
    """获取所有任务的数据概览"""
    try:
        collector = get_collector()
        tasks = list_collection_tasks()
        
        tasks_summary = []
        for task in tasks:
            task_id = task['task_id']
            
            # 获取任务基本信息
            summary = {
                'task_id': task_id,
                'token_symbol': task.get('token_symbol', ''),
                'status': task.get('status', ''),
                'interval_hours': task.get('interval_hours', 0),
                'total_collections': task.get('total_collections', 0),
                'latest_collection': task.get('latest_collection', ''),
                'description': task.get('description', ''),
                'recent_data_count': 0,
                'has_data': False
            }
            
            # 获取最近数据量
            try:
                recent_data = get_task_data(task_id, limit=50)
                summary['recent_data_count'] = len(recent_data)
                summary['has_data'] = len(recent_data) > 0
            except:
                pass
            
            tasks_summary.append(summary)
        
        return tasks_summary
    
    except Exception as e:
        logger.error(f"获取任务概览失败: {e}")
        return []

def pause_collection_task(task_id: str) -> bool:
    """暂停采集任务"""
    collector = get_collector()
    return collector.pause_task(task_id)

def resume_collection_task(task_id: str) -> bool:
    """恢复采集任务"""
    collector = get_collector()
    return collector.resume_task(task_id)

def remove_collection_task(task_id: str) -> bool:
    """删除采集任务"""
    collector = get_collector()
    return collector.remove_task(task_id)

def run_task_now(task_id: str):
    """立即执行一次采集任务"""
    collector = get_collector()
    collector.run_collection(task_id)

def analyze_holder_patterns(task_id: str, top_n: int = 100, min_snapshots: int = 3) -> Dict:
    """
    分析定时采集的持仓数据，识别早期入局且长期持仓的地址（疑似庄家）
    
    Args:
        task_id: 采集任务ID
        top_n: 分析前N名持仓者
        min_snapshots: 最少出现在几个快照中才算持续持仓
    
    Returns:
        Dict: 分析结果
    """
    try:
        # 获取任务的所有快照数据
        snapshots = get_task_data(task_id, limit=1000)
        
        if not snapshots:
            raise Exception(f"任务 {task_id} 暂无采集数据")
        
        # 按时间分组快照
        snapshots_by_time = {}
        for record in snapshots:
            snapshot_time = record['snapshot_time']
            if snapshot_time not in snapshots_by_time:
                snapshots_by_time[snapshot_time] = []
            snapshots_by_time[snapshot_time].append(record)
        
        if len(snapshots_by_time) < 2:
            raise Exception(f"快照数量不足，需要至少2个时间点的数据，当前只有 {len(snapshots_by_time)} 个")
        
        # 分析每个快照的top_n持仓者
        time_points = sorted(snapshots_by_time.keys())
        holder_analysis = {}
        
        # 统计每个地址在不同快照中的表现
        address_snapshots = {}  # {address: {time: {rank, percentage, balance}}}
        
        for snapshot_time in time_points:
            snapshot_holders = snapshots_by_time[snapshot_time]
            
            # 按持仓比例排序，取top_n
            snapshot_holders.sort(key=lambda x: float(x.get('percentage', 0)), reverse=True)
            top_holders = snapshot_holders[:top_n]
            
            for rank, holder in enumerate(top_holders, 1):
                address = holder['holder_address']
                
                if address not in address_snapshots:
                    address_snapshots[address] = {}
                
                address_snapshots[address][snapshot_time] = {
                    'rank': rank,
                    'percentage': float(holder.get('percentage', 0)),
                    'balance': holder.get('balance', '0'),
                    'value_usd': float(holder.get('value_usd', 0))
                }
        
        # 分析不同类型的地址
        persistent_whales = []      # 早期入局且长期持仓（疑似庄家）
        frequent_traders = []       # 频繁进出的地址（搬砖党）
        new_entrants = []          # 新入场的大户
        disappeared_holders = []    # 已经退出的地址
        
        for address, time_data in address_snapshots.items():
            snapshot_count = len(time_data)
            time_list = sorted(time_data.keys())
            
            # 计算持仓稳定性
            if snapshot_count >= min_snapshots:
                # 检查是否早期就在榜上
                earliest_time = time_list[0]
                latest_time = time_list[-1]
                
                earliest_rank = time_data[earliest_time]['rank']
                latest_rank = time_data[latest_time]['rank']
                
                # 计算平均排名和排名波动
                ranks = [data['rank'] for data in time_data.values()]
                avg_rank = sum(ranks) / len(ranks)
                rank_volatility = max(ranks) - min(ranks)
                
                # 计算持仓比例变化
                percentages = [data['percentage'] for data in time_data.values()]
                avg_percentage = sum(percentages) / len(percentages)
                percentage_change = percentages[-1] - percentages[0] if len(percentages) > 1 else 0
                
                # 判断地址类型
                analysis_data = {
                    'address': address,
                    'snapshot_count': snapshot_count,
                    'first_seen': earliest_time,
                    'last_seen': latest_time,
                    'earliest_rank': earliest_rank,
                    'latest_rank': latest_rank,
                    'avg_rank': round(avg_rank, 1),
                    'rank_volatility': rank_volatility,
                    'avg_percentage': round(avg_percentage, 4),
                    'percentage_change': round(percentage_change, 4),
                    'latest_percentage': percentages[-1],
                    'latest_balance': time_data[latest_time]['balance'],
                    'latest_value_usd': time_data[latest_time]['value_usd'],
                    'stability_score': round((snapshot_count / len(time_points)) * (1 - rank_volatility / 100), 3)
                }
                
                # 早期入局且长期持仓（疑似庄家特征）
                if (earliest_rank <= 20 and  # 早期就在前20
                    rank_volatility <= 30 and  # 排名变化不大
                    percentage_change >= -0.5):  # 持仓比例没有大幅减少
                    analysis_data['whale_type'] = '疑似庄家'
                    analysis_data['confidence'] = 'high' if rank_volatility <= 15 else 'medium'
                    persistent_whales.append(analysis_data)
                
                # 频繁进出（搬砖党特征）
                elif rank_volatility > 50:  # 排名波动很大
                    analysis_data['whale_type'] = '搬砖地址'
                    analysis_data['confidence'] = 'high' if rank_volatility > 80 else 'medium'
                    frequent_traders.append(analysis_data)
                
                # 新入场大户
                elif earliest_time in time_list[-3:] and latest_rank <= 50:  # 最近才出现但排名靠前
                    analysis_data['whale_type'] = '新入场大户'
                    analysis_data['confidence'] = 'high' if latest_rank <= 20 else 'medium'
                    new_entrants.append(analysis_data)
            
            # 已消失的地址（在早期快照中出现但最近消失）
            elif snapshot_count >= 2 and time_list[-1] not in time_list[-2:]:
                latest_data = time_data[time_list[-1]]
                disappeared_holders.append({
                    'address': address,
                    'last_seen': time_list[-1],
                    'last_rank': latest_data['rank'],
                    'last_percentage': latest_data['percentage'],
                    'snapshot_count': snapshot_count
                })
        
        # 按相关指标排序
        persistent_whales.sort(key=lambda x: (x['stability_score'], -x['avg_rank']), reverse=True)
        frequent_traders.sort(key=lambda x: x['rank_volatility'], reverse=True)
        new_entrants.sort(key=lambda x: x['latest_rank'])
        disappeared_holders.sort(key=lambda x: x['last_rank'])
        
        # 生成分析报告
        analysis_result = {
            'task_id': task_id,
            'total_snapshots': len(time_points),
            'time_range': {
                'start': time_points[0],
                'end': time_points[-1]
            },
            'total_addresses_analyzed': len(address_snapshots),
            'persistent_whales': persistent_whales[:20],  # 前20个疑似庄家
            'frequent_traders': frequent_traders[:15],    # 前15个搬砖地址  
            'new_entrants': new_entrants[:10],           # 前10个新入场大户
            'disappeared_holders': disappeared_holders[:10],  # 前10个消失地址
            'summary': {
                'suspected_whales_count': len(persistent_whales),
                'active_traders_count': len(frequent_traders),
                'new_big_holders_count': len(new_entrants),
                'disappeared_count': len(disappeared_holders)
            }
        }
        
        logger.info(f"✅ 持仓模式分析完成: {task_id}")
        logger.info(f"   疑似庄家: {len(persistent_whales)}个")
        logger.info(f"   搬砖地址: {len(frequent_traders)}个") 
        logger.info(f"   新入场大户: {len(new_entrants)}个")
        
        return analysis_result
        
    except Exception as e:
        logger.error(f"❌ 持仓模式分析失败: {e}")
        raise

# 示例用法
def example_usage():
    """示例用法"""
    print("\n" + "="*60)
    print("📖 Holder数据自动采集系统使用示例")
    print("="*60)
    
    # 1. 创建采集任务
    print("\n1. 创建采集任务...")
    success = create_collection_task(
        task_id="BONK_holders",
        token_address="6FtbGaqgZzti1TxJksBV4PSya5of9VqA9vJNDxPwbonk",
        token_symbol="BONK",
        chain="501",  # Solana
        interval_hours=4,  # 每4小时采集一次
        max_records=100,
        description="BONK代币持仓者数据采集"
    )
    print(f"任务创建结果: {'✅ 成功' if success else '❌ 失败'}")
    
    # 2. 启动采集服务
    print("\n2. 启动采集服务...")
    start_collection_service()
    
    # 3. 列出所有任务
    print("\n3. 当前采集任务:")
    tasks = list_collection_tasks()
    for i, task in enumerate(tasks, 1):
        print(f"  {i}. {task['task_id']} - {task['token_symbol']} ({task['status']})")
        print(f"     间隔: {task['interval_hours']}小时, 已采集: {task['total_collections']}次")
    
    # 4. 立即执行一次采集
    print("\n4. 立即执行一次采集...")
    if tasks:
        task_id = tasks[0]['task_id']
        run_task_now(task_id)
        
        # 查看采集数据
        print("\n5. 查看采集数据...")
        data = get_task_data(task_id, limit=10)
        print(f"获取到 {len(data)} 条数据")
        
        # 导出数据
        print("\n6. 导出数据...")
        csv_path = export_task_data_csv(task_id)
        print(f"数据已导出到: {csv_path}")
    
    print("\n✅ 示例完成！")
    print("💡 提示: 采集服务将在后台持续运行，按设定间隔自动采集数据")


if __name__ == "__main__":
    print("🚀 Holder数据自动采集系统")
    
    user_choice = input("\n请选择操作:\n1. 运行示例\n2. 原有测试\n请输入选择 (1/2): ")
    
    if user_choice == "1":
        example_usage()
    else:
        # 原有的测试代码
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
            
            # 测试多时间点快照功能
            user_input = input("\n是否测试多时间点快照功能? (y/n): ")
            if user_input.lower() in ('y', 'yes'):
                test_historical_snapshots()
            
        else:
            print("❌ 测试失败，未获取到数据")
            print("\n🔍 请检查:")
            print("  1. 网络连接是否正常")
            print("  2. 代币地址是否正确")
            print("  3. 是否被API限制访问")