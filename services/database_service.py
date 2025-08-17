"""
数据库操作服务
提供数据的增删改查功能，针对Render免费版PostgreSQL优化
"""

from sqlalchemy.orm import Session
from sqlalchemy import desc, and_, or_
from models.database_models import TopTrader, TokenHolder, WalletTag, TransactionHistory, AnalysisJob
from config.database import get_db_session, get_db_engine, get_db
import pandas as pd
import json
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TopTraderService:
    """TOP交易者数据服务 - Render优化版"""
    
    @staticmethod
    def save_traders(traders_data, token_address, chain_id):
        """保存交易者数据到数据库 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器，自动释放连接
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                # 先删除相同token的旧数据
                db.query(TopTrader).filter(
                    and_(
                        TopTrader.token_address == token_address,
                        TopTrader.chain_id == chain_id
                    )
                ).delete()
                
                # 添加新数据
                saved_count = 0
                for trader_data in traders_data:
                    trader = TopTrader(
                        wallet_address=trader_data.get('walletAddress', ''),
                        token_address=token_address,
                        chain_id=chain_id,
                        total_pnl=float(trader_data.get('totalPnl', 0)),
                        total_pnl_percentage=float(trader_data.get('totalProfitPercentage', 0)),
                        realized_profit=float(trader_data.get('realizedProfit', 0)),
                        realized_profit_percentage=float(trader_data.get('realizedProfitPercentage', 0)),
                        roi=float(trader_data.get('roi', 0)),
                        buy_count=int(trader_data.get('buyCount', 0)),
                        sell_count=int(trader_data.get('sellCount', 0)),
                        total_count=int(trader_data.get('totalCount', 0)),
                        win_rate=float(trader_data.get('winRate', 0)),
                        buy_value=float(trader_data.get('buyValue', 0)),
                        sell_value=float(trader_data.get('sellValue', 0)),
                        hold_amount=float(trader_data.get('holdAmount', 0)),
                        bought_avg_price=float(trader_data.get('boughtAvgPrice', 0)),
                        sold_avg_price=float(trader_data.get('soldAvgPrice', 0)),
                        tags=trader_data.get('tags', ''),
                        remark=trader_data.get('remark', '')
                    )
                    db.add(trader)
                    saved_count += 1
                
                db.commit()
                logger.info(f"✅ 成功保存 {saved_count} 个交易者数据")
                return saved_count
                
            except Exception as e:
                db.rollback()
                raise e
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 保存交易者数据失败: {e}")
            raise
    
    @staticmethod
    def get_traders(token_address, chain_id, limit=100):
        """从数据库获取交易者数据 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                traders = db.query(TopTrader).filter(
                    and_(
                        TopTrader.token_address == token_address,
                        TopTrader.chain_id == chain_id
                    )
                ).order_by(desc(TopTrader.total_pnl)).limit(limit).all()
                
                result = []
                for trader in traders:
                    result.append({
                        'walletAddress': trader.wallet_address,
                        'totalPnl': trader.total_pnl,
                        'totalProfitPercentage': trader.total_pnl_percentage,
                        'realizedProfit': trader.realized_profit,
                        'realizedProfitPercentage': trader.realized_profit_percentage,
                        'roi': trader.roi,
                        'buyCount': trader.buy_count,
                        'sellCount': trader.sell_count,
                        'totalCount': trader.total_count,
                        'winRate': trader.win_rate,
                        'buyValue': trader.buy_value,
                        'sellValue': trader.sell_value,
                        'holdAmount': trader.hold_amount,
                        'boughtAvgPrice': trader.bought_avg_price,
                        'soldAvgPrice': trader.sold_avg_price,
                        'tags': trader.tags,
                        'remark': trader.remark,
                        'updatedAt': trader.updated_at.isoformat() if trader.updated_at else None
                    })
                
                logger.info(f"✅ 从数据库获取到 {len(result)} 个交易者数据")
                return result
                
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 获取交易者数据失败: {e}")
            return []
    
    @staticmethod
    def is_data_fresh(token_address, chain_id, max_age_hours=1):
        """检查数据是否新鲜（1小时内） - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)
                
                count = db.query(TopTrader).filter(
                    and_(
                        TopTrader.token_address == token_address,
                        TopTrader.chain_id == chain_id,
                        TopTrader.updated_at >= cutoff_time
                    )
                ).count()
                
                return count > 0
                
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 检查数据新鲜度失败: {e}")
            return False

class WalletTagService:
    """钱包标签服务 - Render优化版"""
    
    @staticmethod
    def save_wallet_tag(wallet_address, tags, remark, category=None):
        """保存钱包标签 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                # 查找现有记录
                wallet_tag = db.query(WalletTag).filter(
                    WalletTag.wallet_address == wallet_address
                ).first()
                
                if wallet_tag:
                    # 更新现有记录
                    wallet_tag.tags = tags
                    wallet_tag.remark = remark
                    if category:
                        wallet_tag.category = category
                    wallet_tag.updated_at = datetime.utcnow()
                else:
                    # 创建新记录
                    wallet_tag = WalletTag(
                        wallet_address=wallet_address,
                        tags=tags,
                        remark=remark,
                        category=category or 'general'
                    )
                    db.add(wallet_tag)
                
                db.commit()
                logger.info(f"✅ 成功保存钱包标签: {wallet_address[:8]}...")
                return True
                
            except Exception as e:
                db.rollback()
                raise e
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 保存钱包标签失败: {e}")
            return False
    
    @staticmethod
    def get_wallet_tag(wallet_address):
        """获取钱包标签 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                wallet_tag = db.query(WalletTag).filter(
                    WalletTag.wallet_address == wallet_address
                ).first()
                
                if wallet_tag:
                    return {
                        'address': wallet_tag.wallet_address,
                        'tags': wallet_tag.tags,
                        'remark': wallet_tag.remark,
                        'category': wallet_tag.category,
                        'is_smart_money': wallet_tag.is_smart_money,
                        'is_whale': wallet_tag.is_whale,
                        'is_institution': wallet_tag.is_institution,
                        'risk_level': wallet_tag.risk_level,
                        'updated_at': wallet_tag.updated_at.isoformat() if wallet_tag.updated_at else None
                    }
                return None
                
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 获取钱包标签失败: {e}")
            return None
    
    @staticmethod
    def batch_get_wallet_tags(wallet_addresses):
        """批量获取钱包标签 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                wallet_tags = db.query(WalletTag).filter(
                    WalletTag.wallet_address.in_(wallet_addresses)
                ).all()
                
                result = {}
                for tag in wallet_tags:
                    result[tag.wallet_address] = {
                        'tags': tag.tags,
                        'remark': tag.remark,
                        'category': tag.category,
                        'is_smart_money': tag.is_smart_money,
                        'is_whale': tag.is_whale,
                        'is_institution': tag.is_institution,
                        'risk_level': tag.risk_level
                    }
                
                return result
                
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 批量获取钱包标签失败: {e}")
            return {}

class AnalysisJobService:
    """分析任务服务 - Render优化版"""
    
    @staticmethod
    def create_job(job_type, job_params):
        """创建分析任务 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                job = AnalysisJob(
                    job_type=job_type,
                    job_params=json.dumps(job_params),
                    status='pending'
                )
                db.add(job)
                db.commit()
                
                logger.info(f"✅ 创建分析任务: {job_type}, ID: {job.id}")
                return job.id
                
            except Exception as e:
                db.rollback()
                raise e
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 创建分析任务失败: {e}")
            return None
    
    @staticmethod
    def update_job_status(job_id, status, progress=None, result_count=None, error_message=None):
        """更新任务状态 - 使用优化的会话管理"""
        try:
            # 🔧 使用新的会话管理器
            db_gen = get_db()
            db = next(db_gen)
            
            try:
                job = db.query(AnalysisJob).filter(AnalysisJob.id == job_id).first()
                if job:
                    job.status = status
                    if progress is not None:
                        job.progress = progress
                    if result_count is not None:
                        job.result_count = result_count
                    if error_message is not None:
                        job.error_message = error_message
                    if status == 'completed':
                        job.completed_at = datetime.utcnow()
                    
                    db.commit()
                    return True
                return False
                
            except Exception as e:
                db.rollback()
                raise e
            finally:
                # 🔧 确保会话被正确关闭
                try:
                    next(db_gen)
                except StopIteration:
                    pass
                
        except Exception as e:
            logger.error(f"❌ 更新任务状态失败: {e}")
            return False

# 导出DataFrame到数据库的通用函数
def save_dataframe_to_db(df, table_name, if_exists='replace'):
    """将DataFrame保存到数据库 - 使用优化的连接管理"""
    try:
        engine = get_db_engine()
        df.to_sql(table_name, engine, if_exists=if_exists, index=False, method='multi')
        logger.info(f"✅ 成功保存DataFrame到表 {table_name}, 记录数: {len(df)}")
        return True
    except Exception as e:
        logger.error(f"❌ 保存DataFrame失败: {e}")
        return False

def load_dataframe_from_db(query):
    """从数据库加载DataFrame - 使用优化的连接管理"""
    try:
        engine = get_db_engine()
        df = pd.read_sql(query, engine)
        logger.info(f"✅ 从数据库加载DataFrame, 记录数: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"❌ 加载DataFrame失败: {e}")
        return pd.DataFrame()

# 🔧 新增：连接池监控函数
def get_connection_pool_status():
    """获取连接池状态信息"""
    try:
        from config.database import get_db_config
        config = get_db_config()
        return config.get_connection_info()
    except Exception as e:
        logger.error(f"❌ 获取连接池状态失败: {e}")
        return {}

# 🔧 新增：长任务装饰器
def with_long_running_session(func):
    """长任务装饰器，使用scoped_session避免连接抢占"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            # 如果是数据库连接相关错误，清理连接池
            if "connection" in str(e).lower():
                from config.database import cleanup_db_connections
                cleanup_db_connections()
            raise e
    return wrapper
