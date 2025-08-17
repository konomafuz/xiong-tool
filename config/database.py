"""
数据库配置和连接管理
支持PostgreSQL数据库连接，针对Render免费版优化
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import logging
from urllib.parse import quote_plus

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy基类
Base = declarative_base()

class DatabaseConfig:
    """数据库配置类 - 针对Render免费版PostgreSQL优化"""
    
    def __init__(self):
        # Render PostgreSQL 连接信息
        self.database_url = os.getenv(
            'DATABASE_URL', 
            'postgresql://xiong_tool_sql_user:jwokxZ31cxSckuksdwzrYzwN349r8ie2mDf@dpg-d2fh98be5dus73aobvd0-a/xiong_tool_sql'
        )
        
        # Render免费版连接池配置（总连接数≤15）
        self.pool_size = int(os.getenv('DB_POOL_SIZE', 3))  # 基础连接池
        self.max_overflow = int(os.getenv('DB_MAX_OVERFLOW', 5))  # 溢出连接
        self.pool_timeout = int(os.getenv('DB_POOL_TIMEOUT', 30))  # 获取连接超时
        self.pool_recycle = int(os.getenv('DB_POOL_RECYCLE', 3600))  # 1小时回收连接
        
        # 创建引擎和会话
        self.engine = None
        self.SessionLocal = None
        self._initialize_engine()
    
    def _initialize_engine(self):
        """初始化数据库引擎 - Render优化版"""
        try:
            # 🔧 修复编码问题：添加客户端编码设置
            connect_args = {
                "connect_timeout": 10,
                "application_name": "okx_pnl_tool",
                "options": "-c default_transaction_isolation=read committed",
                "client_encoding": "utf8"  # 明确设置客户端编码
            }
            
            # 创建SQLAlchemy引擎，针对Render环境优化
            self.engine = create_engine(
                self.database_url,
                pool_size=self.pool_size,
                max_overflow=self.max_overflow,
                pool_timeout=self.pool_timeout,
                pool_recycle=self.pool_recycle,
                pool_pre_ping=True,  # 🔧 避免idle连接被杀掉
                echo=False,  # 生产环境设为False
                connect_args=connect_args
            )
            
            # 🔧 使用scoped_session避免长任务抢占连接
            self.SessionLocal = scoped_session(
                sessionmaker(
                    bind=self.engine,
                    autoflush=False,
                    autocommit=False
                )
            )
            
            logger.info(f"✅ 数据库引擎初始化成功")
            logger.info(f"🔧 连接池配置: size={self.pool_size}, overflow={self.max_overflow}")
            
        except Exception as e:
            error_msg = str(e)
            # 🔧 处理编码相关的错误信息
            try:
                error_msg = error_msg.encode('utf-8', errors='replace').decode('utf-8')
            except:
                error_msg = "数据库引擎初始化错误（编码问题）"
            
            logger.error(f"❌ 数据库引擎初始化失败: {error_msg}")
            raise
    
    def test_connection(self):
        """测试数据库连接 - 修复编码问题"""
        try:
            # 🔧 修复编码问题：使用proper连接参数
            connect_args = {
                "connect_timeout": 10,
                "application_name": "okx_pnl_tool",
                "options": "-c default_transaction_isolation=read committed",
                "client_encoding": "utf8"  # 明确设置客户端编码
            }
            
            # 创建临时连接进行测试
            from sqlalchemy import create_engine
            test_engine = create_engine(
                self.database_url,
                connect_args=connect_args
            )
            
            with test_engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                row = result.fetchone()
                logger.info("✅ 数据库连接测试成功")
                return True
                
        except Exception as e:
            error_msg = str(e)
            # 🔧 处理编码相关的错误信息
            try:
                error_msg = error_msg.encode('utf-8', errors='replace').decode('utf-8')
            except:
                error_msg = "数据库连接错误（编码问题）"
            
            logger.error(f"❌ 数据库连接测试失败: {error_msg}")
            return False
    
    def get_db(self):
        """获取数据库会话的生成器（推荐用法）"""
        db = self.SessionLocal()
        try:
            yield db
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"❌ 数据库会话错误: {e}")
            raise
        finally:
            db.close()  # 🔧 及时释放连接
    
    @contextmanager
    def get_session(self):
        """获取数据库会话的上下文管理器（兼容旧代码）"""
        db = self.SessionLocal()
        try:
            yield db
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"❌ 数据库会话错误: {e}")
            raise
        finally:
            db.close()  # 🔧 及时释放连接
    
    def get_engine(self):
        """获取数据库引擎"""
        return self.engine
    
    def create_tables(self):
        """创建所有表"""
        try:
            Base.metadata.create_all(bind=self.engine)
            logger.info("✅ 数据库表创建成功")
        except Exception as e:
            logger.error(f"❌ 数据库表创建失败: {e}")
            raise
    
    def get_connection_info(self):
        """获取连接池信息"""
        try:
            pool = self.engine.pool
            return {
                'pool_size': pool.size(),
                'checked_in': pool.checkedin(),
                'checked_out': pool.checkedout(),
                'overflow': pool.overflow(),
                'invalid': pool.invalid()
            }
        except Exception as e:
            logger.error(f"❌ 获取连接池信息失败: {e}")
            return {}
    
    def cleanup_connections(self):
        """清理空闲连接（适用于长时间运行的应用）"""
        try:
            # 移除scoped_session的当前会话
            self.SessionLocal.remove()
            
            # 清理连接池中的空闲连接
            self.engine.dispose()
            
            logger.info("✅ 连接池清理完成")
        except Exception as e:
            logger.error(f"❌ 连接池清理失败: {e}")

# 全局数据库配置实例
db_config = None

def get_db_config():
    """获取数据库配置实例（单例模式）"""
    global db_config
    if db_config is None:
        db_config = DatabaseConfig()
    return db_config

def init_database():
    """初始化数据库"""
    config = get_db_config()
    
    # 测试连接
    if not config.test_connection():
        raise Exception("数据库连接失败")
    
    # 创建表
    config.create_tables()
    
    logger.info("🎯 数据库初始化完成")
    return config

# 便捷函数
def get_db_session():
    """获取数据库会话（兼容旧代码）"""
    config = get_db_config()
    return config.get_session()

def get_db():
    """获取数据库会话生成器（推荐用法）"""
    config = get_db_config()
    return config.get_db()

def get_db_engine():
    """获取数据库引擎"""
    config = get_db_config()
    return config.get_engine()

def cleanup_db_connections():
    """清理数据库连接"""
    config = get_db_config()
    if config:
        config.cleanup_connections()
