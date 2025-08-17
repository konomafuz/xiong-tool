#!/usr/bin/env python3
"""
数据库初始化脚本
用于创建数据库表和初始数据
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.database import init_database, get_db_config
from models.database_models import Base
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """主函数"""
    try:
        logger.info("🚀 开始初始化数据库...")
        
        # 初始化数据库配置
        db_config = init_database()
        
        # 创建所有表
        logger.info("📋 创建数据库表...")
        Base.metadata.create_all(bind=db_config.get_engine())
        
        logger.info("✅ 数据库初始化完成！")
        logger.info("🎯 数据库连接信息:")
        logger.info(f"   - 引擎: {db_config.engine.url}")
        logger.info(f"   - 连接池大小: {db_config.pool_size}")
        logger.info(f"   - 最大溢出: {db_config.max_overflow}")
        
        # 测试连接
        logger.info("🔍 测试数据库连接...")
        if db_config.test_connection():
            logger.info("✅ 数据库连接测试成功！")
        else:
            logger.error("❌ 数据库连接测试失败！")
            return False
        
        # 显示表信息
        from sqlalchemy import inspect
        inspector = inspect(db_config.get_engine())
        tables = inspector.get_table_names()
        
        logger.info("📊 已创建的数据表:")
        for table in tables:
            logger.info(f"   - {table}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        import traceback
        logger.error(f"📝 详细错误: {traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
