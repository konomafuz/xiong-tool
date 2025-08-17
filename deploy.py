#!/usr/bin/env python3
"""
生产部署脚本 - Render优化版
只处理必要的初始化，跳过依赖安装
"""

import os
import sys
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_environment():
    """设置环境变量"""
    try:
        logger.info("🔧 设置环境变量...")
        
        # 加载.env文件
        from dotenv import load_dotenv
        load_dotenv()
        
        # 检查关键环境变量
        db_url = os.getenv('DATABASE_URL')
        if db_url:
            logger.info("✅ DATABASE_URL已设置")
        else:
            logger.warning("⚠️  DATABASE_URL未设置，使用默认值")
        
        # 设置编码环境变量
        os.environ['PYTHONIOENCODING'] = 'utf-8'
        os.environ['LANG'] = 'C.UTF-8'
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 环境设置失败: {e}")
        return False

def validate_project_structure():
    """验证项目结构"""
    try:
        logger.info("📂 验证项目结构...")
        
        required_files = [
            'app.py',
            'config/database.py', 
            'models/database_models.py',
            'services/database_service.py',
            'requirements.txt'
        ]
        
        missing_files = []
        for file_path in required_files:
            if os.path.exists(file_path):
                logger.info(f"✅ {file_path}")
            else:
                missing_files.append(file_path)
                logger.error(f"❌ 缺少文件: {file_path}")
        
        if missing_files:
            logger.error(f"❌ 缺少 {len(missing_files)} 个必要文件")
            return False
        
        logger.info("✅ 项目结构验证通过")
        return True
        
    except Exception as e:
        logger.error(f"❌ 项目结构验证失败: {e}")
        return False

def test_imports():
    """测试关键模块导入"""
    try:
        logger.info("📦 测试模块导入...")
        
        # 测试Flask
        import flask
        logger.info(f"✅ Flask {flask.__version__}")
        
        # 测试数据库相关
        import sqlalchemy
        logger.info(f"✅ SQLAlchemy {sqlalchemy.__version__}")
        
        import psycopg2
        logger.info(f"✅ psycopg2 {psycopg2.__version__}")
        
        # 测试项目模块
        from config.database import DatabaseConfig
        logger.info("✅ 数据库配置模块")
        
        from models.database_models import TopTrader
        logger.info("✅ 数据库模型")
        
        from services.database_service import TopTraderService
        logger.info("✅ 数据库服务")
        
        logger.info("✅ 所有模块导入成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 模块导入失败: {e}")
        return False

def init_database_tables():
    """初始化数据库表（仅在连接可用时）"""
    try:
        logger.info("🗄️  尝试初始化数据库表...")
        
        from config.database import DatabaseConfig, Base
        
        # 创建配置实例
        config = DatabaseConfig()
        
        # 尝试连接（允许失败）
        try:
            if config.test_connection():
                logger.info("✅ 数据库连接成功")
                
                # 创建表
                Base.metadata.create_all(bind=config.get_engine())
                logger.info("✅ 数据库表创建/更新成功")
                return True
            else:
                logger.warning("⚠️  数据库连接失败，跳过表创建")
                return True  # 允许跳过
                
        except Exception as e:
            logger.warning(f"⚠️  数据库操作失败: {e}")
            logger.info("💡 这在本地环境是正常的，生产环境会自动处理")
            return True  # 允许跳过
        
    except Exception as e:
        logger.error(f"❌ 数据库初始化失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 开始生产部署初始化...")
    
    # 步骤1: 设置环境
    if not setup_environment():
        logger.error("❌ 环境设置失败")
        return False
    
    # 步骤2: 验证项目结构
    if not validate_project_structure():
        logger.error("❌ 项目结构验证失败")
        return False
    
    # 步骤3: 测试模块导入
    if not test_imports():
        logger.error("❌ 模块导入测试失败")
        return False
    
    # 步骤4: 初始化数据库（可选）
    init_database_tables()  # 允许失败
    
    logger.info("🎉 生产部署初始化完成！")
    logger.info("💡 如果数据库连接失败，请在生产环境中重新运行")
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("✅ 部署准备完成，可以启动应用")
    else:
        logger.error("❌ 部署准备失败")
    sys.exit(0 if success else 1)
