#!/usr/bin/env python3
"""
数据库管理工具
提供常用的数据库操作命令
"""

import sys
import os
import argparse
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.database import get_db_config, get_db_session
from services.database_service import TopTraderService, WalletTagService
from sqlalchemy import text
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_connection():
    """测试数据库连接"""
    try:
        db_config = get_db_config()
        if db_config.test_connection():
            logger.info("✅ 数据库连接正常")
            return True
        else:
            logger.error("❌ 数据库连接失败")
            return False
    except Exception as e:
        logger.error(f"❌ 连接测试异常: {e}")
        return False

def show_tables():
    """显示所有表"""
    try:
        with get_db_session() as session:
            result = session.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """))
            
            tables = result.fetchall()
            logger.info("📊 数据库表列表:")
            for table in tables:
                logger.info(f"   - {table[0]}")
            
            return True
    except Exception as e:
        logger.error(f"❌ 获取表列表失败: {e}")
        return False

def show_table_info(table_name):
    """显示表信息"""
    try:
        with get_db_session() as session:
            # 获取表结构
            result = session.execute(text(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """))
            
            columns = result.fetchall()
            logger.info(f"📋 表 '{table_name}' 结构:")
            for col in columns:
                logger.info(f"   - {col[0]}: {col[1]} {'NULL' if col[2] == 'YES' else 'NOT NULL'}")
            
            # 获取记录数
            result = session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            logger.info(f"📊 记录数: {count}")
            
            return True
    except Exception as e:
        logger.error(f"❌ 获取表信息失败: {e}")
        return False

def clear_table(table_name):
    """清空表数据"""
    try:
        with get_db_session() as session:
            session.execute(text(f"DELETE FROM {table_name}"))
            session.commit()
            logger.info(f"✅ 表 '{table_name}' 已清空")
            return True
    except Exception as e:
        logger.error(f"❌ 清空表失败: {e}")
        return False

def export_table(table_name, output_file):
    """导出表数据到CSV"""
    try:
        with get_db_session() as session:
            df = pd.read_sql(f"SELECT * FROM {table_name}", session.connection())
            df.to_csv(output_file, index=False)
            logger.info(f"✅ 表 '{table_name}' 已导出到 {output_file}")
            logger.info(f"📊 导出记录数: {len(df)}")
            return True
    except Exception as e:
        logger.error(f"❌ 导出表失败: {e}")
        return False

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='数据库管理工具')
    parser.add_argument('command', choices=[
        'test', 'tables', 'info', 'clear', 'export'
    ], help='执行的命令')
    parser.add_argument('--table', help='表名')
    parser.add_argument('--output', help='输出文件路径')
    
    args = parser.parse_args()
    
    if args.command == 'test':
        return test_connection()
    
    elif args.command == 'tables':
        return show_tables()
    
    elif args.command == 'info':
        if not args.table:
            logger.error("❌ 请指定表名: --table <table_name>")
            return False
        return show_table_info(args.table)
    
    elif args.command == 'clear':
        if not args.table:
            logger.error("❌ 请指定表名: --table <table_name>")
            return False
        
        # 确认操作
        confirm = input(f"确定要清空表 '{args.table}' 吗？(y/N): ")
        if confirm.lower() != 'y':
            logger.info("操作已取消")
            return True
        
        return clear_table(args.table)
    
    elif args.command == 'export':
        if not args.table:
            logger.error("❌ 请指定表名: --table <table_name>")
            return False
        if not args.output:
            logger.error("❌ 请指定输出文件: --output <file_path>")
            return False
        
        return export_table(args.table, args.output)
    
    return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
