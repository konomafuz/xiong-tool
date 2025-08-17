#!/usr/bin/env python3
"""
Render免费版启动脚本
确保应用能在免费版环境下正常启动
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
        # 设置编码
        os.environ['PYTHONIOENCODING'] = 'utf-8'
        if 'LANG' not in os.environ:
            os.environ['LANG'] = 'C.UTF-8'
        
        # 检查是否在Render环境
        is_render = os.getenv('RENDER') == 'true'
        if is_render:
            logger.info("🎭 检测到Render环境")
        else:
            logger.info("💻 本地开发环境")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 环境设置失败: {e}")
        return False

def main():
    """主函数"""
    logger.info("🚀 启动OKX PnL工具...")
    
    # 设置环境
    if not setup_environment():
        sys.exit(1)
    
    try:
        # 导入并启动Flask应用
        from app import app
        
        # 获取端口（Render会提供PORT环境变量）
        port = int(os.getenv('PORT', 5000))
        
        # 启动应用
        logger.info(f"🌐 启动Flask应用，端口: {port}")
        app.run(
            host="0.0.0.0",
            port=port,
            debug=os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
        )
        
    except Exception as e:
        logger.error(f"❌ 应用启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
