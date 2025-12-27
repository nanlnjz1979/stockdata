#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
调试指数成分股获取功能
"""

import logging
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def debug_index_stock_cons():
    """
    调试指数成分股获取功能
    """
    try:
        # 直接导入akshare查看函数文档
        import akshare as ak
        
        logger.info("直接调用akshare的index_stock_cons函数进行调试")
        
        # 调用函数获取数据
        symbol = "000300"
        logger.info(f"调用ak.index_stock_cons(symbol='{symbol}')")
        df = ak.index_stock_cons(symbol=symbol)
        
        # 打印数据基本信息
        logger.info(f"返回数据类型: {type(df)}")
        logger.info(f"数据是否为空: {df.empty}")
        if not df.empty:
            logger.info(f"数据形状: {df.shape}")
            logger.info(f"数据列名: {df.columns.tolist()}")
            logger.info(f"数据前5行:\n{df.head()}")
        
        # 测试其他指数
        symbol = "000905"
        logger.info(f"\n调用ak.index_stock_cons(symbol='{symbol}')")
        df2 = ak.index_stock_cons(symbol=symbol)
        logger.info(f"返回数据类型: {type(df2)}")
        logger.info(f"数据是否为空: {df2.empty}")
        if not df2.empty:
            logger.info(f"数据形状: {df2.shape}")
            logger.info(f"数据列名: {df2.columns.tolist()}")
            logger.info(f"数据前5行:\n{df2.head()}")
        
        return True
        
    except Exception as e:
        logger.error(f"调试过程中发生错误：{str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    debug_index_stock_cons()
