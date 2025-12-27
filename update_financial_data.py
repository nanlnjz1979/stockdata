#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融数据更新脚本
用于更新一次金融数据，然后sleep 1秒
"""

import logging
import time
from datetime import datetime
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def update_financial_data_once():
    """
    执行一次金融数据更新
    """
    try:
        logger.info("开始执行金融数据更新")
        
        # 添加项目根目录到Python路径
        import sys
        sys.path.append(os.path.dirname(os.path.abspath(__file__)))
        
        # 导入必要的模块
        from backend.global_config.stock_info import StockInfo
        from backend.global_config.data_fetch import AkshareFetcher
        
        # 获取所有股票信息
        basics = StockInfo.get_all_stocks()
        stock_codes = [stock.get('code', '') for stock in basics if stock.get('code')]
        
        logger.info(f"共获取到{len(stock_codes)}只股票")
        
        # 只处理一只股票作为示例
        if stock_codes:
            code = stock_codes[0]
            logger.info(f"开始处理股票: {code}")
            
            # 创建fetcher实例
            fetcher = AkshareFetcher()
            
            # 创建保存目录
            financial_data_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'backend', 'data', 'financial_data'
            )
            os.makedirs(financial_data_dir, exist_ok=True)
            
            # 获取并保存金融数据
            df = fetcher.fetch_stock_financial_data(code)
            
            if not df.empty:
                csv_file_path = os.path.join(financial_data_dir, f"{code}.csv")
                df.to_csv(csv_file_path, index=False, encoding='utf-8')
                logger.info(f"成功保存股票{code}的金融数据到{csv_file_path}，共{len(df)}条记录")
                return True
            else:
                logger.warning(f"未获取到股票{code}的有效金融数据")
                return False
        else:
            logger.warning("没有可处理的股票代码")
            return False
            
    except Exception as e:
        logger.error(f"执行金融数据更新时发生错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # 执行一次金融数据更新
    success = update_financial_data_once()
    
    # sleep 1秒
    logger.info("金融数据更新完成，开始sleep 1秒")
    time.sleep(1)
    logger.info("sleep 1秒结束")
    
    if success:
        logger.info("金融数据更新成功")
    else:
        logger.info("金融数据更新失败")
