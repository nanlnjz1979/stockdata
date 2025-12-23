import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from backend.global_config.stock_info import StockInfo


def test_save_stocks():
    """
    测试保存股票数据到数据库
    """
    try:
        # 获取所有股票数据
        print("正在获取所有股票数据...")
        all_stocks = StockInfo.get_all_stocks()
        print(f"成功获取{len(all_stocks)}条股票数据")
        
        if not all_stocks:
            print("没有获取到股票数据，测试结束")
            return
        
        # 保存股票数据到数据库
        print("正在保存股票数据到数据库...")
        success = StockInfo.save_stocks_to_db(all_stocks)
        
        if success:
            print("股票数据保存成功！")
        else:
            print("股票数据保存失败！")
            
    except Exception as e:
        print(f"测试过程中发生错误: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    test_save_stocks()
