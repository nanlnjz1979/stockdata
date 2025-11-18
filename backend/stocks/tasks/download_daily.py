import json
import logging
import threading
import os
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import datetime
from backend.global_config.utils import save_to_csv
from backend.global_config import norm_date
from backend.global_config.utils import _num, _int, make_symbol
from backend.global_config.data_fetch import AkshareFetcher
from backend.global_config.file_config import FileConfig
from .base import BaseTask





def _insert_daily_thread(code, df, adj, conn=None, result_dict=None):
    """在线程中执行数据插入操作并更新结果字典"""
    try:
        saved_count = _insert_daily(code, df, adj, conn)
        if result_dict is not None:
            with result_dict.get('lock', threading.Lock()):
                result_dict['total_saved'] += saved_count
                if saved_count > 0:
                    result_dict['success_codes'].append(code)
        return saved_count
    except Exception as e:
        logger.exception("线程数据保存失败: code=%s, adj=%s, error=%s", code, adj, e)
        if result_dict is not None:
            with result_dict.get('lock', threading.Lock()):
                result_dict['failed_codes'].append((code, str(e)))
        return 0

def _insert_daily(code, df, adj, conn=None):
    if df is None or getattr(df, 'empty', True):
        return 0
    conn_local = conn 
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        cols = {
            '日期': 'date',
            '开盘': 'open',
            '收盘': 'close',
            '最高': 'high',
            '最低': 'low',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover',
            '流通股本': 'outstanding_share',
        }
        df = df.rename(columns=cols)
        values = []
        for _, r in df.iterrows():
            d = r.get('date')
            if not d:
                continue
            try:
                trade_date = d if isinstance(d, datetime) else datetime.strptime(str(d), '%Y-%m-%d')
            except Exception:
                continue
            adj_norm = adj if (adj and str(adj).strip()) else None
            values.append((
                code,
                trade_date,
                adj_norm,
                _num(r.get('open')),
                _num(r.get('close')),
                _num(r.get('high')),
                _num(r.get('low')),
                _int(r.get('volume')),
                _num(r.get('amount')),
                _num(r.get('turnover')),
                _num(r.get('outstanding_share')),
            ))
        if values:
            try:
                cur.executemany(
                    """
                    insert into stock_daily (
                      code, trade_date, adjust_type, open, close, high, low, volume, amount, turnover, outstanding_share
                    ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    values
                )
            except Exception:
                return 0
        if conn is None:
            try:
                conn_local.close()
            except Exception:
                pass
        return len(values)
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return 0

# 依赖：数据源
fetcher = AkshareFetcher()

logger = logging.getLogger(__name__)


class DownloadDailyTask(BaseTask):
    """
    下载所有股票日期的数据，一般是第一次运行时使用。
    从 BaseTask 派生的任务：下载并写入股票日线数据到 QuestDB。
    params 支持：
      - code: 单个股票代码（字符串）
      - codes: 多个股票代码（列表）
      - market: 市场标识（可选，'SH'/'SZ'/'BJ'，默认空）
      - start_date: 开始日期（'YYYYMMDD'或'YYYY-MM-DD'）
      - end_date: 结束日期（'YYYYMMDD'或'YYYY-MM-DD'）
    执行逻辑：为每个股票调用 ak.stock_zh_a_hist 并写入 QuestDB。
    """

    def __init__(self, orm):
        # 仅要求传入 orm，其余字段在 generate() 时设置
        super().__init__(orm, task_type="", task_desc="", params=None, priority=0)

    def generate(self, task_type: str, task_desc: str = "", params: Optional[Dict[str, Any]] = None, priority: int = 0,conn=None) -> str:
        # 在生成前配置必要字段
        self.task_type = task_type
        self.task_desc = task_desc
        self.params_str = self._ensure_json_str(params)
        self.priority = priority
        return super().generate()

    def _parse_params(self) -> Dict[str, Any]:
        try:
            return json.loads(self.params_str or '{}')
        except Exception:
            return {}
    @classmethod
    def taskID(cls) -> str:
        return "Download_Full_Daily"
    def run(self, conn=None,params_str: str = None) -> bool:
        # 检查依赖
        if params_str:
            self.params_str = params_str
        params = self._parse_params()
        market = (params.get('market') or '').upper()
        
        adjust = (params.get('adjust') or '').lower()
        start_date = norm_date(params.get('start_date')) or '19900101'
        end_date = norm_date(params.get('end_date')) or datetime.now().strftime('%Y%m%d')

        # 收集目标代码列表
        codes: List[str] = []
        code_single = params.get('code')
        codes_multi = params.get('codes')
        if isinstance(code_single, str) and code_single.strip():
            codes.append(code_single.strip())
        if isinstance(codes_multi, list):
            for c in codes_multi:
                if isinstance(c, str) and c.strip():
                    codes.append(c.strip())
        # 去重
        codes = list(dict.fromkeys(codes))
        if not codes:
            logger.warning("未提供有效的股票代码（params 需包含 'code' 或 'codes'）")
            return False
        
        # 检查是否保存到CSV文件
        to_csv = FileConfig.get("to_csv", False)
        logger.info(f"任务配置：保存到CSV文件 = {to_csv}")
        
        # 如果不保存到CSV文件，则检查数据库连接
        if not to_csv:
            conn_local = conn 
            if not conn_local:
                logger.error("QuestDB 连接失败")
                return False
        
        try:
            # 用于存储结果
            result_dict = {
                'total_saved': 0,
                'success_codes': [],
                'failed_codes': [],
                'lock': threading.Lock()
            }
            
            if to_csv:
                # 保存到CSV：不使用线程，直接保存
                for code in codes:
                    try:
                        # 获取所有需要的复权类型
                        adjust_all = ['', 'qfq', 'hfq'] if adjust == "all" else [adjust]
                        
                        # 收集所有复权类型的数据
                        all_data = []
                        total_rows = 0
                        
                        for adj in adjust_all:
                            df = fetcher.fetch_stock_daily(code=code, start_date=start_date, end_date=end_date, adjust=adj)
                            if df is not None and not df.empty:
                                # 复制DataFrame以避免修改原始数据
                                temp_df = df.copy()
                                # 添加复权类型标识
                                temp_df['adjust_type'] = adj if (adj and str(adj).strip()) else None
                                all_data.append(temp_df)
                                total_rows += len(temp_df)
                        
                        # 合并所有复权类型的数据并一次性保存
                        if all_data:
                            # 合并所有DataFrame
                            combined_df = pd.concat(all_data, ignore_index=True)
                            # 一次性保存到CSV
                            saved_count = save_to_csv(code, combined_df, None)  # 传入None作为adj参数，因为已经在数据中包含了adjust_type
                            result_dict['total_saved'] += saved_count
                            
                            if saved_count > 0:
                                result_dict['success_codes'].append(code)
                                logger.info(f"成功保存股票 {code} 的所有复权类型数据，共 {total_rows} 行")
                    except Exception as e:
                        logger.exception("数据获取或保存失败: code=%s, error=%s", code, e)
                        result_dict['failed_codes'].append((code, str(e)))
            else:
                # 保存到数据库：继续使用线程
                threads = []
                for code in codes:
                    adjust_all = ['', 'qfq', 'hfq'] if adjust == "all" else [adjust]
                    for adj in adjust_all:
                        try:
                            # 先获取数据
                            df = fetcher.fetch_stock_daily(code=code, start_date=start_date, end_date=end_date, adjust=adj)
                            
                            # 创建并启动线程来保存数据到数据库
                            t = threading.Thread(
                                target=_insert_daily_thread,
                                args=(code, df, adj, conn_local, result_dict),
                                name=f"save_db_{code}_{adj}"
                            )
                            threads.append(t)
                            t.start()
                        except Exception as e:
                            logger.exception("数据获取失败: code=%s, adj=%s, error=%s", code, adj, e)
                            with result_dict['lock']:
                                result_dict['failed_codes'].append((code, f"获取数据失败: {str(e)}"))
                
                # 等待所有线程完成
                for t in threads:
                    t.join()
            
            # 记录结果
            total_saved = result_dict['total_saved']
            success_count = len(result_dict['success_codes'])
            failed_count = len(result_dict['failed_codes'])
            
            save_type = "CSV文件" if to_csv else "数据库"
            logger.info("任务完成：codes=%s, 保存到=%s, total_saved=%s, success_codes=%d, failed_codes=%d", 
                      codes, save_type, total_saved, success_count, failed_count)
            
            if failed_count > 0:
                logger.warning("部分代码保存失败: %s", result_dict['failed_codes'])
            
            return total_saved > 0
        finally:
            # 只有在保存到数据库时才需要关闭连接
            if not to_csv and conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass


