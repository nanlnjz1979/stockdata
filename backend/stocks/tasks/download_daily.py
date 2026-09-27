import json
import logging
import threading
import os
import time
import pandas as pd
from typing import Any, Dict, List, Optional
from datetime import datetime
from backend.global_config.utils import save_to_csv
from backend.global_config import norm_date
from backend.global_config.utils import _num, _int, make_symbol
from backend.global_config.data_fetch import AkshareFetcher
from backend.global_config.file_config import FileConfig
from .base import BaseTask





def _insert_daily_thread(code, df, conn=None, result_dict=None):
    """在线程中执行数据插入操作并更新结果字典"""
    try:
        saved_count = _insert_daily(code, df, conn)
        if result_dict is not None:
            with result_dict.get('lock', threading.Lock()):
                result_dict['total_saved'] += saved_count
                if saved_count > 0:
                    result_dict['success_codes'].append(code)
                else:
                    result_dict.setdefault('zero_saved_codes', []).append(code)
        if saved_count > 0:
            logger.debug("[下载任务] code=%s 数据库写入完成 saved_rows=%d", code, saved_count)
        else:
            logger.debug("[下载任务] code=%s 数据库写入结果为0行", code)
        return saved_count
    except Exception as e:
        logger.error("线程数据保存失败: code=%s, error=%s", code, e)
        if result_dict is not None:
            with result_dict.get('lock', threading.Lock()):
                result_dict['failed_codes'].append((code, str(e)))
        return 0

def _stored_float(value):
    """Non-nullable Float64 columns reject None and NaN."""
    number = _num(value)
    if number is None or number != number or number in (float("inf"), float("-inf")):
        return 0.0
    return number


def _stored_int(value):
    number = _int(value)
    if number is None:
        number = _num(value)
        if number is None or number != number:
            return 0
        return int(number)
    return number


def _insert_daily(code, df, conn=None):
    if df is None or getattr(df, 'empty', True):
        return 0
    conn_local = conn 
    if not conn_local:
        return 0
    try:
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
                date = d if isinstance(d, datetime) else datetime.strptime(str(d), '%Y-%m-%d')
            except Exception:
                continue
            values.append((
                code,
                date,
                _stored_float(r.get('open')),
                _stored_float(r.get('close')),
                _stored_float(r.get('high')),
                _stored_float(r.get('low')),
                _stored_int(r.get('volume')),
                _stored_float(r.get('amount')),
                _stored_float(r.get('turnover')),
                _stored_float(r.get('outstanding_share')),
            ))
        if values:
            try:
                # clickhouse-driver 用原生数据块写入；SQL 里的 %s 会被服务器原样解析并失败。
                conn_local.execute(
                    """
                    INSERT INTO stock_daily (
                      code, date, open, close, high, low, volume, amount, turnover, outstanding_share
                    ) VALUES
                    """,
                    values,
                )
            except Exception as exc:
                logging.getLogger(__name__).error("写入 stock_daily 失败 code=%s error=%s", code, exc)
                return 0
        if conn is None:
            try:
                # ClickHouse客户端使用disconnect()方法关闭连接
                if hasattr(conn_local, 'disconnect'):
                    conn_local.disconnect()
                else:
                    conn_local.close()
            except Exception:
                pass
        return len(values)
    except Exception:
        try:
            if conn is None:
                # ClickHouse客户端使用disconnect()方法关闭连接
                if hasattr(conn_local, 'disconnect'):
                    conn_local.disconnect()
                else:
                    conn_local.close()
        except Exception:
            pass
        return 0

# 依赖：数据源
fetcher = AkshareFetcher()

logger = logging.getLogger(__name__)


def _short_code_list(codes: List[str], limit: int = 5) -> str:
    if not codes:
        return "-"
    if len(codes) <= limit:
        return ",".join(codes)
    return f"{','.join(codes[:limit])} ... 共{len(codes)}只"


def _format_failed_codes(failed_codes: List[Any], limit: int = 5) -> str:
    if not failed_codes:
        return "-"
    preview = []
    for item in failed_codes[:limit]:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            preview.append(f"{item[0]}:{item[1]}")
        else:
            preview.append(str(item))
    if len(failed_codes) > limit:
        preview.append(f"... 其余{len(failed_codes) - limit}项")
    return " | ".join(preview)


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

    def __init__(self, orm = None):
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
        started_at = time.perf_counter()
        # 检查依赖
        if params_str:
            self.params_str = params_str
        params = self._parse_params()
        market = (params.get('market') or '').upper()
        
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
            logger.warning("[下载任务] task_id=%s 未提供有效的股票代码（params 需包含 'code' 或 'codes'）", self.task_id or "-")
            return False
        
        # 检查是否保存到CSV文件
        to_csv = FileConfig.get("to_csv", False)
        save_type = "CSV文件" if to_csv else "数据库"
        task_prefix = (
            f"[下载任务] task_id={self.task_id or '-'} type={self.task_type or self.taskID()} "
            f"codes={len(codes)} sample={_short_code_list(codes)} "
            f"range={start_date}~{end_date} market={market or '-'} save_to={save_type}"
        )
        summary_log = logger.info if len(codes) > 1 else logger.debug
        summary_log("%s 开始执行", task_prefix)
        
        # 如果不保存到CSV文件，则检查数据库连接
        if not to_csv:
            conn_local = conn 
            if not conn_local:
                logger.error("%s 数据库连接失败", task_prefix)
                return False
        
        try:
            # 用于存储结果
            result_dict = {
                'total_saved': 0,
                'success_codes': [],
                'failed_codes': [],
                'fetched_rows': 0,
                'no_data_count': 0,
                'zero_saved_codes': [],
                'lock': threading.Lock()
            }
            
            if to_csv:
                # 保存到CSV：不使用线程，直接保存
                for code in codes:
                    try:
                        logger.debug("%s code=%s 开始抓取", task_prefix, code)
                        # 只获取不复权的数据
                        df = fetcher.fetch_stock_daily(code=code, start_date=start_date, end_date=end_date)
                        if df is not None and not df.empty:
                            result_dict['fetched_rows'] += len(df)
                            saved_count = save_to_csv(code, df)
                            result_dict['total_saved'] += saved_count
                            
                            if saved_count > 0:
                                result_dict['success_codes'].append(code)
                                logger.debug("%s code=%s 抓取完成并已保存到CSV fetched_rows=%d saved_rows=%d", task_prefix, code, len(df), saved_count)
                            else:
                                result_dict['zero_saved_codes'].append(code)
                                logger.debug("%s code=%s 已抓取到数据但保存结果为0行 fetched_rows=%d", task_prefix, code, len(df))
                        else:
                            result_dict['no_data_count'] += 1
                            logger.debug("%s code=%s 未抓取到数据", task_prefix, code)
                    except Exception as e:
                        logger.error("%s code=%s 数据获取或保存失败: %s", task_prefix, code, e)
                        result_dict['failed_codes'].append((code, str(e)))
            else:
                # 保存到数据库：继续使用线程
                threads = []
                for code in codes:
                    try:
                        logger.debug("%s code=%s 开始抓取", task_prefix, code)
                        # 先获取数据
                        df = fetcher.fetch_stock_daily(code=code, start_date=start_date, end_date=end_date)
                        if df is None or df.empty:
                            result_dict['no_data_count'] += 1
                            logger.debug("%s code=%s 未抓取到数据", task_prefix, code)
                            continue
                        result_dict['fetched_rows'] += len(df)
                        logger.debug("%s code=%s 抓取完成 fetched_rows=%d，准备写入数据库", task_prefix, code, len(df))
                        
                        # 创建并启动线程来保存数据到数据库
                        t = threading.Thread(
                            target=_insert_daily_thread,
                            args=(code, df, conn_local, result_dict),
                            name=f"save_db_{code}"
                        )
                        threads.append(t)
                        t.start()
                    except Exception as e:
                        logger.error("%s code=%s 数据获取失败: %s", task_prefix, code, e)
                        with result_dict['lock']:
                            result_dict['failed_codes'].append((code, f"获取数据失败: {str(e)}"))
                
                # 等待所有线程完成
                for t in threads:
                    t.join()
            
            # 记录结果
            total_saved = result_dict['total_saved']
            success_count = len(result_dict['success_codes'])
            failed_count = len(result_dict['failed_codes'])
            zero_saved_count = len(result_dict['zero_saved_codes'])
            elapsed = time.perf_counter() - started_at
            summary_log(
                "%s 执行完成 fetched_rows=%d saved_rows=%d success_codes=%d no_data=%d zero_saved=%d failed_codes=%d elapsed=%.2fs",
                task_prefix,
                result_dict['fetched_rows'],
                total_saved,
                success_count,
                result_dict['no_data_count'],
                zero_saved_count,
                failed_count,
                elapsed,
            )
            
            if zero_saved_count > 0:
                logger.warning("%s 保存0行摘要: %s", task_prefix, _short_code_list(result_dict['zero_saved_codes']))

            if failed_count > 0:
                logger.warning("%s 失败摘要: %s", task_prefix, _format_failed_codes(result_dict['failed_codes']))
            
            return total_saved > 0
        except Exception as e:
            elapsed = time.perf_counter() - started_at
            logger.error("%s 执行异常 elapsed=%.2fs error=%s", task_prefix, elapsed, e)
            return False
        finally:
            # 只有在保存到数据库时才需要关闭连接
            if not to_csv and conn is None:
                try:
                    # ClickHouse客户端使用disconnect()方法关闭连接
                    if hasattr(conn_local, 'disconnect'):
                        conn_local.disconnect()
                    else:
                        conn_local.close()
                except Exception:
                    pass
