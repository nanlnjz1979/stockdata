from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import logging
import pandas as pd
from datetime import datetime
import os
import psycopg2
from psycopg2 import OperationalError
from db.db_pool import get_conn, put_conn
from global_config.data_fetch import get_sw_industry_first_info, get_sw_industry_second_info, get_sw_industry_third_info, DataFetchError

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _infer_market(stock_code):
    """Infer A-share market suffix from a six-digit stock code."""
    code = str(stock_code or '').strip()
    if '.' in code:
        code, market = code.split('.', 1)
        return code, market.upper()

    if code.startswith(('600', '601', '603', '605', '688', '689')):
        return code, 'SH'
    if code.startswith(('000', '001', '002', '003', '004', '300', '301', '302')):
        return code, 'SZ'
    if code.startswith(('430', '830', '831', '832', '833', '834', '835', '836', '837', '838', '839',
                        '870', '871', '872', '873', '874', '875', '876', '877', '878', '920')):
        return code, 'BJ'
    return code, ''


def _normalize_sw_code(sw_code):
    return str(sw_code or '').strip().replace('.SI', '').replace('.si', '')


def _normalize_sw_component_df(df):
    """Normalize akshare.index_component_sw fields for current and legacy save paths."""
    standard_columns = ['证券代码', '证券名称', '最新权重', '计入日期']
    if df is None:
        df = pd.DataFrame()
    elif not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(df)

    alias_map = {
        '证券代码': ['证券代码', 'stockcode', 'stock_code', '代码', '股票代码', '成分券代码', 'code'],
        '证券名称': ['证券名称', 'stockname', 'stock_name', '名称', '股票简称', '成分券名称', 'name'],
        '最新权重': ['最新权重', 'newweight', 'weight', '权重'],
        '计入日期': ['计入日期', 'beginningdate', '纳入时间', '纳入日期', 'include_date'],
    }

    rename_map = {}
    for target, candidates in alias_map.items():
        for candidate in candidates:
            if candidate in df.columns:
                rename_map[candidate] = target
                break

    normalized_df = df.rename(columns=rename_map).copy()
    for column in standard_columns:
        if column not in normalized_df.columns:
            normalized_df[column] = None if column in ('最新权重', '计入日期') else ''

    normalized_df = normalized_df[standard_columns]
    normalized_df['证券代码'] = normalized_df['证券代码'].fillna('').astype(str).str.strip()
    normalized_df['证券名称'] = normalized_df['证券名称'].fillna('').astype(str).str.strip()
    normalized_df['最新权重'] = pd.to_numeric(normalized_df['最新权重'], errors='coerce')
    normalized_df['计入日期'] = pd.to_datetime(normalized_df['计入日期'], errors='coerce').dt.date
    normalized_df['股票代码'] = normalized_df['证券代码']
    normalized_df['股票简称'] = normalized_df['证券名称']
    normalized_df['纳入时间'] = normalized_df['计入日期'].apply(
        lambda value: value.strftime('%Y-%m-%d') if pd.notna(value) else ''
    )
    return normalized_df


def _fetch_sw_component_df(sw_code):
    """Fetch SW component stocks through akshare.index_component_sw only."""
    try:
        import akshare as ak

        normalized_sw_code = _normalize_sw_code(sw_code)
        if not normalized_sw_code:
            raise DataFetchError("申万行业代码为空")
        return _normalize_sw_component_df(ak.index_component_sw(symbol=normalized_sw_code))
    except DataFetchError:
        raise
    except Exception as e:
        raise DataFetchError(f"akshare.index_component_sw 获取失败: {e}")


def _insert_sw_industry_stocks(conn, records):
    """统一写入正式申万股票分类表。"""
    if not records:
        return 0

    insert_data = [
        (
            item['code'],
            item['market'],
            item['stock_name'],
            item['sw_first_level'],
            item['sw_second_level'],
            item['sw_third_level'],
        )
        for item in records
    ]
    conn.execute(
        """
        INSERT INTO default.sw_industry_stocks
        (
            code,
            market,
            stock_name,
            sw_first_level,
            sw_second_level,
            sw_third_level
        ) VALUES
        """,
        insert_data,
    )
    return len(insert_data)


class SWIndustryDataAPI(APIView):
    """
    申万行业数据抓取和存储API
    调用方式: POST /api/stocks/sw/generate
    """

    def _load_cached_industry_snapshot(self, conn):
        """Load the latest cached SW industry classification from ClickHouse."""
        query_sql = """
        SELECT
            industry_code,
            industry_name,
            parent_industry,
            component_count,
            static_pe,
            ttm_pe,
            pb_ratio,
            static_dividend_yield
        FROM sw_industry_data_v
        ORDER BY industry_code
        """
        rows = conn.execute(query_sql)
        columns = [
            'industry_code',
            'industry_name',
            'parent_industry',
            'component_count',
            'static_pe',
            'ttm_pe',
            'pb_ratio',
            'static_dividend_yield',
        ]
        return pd.DataFrame(rows, columns=columns)

    def _split_cached_industry_levels(self, cached_df):
        """Split cached SW industry data into first/second/third level sets."""
        if cached_df.empty:
            return cached_df, cached_df, cached_df

        first_df = cached_df[cached_df['parent_industry'] == ''].copy()
        first_level_names = set(first_df['industry_name'].astype(str).tolist())
        second_df = cached_df[cached_df['parent_industry'].isin(first_level_names)].copy()
        third_df = cached_df[
            (cached_df['parent_industry'] != '')
            & (~cached_df['parent_industry'].isin(first_level_names))
        ].copy()
        return first_df, second_df, third_df
    
    def post(self, request):
        try:
            # 获取数据库连接
            conn = get_conn()
            
            try:
                try:
                    # 抓取申万一级行业数据
                    sw_first_df = get_sw_industry_first_info()
                    
                    # 抓取申万二级行业数据
                    sw_second_df = get_sw_industry_second_info()
                    
                    # 抓取申万三级行业数据
                    sw_third_df = get_sw_industry_third_info()
                except DataFetchError as fetch_error:
                    cached_df = self._load_cached_industry_snapshot(conn)
                    if cached_df.empty:
                        raise

                    cached_first_df, cached_second_df, cached_third_df = self._split_cached_industry_levels(cached_df)
                    logger.warning(
                        "申万行业实时抓取失败，回退到数据库已有快照: %s",
                        str(fetch_error)
                    )
                    return Response({
                        "success": True,
                        "message": "申万行业实时抓取失败，已回退到数据库已有分类数据",
                        "data": {
                            "first_level_count": len(cached_first_df),
                            "second_level_count": len(cached_second_df),
                            "third_level_count": len(cached_third_df),
                            "total_count": len(cached_df),
                            "source": "cached",
                            "fallback_reason": str(fetch_error),
                        }
                    }, status=status.HTTP_200_OK)
                
                # 表已经提前创建，跳过表操作
                logger.info("表已存在，跳过表创建操作")
                
                # 存储数据
                current_timestamp = datetime.now()
                
                # 存储一级行业数据
                self._save_industry_data(conn, sw_first_df, "一级行业", current_timestamp)
                
                # 存储二级行业数据，需要关联一级行业
                self._save_industry_data(conn, sw_second_df, "二级行业", current_timestamp, sw_first_df)
                
                # 存储三级行业数据，需要关联二级行业
                self._save_industry_data(conn, sw_third_df, "三级行业", current_timestamp, sw_second_df)
                
                return Response({
                    "success": True,
                    "message": "申万行业数据抓取和存储成功",
                    "data": {
                        "first_level_count": len(sw_first_df),
                        "second_level_count": len(sw_second_df),
                        "third_level_count": len(sw_third_df),
                        "total_count": len(sw_first_df) + len(sw_second_df) + len(sw_third_df)
                    }
                }, status=status.HTTP_200_OK)
                
            finally:
                # 归还数据库连接
                put_conn(conn)
                
        except ImportError:
            logger.error("缺少akshare依赖")
            return Response({
                "success": False,
                "message": "缺少akshare依赖，请安装: pip install akshare"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except DataFetchError as e:
            logger.error(f"数据抓取失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据抓取失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        except OperationalError as e:
            logger.error(f"QuestDB连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"抓取申万行业数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"数据抓取失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
    

    
    def _save_industry_data(self, conn, df, level_name, timestamp, parent_df=None):
        """
        保存行业数据到数据库
        """
        try:
            # 处理DataFrame，添加timestamp列并调整列名
            insert_df = df.copy()
            
            # 确定上级行业
            insert_df['parent_industry'] = ''  # 一级行业默认空字符串
            if '上级行业' in insert_df.columns:
                insert_df['parent_industry'] = insert_df['上级行业'].fillna('')
            
            # 提取和转换字段
            insert_df['component_count'] = insert_df.get('成份个数', 0).fillna(0)
            insert_df['static_pe'] = insert_df.get('静态市盈率', 0).fillna(0)
            insert_df['ttm_pe'] = insert_df.get('TTM(滚动)市盈率', insert_df.get('TTM市盈率', 0)).fillna(0)
            insert_df['pb_ratio'] = insert_df.get('市净率', 0).fillna(0)
            insert_df['static_dividend_yield'] = insert_df.get('静态股息率', 0).fillna(0)
            
            # 重命名列以匹配数据库表结构
            insert_df = insert_df.rename(columns={
                '行业代码': 'industry_code',
                '行业名称': 'industry_name'
            })
            
            # 准备插入数据
            insert_data = []
            for _, row in insert_df.iterrows():
                # 将每一行转换为Python原生类型的元组，确保LowCardinality(String)字段使用空字符串而非None
                data_row = (
                    str(row['industry_code']) if row['industry_code'] is not None else '',
                    str(row['industry_name']) if row['industry_name'] is not None else '',
                    str(row['parent_industry']) if row['parent_industry'] is not None else '',
                    int(row['component_count']),
                    float(row['static_pe']),
                    float(row['ttm_pe']),
                    float(row['pb_ratio']),
                    float(row['static_dividend_yield']),
                    timestamp  # 已经是Python datetime类型
                )
                insert_data.append(data_row)
            
            # 使用execute方法批量插入数据
            if insert_data:
                # ClickHouse驱动直接接受数据列表，不需要手动构建SQL
                insert_sql = """
                INSERT INTO sw_industry_data (
                    industry_code, industry_name, parent_industry, component_count, 
                    static_pe, ttm_pe, pb_ratio, static_dividend_yield, timestamp
                ) VALUES
                """
                
                # 直接将数据列表传递给execute方法
                conn.execute(insert_sql, insert_data)
            
            # ClickHouse自动提交，不需要显式commit
            logger.info(f"成功存储{level_name}数据，共{len(insert_data)}条")
            
        except Exception as e:
            # ClickHouse不需要显式rollback
            logger.error(f"存储{level_name}数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            raise


class SWIndustryClassificationAPI(APIView):
    """
    申万行业分类数据查询API
    调用方式: GET /api/stocks/sw/classification
    """
    
    def get(self, request):
        try:
            # 获取数据库连接
            conn = get_conn()
            
            # 查询申万行业分类数据
            query_sql = """
            SELECT 
                industry_code,
                industry_name,
                parent_industry,
                component_count,
                static_pe,
                ttm_pe,
                pb_ratio,
                static_dividend_yield
            FROM 
                sw_industry_data_v
            ORDER BY 
                CASE 
                    WHEN parent_industry = '' THEN 0
                    ELSE 1
                END,
                industry_code
            """
            
            # 使用ClickHouse Client的execute方法查询数据
            rows = conn.execute(query_sql)
            
            # 定义列名
            columns = ['industry_code', 'industry_name', 'parent_industry', 'component_count', 
                     'static_pe', 'ttm_pe', 'pb_ratio', 'static_dividend_yield']
            
            # 创建DataFrame
            df = pd.DataFrame(rows, columns=columns)
            
            # 转换为字典列表
            result = df.to_dict('records')
            
            return Response({
                "success": True,
                "message": "查询成功",
                "data": result
            }, status=status.HTTP_200_OK)
            
        except OperationalError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"查询申万行业分类数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"查询失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 归还数据库连接
            if 'conn' in locals():
                put_conn(conn)


class SWThirdLevelIndustryCodesAPI(APIView):
    """
    申万三级行业成分股API
    调用方式: GET /api/stocks/sw/third_level_industry_codes?code=行业代码
    功能: 获取并保存三级行业的成分股数据
    """
    

    
    def _save_industry_stocks(self, conn, df, industry_code):
        """
        保存行业成分股数据到数据库
        使用批量插入方式提高性能
        """
        try:
            hierarchy_rows = conn.execute(
                """
                SELECT
                    third.industry_name,
                    second.industry_name,
                    first.industry_name
                FROM default.sw_industry_data_v AS third
                LEFT JOIN default.sw_industry_data_v AS second
                    ON third.parent_industry = second.industry_name
                LEFT JOIN default.sw_industry_data_v AS first
                    ON second.parent_industry = first.industry_name
                WHERE third.industry_code = %(industry_code)s
                LIMIT 1
                """,
                {'industry_code': industry_code},
            )
            sw_third_level, sw_second_level, sw_first_level = (
                hierarchy_rows[0] if hierarchy_rows else ('', '', '')
            )
            records = []
            for _, row in df.iterrows():
                code, market = _infer_market(row.get('证券代码', ''))
                if not code:
                    continue
                records.append({
                    'code': code,
                    'market': market,
                    'stock_name': str(row.get('证券名称', '') or ''),
                    'sw_first_level': str(sw_first_level or ''),
                    'sw_second_level': str(sw_second_level or ''),
                    'sw_third_level': str(sw_third_level or ''),
                })

            saved_count = _insert_sw_industry_stocks(conn, records)
            logger.info("申万行业 %s 成分股保存完成，写入 %d 条", industry_code, saved_count)
        except Exception as e:
            # ClickHouse不需要显式rollback
            logger.error(f"保存行业{industry_code}的成分股数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
    
    def get(self, request):
        conn = None
        try:
            # 获取请求中的code参数
            code = request.GET.get('code', None)
            
            # 如果提供了code参数，获取该行业对应的股票代码
            if code:
                df = _fetch_sw_component_df(code)
                
                # 获取数据库连接并保存数据
                conn = get_conn()
                

                # 表已经提前创建，跳过表检查
                
                # 保存数据到数据库
                self._save_industry_stocks(conn, df, code)
                
                # 转换为字典列表返回
                result = df.to_dict('records')
                
                return Response({
                    "success": True,
                    "message": f"成功获取并保存行业{code}的成分股数据",
                    "data": {
                        #"stocks": result,
                        "count": len(result),
                        "saved_to_db": True
                    }
                }, status=status.HTTP_200_OK)
            else:
                # 如果没有提供code参数，返回错误信息
                return Response({
                    "success": False,
                    "message": "请提供行业代码参数code",
                    "data": None
                }, status=status.HTTP_400_BAD_REQUEST)
           
        except OperationalError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"查询或保存行业数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"操作失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            # 归还数据库连接
            if conn:
                put_conn(conn)


class SWIndustryStocksSyncAPI(APIView):
    """
    申万三级行业股票分类同步API
    调用方式: POST /api/stocks/sw/sync_industry_stocks
    功能: 使用 ak.index_component_sw 获取申万三级成分股并保存到 sw_industry_stocks
    """

    def _get_third_level_industries(self, conn):
        query_sql = """
        SELECT
            third.industry_code AS sw_third_code,
            third.industry_name AS sw_third_level,
            second.industry_name AS sw_second_level,
            first.industry_name AS sw_first_level
        FROM default.sw_industry_data_v AS third
        LEFT JOIN default.sw_industry_data_v AS second
            ON third.parent_industry = second.industry_name
        LEFT JOIN default.sw_industry_data_v AS first
            ON second.parent_industry = first.industry_name
        WHERE third.parent_industry != ''
          AND second.parent_industry != ''
        ORDER BY third.industry_code
        """
        rows = conn.execute(query_sql)
        columns = ['sw_third_code', 'sw_third_level', 'sw_second_level', 'sw_first_level']
        return [dict(zip(columns, row)) for row in rows]

    def _fetch_component_stocks(self, industry):
        df = _fetch_sw_component_df(industry['sw_third_code'])
        if df is None or df.empty:
            return []

        records = []
        for _, row in df.iterrows():
            stock_code, market = _infer_market(row.get('证券代码', ''))
            if not stock_code:
                continue
            records.append({
                'code': stock_code,
                'market': market,
                'stock_name': str(row.get('证券名称', '') or ''),
                'sw_first_level': str(industry.get('sw_first_level') or ''),
                'sw_second_level': str(industry.get('sw_second_level') or ''),
                'sw_third_level': str(industry.get('sw_third_level') or ''),
            })
        return records

    def post(self, request):
        conn = None
        try:
            conn = get_conn()
            industries = self._get_third_level_industries(conn)
            if not industries:
                return Response({
                    "success": False,
                    "message": "未找到申万三级行业数据，请先执行 /api/stocks/sw/generate",
                    "data": None
                }, status=status.HTTP_400_BAD_REQUEST)

            records_by_code = {}
            failed_industries = []
            empty_industries = []
            duplicate_count = 0

            for industry in industries:
                try:
                    records = self._fetch_component_stocks(industry)
                    if not records:
                        logger.warning(
                            "申万三级行业 %s(%s) 官方接口返回空成分股，已跳过；该行业代码可能已失效或当前无公开成分股数据",
                            industry['sw_third_level'],
                            industry['sw_third_code'],
                        )
                        empty_industries.append({
                            "sw_third_code": industry.get('sw_third_code', ''),
                            "sw_third_level": industry.get('sw_third_level', ''),
                            "reason": "empty_components",
                        })
                        continue

                    for item in records:
                        if item['code'] in records_by_code:
                            duplicate_count += 1
                        records_by_code[item['code']] = item
                    logger.info(
                        "申万三级行业 %s(%s) 获取成分股 %d 条",
                        industry['sw_third_level'],
                        industry['sw_third_code'],
                        len(records)
                    )
                except Exception as e:
                    logger.warning(
                        "申万三级行业 %s(%s) 成分股获取失败: %s",
                        industry.get('sw_third_level', ''),
                        industry.get('sw_third_code', ''),
                        str(e)
                    )
                    failed_industries.append({
                        "sw_third_code": industry.get('sw_third_code', ''),
                        "sw_third_level": industry.get('sw_third_level', ''),
                        "error": str(e),
                    })

            records = list(records_by_code.values())
            saved_count = _insert_sw_industry_stocks(conn, records)

            return Response({
                "success": True,
                "message": f"申万三级股票分类同步完成，写入{saved_count}条，跳过{len(empty_industries)}个空成分行业",
                "data": {
                    "industry_count": len(industries),
                    "stock_count": saved_count,
                    "duplicate_count": duplicate_count,
                    "empty_count": len(empty_industries),
                    "empty_industries": empty_industries[:50],
                    "failed_count": len(failed_industries),
                    "failed_industries": failed_industries[:20],
                }
            }, status=status.HTTP_200_OK)

        except OperationalError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            return Response({
                "success": False,
                "message": f"数据库连接失败: {str(e)}"
            }, status=status.HTTP_503_SERVICE_UNAVAILABLE)
        except Exception as e:
            logger.error(f"同步申万三级股票分类时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return Response({
                "success": False,
                "message": f"同步失败: {str(e)}"
            }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        finally:
            if conn:
                put_conn(conn)
