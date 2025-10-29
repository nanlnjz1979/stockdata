import time
from datetime import datetime

try:
    import akshare as ak
except Exception:
    ak = None
import os
# 新增：QuestDB PG 连接
try:
    import psycopg2
except Exception:
    psycopg2 = None


def qdb_connect():
    """连接 QuestDB（PG wire），从环境变量读取连接信息。"""
    if not psycopg2:
        return None
    import os
    host = os.getenv('QDB_HOST', 'localhost')
    port = int(os.getenv('QDB_PORT', '8812'))
    user = os.getenv('QDB_USER', 'admin')
    password = os.getenv('QDB_PASS', 'quest')
    dbname = os.getenv('QDB_DB', 'qdb')
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)
        conn.autocommit = True
        return conn
    except Exception:
        return None


# 修改：允许复用连接
def qdb_ensure_tables(conn=None):
    conn = conn or qdb_connect()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute("""
          create table if not exists stock_basic (
            code symbol,
            name string,
            company_name string,
            market symbol,
            listing_date date
          );
        """)

        cur.execute("""
          create table if not exists stock_daily (
            code symbol,
            trade_date date,
            adjust_type symbol,
            open double,
            close double,
            high double,
            low double,
            volume long,
            amount double,
            turnover double,
            outstanding_share double
          );
        """)
        
        cur.execute("""
          create table if not exists tasks (
            task_id symbol,
            task_type string,
            task_desc string,
            task_params string,
            priority int,
            status symbol,
            created_at timestamp,
            started_at timestamp,
            ended_at timestamp
          );
        """)
        # 确保列存在（兼容已存在旧表）
        try:
            cur.execute("alter table tasks add column created_at timestamp")
        except Exception:
            pass
        try:
            cur.execute("alter table tasks add column started_at timestamp")
        except Exception:
            pass
        try:
            cur.execute("alter table tasks add column ended_at timestamp")
        except Exception:
            pass
        if conn is not qdb_connect:
            pass
        return True
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return False


def qdb_insert_basic(rows, conn=None):
    """批量插入基础股票到 QuestDB。rows 为 [{'code','name','company_name','market','listing_date'}]"""
    if not rows:
        return 0
    conn_local = conn or qdb_connect()
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        qdb_ensure_tables(conn_local)
        values = []
        for r in rows:
            code = r.get('code')
            name = r.get('name') or (code or '')
            company_name = r.get('company_name') or name
            market = r.get('market') or ''
            ld = r.get('listing_date')
            if isinstance(ld, datetime):
                ld = ld.date()
            values.append((code, name, company_name, market, ld))
        try:
            cur.executemany(
                "insert into stock_basic (code, name, company_name, market, listing_date) values (%s,%s,%s,%s,%s)",
                values
            )
        except Exception as e:
            print(f"qdb_insert_basic executemany failed: {e}")
            if conn is None:
                try:
                    conn_local.close()
                except Exception:
                    pass
            return 0
        if conn is None:
            conn_local.close()
        return len(values)
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return 0


# 新增：删除 QuestDB 表（stock_daily 与 stock_basic）
def qdb_drop_tables(conn=None):
  conn_local = conn or qdb_connect()
  if not conn_local:
    return False
  try:
    cur = conn_local.cursor()
    for tbl in ['stock_daily', 'stock_basic', 'tasks']:
      try:
        cur.execute(f"drop table if exists {tbl}")
      except Exception:
        try:
          cur.execute(f"drop table {tbl}")
        except Exception:
          pass
    if conn is None:
      conn_local.close()
    return True
  except Exception:
    try:
      if conn is None:
        conn_local.close()
    except Exception:
      pass
    return False

# 一次读取全部基础股票（code, market, name）
def qdb_get_all_basic(conn=None):
    conn_local = conn or qdb_connect()
    if not conn_local:
        return []
    try:
        cur = conn_local.cursor()
        qdb_ensure_tables(conn_local)
        cur.execute('select code, market, name, company_name, listing_date from stock_basic')
        rows = cur.fetchall() or []
        if conn is None:
            conn_local.close()
        return [{'code': r[0], 'market': r[1], 'name': r[2], 'company_name': r[3], 'listing_date': r[4]} for r in rows]
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return []


def qdb_insert_daily(code, df, adj, conn=None):
    """将单个股票某复权类型的日线数据写入 QuestDB。"""
    if df is None or getattr(df, 'empty', True):
        return 0
    conn_local = conn or qdb_connect()
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        qdb_ensure_tables(conn_local)
        cols = {
            '日期': 'date', 
            '开盘': 'open', 
            '收盘': 'close', 
            '最高': 'high', 
            '最低': 'low',
            '成交量': 'volume', 
            '成交额': 'amount', 
            '换手率': 'turnover',
            '流通股本': 'outstanding_share'
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
                trade_date.date(),
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
            except Exception as e:
                try:
                    bad = values[0] if values else None
                    print(f"qdb_insert_daily executemany failed: {e}; sample={bad}")
                except Exception:
                    pass
                if conn is None:
                    try:
                        conn_local.close()
                    except Exception:
                        pass
                return 0
        if conn is None:
            conn_local.close()
        return len(values)
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return 0


def _num(x):
    try:
        return float(x) if x not in (None, '') else None
    except Exception:
        return None


def _int(x):
    try:
        return int(x) if x not in (None, '') else None
    except Exception:
        return None


def ensure_tables():
    """确保新表存在（避免当前环境无法执行makemigrations/migrate）。"""
    from django.db import connection
    from stocks.models import StockDaily, StockUpdateStatus
    existing = set(connection.introspection.table_names())
    with connection.schema_editor() as editor:
        if StockDaily._meta.db_table not in existing:
            editor.create_model(StockDaily)
        if StockUpdateStatus._meta.db_table not in existing:
            editor.create_model(StockUpdateStatus)

def sync_basic_to_django(conn=None):
    """将 QuestDB 中的基础股票信息同步到 Django ORM（StockBasic）。"""
    try:
        from stocks.models import StockBasic
    except Exception:
        return 0
    basics = qdb_get_all_basic(conn=conn)
    cnt = 0
    for b in basics:
        code = b.get('code')
        name = b.get('name') or (code or '')
        company_name = b.get('company_name') or name
        market = b.get('market') or ''
        ld = b.get('listing_date')
        try:
            StockBasic.objects.update_or_create(
                stock_code=code,
                defaults={'stock_name': name, 'company_name': company_name, 'market': market, 'listing_date': ld}
            )
            cnt += 1
        except Exception:
            pass
    return cnt


def fetch_realtime_quote(code: str):
    """占位：从 Akshare/Tushare 拉取实时行情。"""
    if ak:
        # 示例：实际使用需根据数据源 API 调整
        # df = ak.stock_zh_a_spot_em()
        # return df[df['代码'] == code].to_dict('records')
        pass
    return [{
        'code': code,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'open_price': 10.0,
        'close_price': 10.2,
        'high_price': 10.3,
        'low_price': 9.9,
        'volume': 12345,
    }]


def clean_data(records):
    """占位：清洗与缺失值处理。"""
    return records


def compute_indicators(records):
    """占位：计算衍生指标（如 MA5/10/20）。"""
    return records


def write_backend(records):
    """占位：调用后端 API 写入（后续接入）。"""
    # requests.post('http://localhost:8000/api/...', json=records)
    print(f"write_backend: {len(records)} records")


# QuestDB helpers
def qdb_basic_count(conn=None):
    conn_local = conn or qdb_connect()
    if not conn_local:
        return 0
    try:
        cur = conn_local.cursor()
        qdb_ensure_tables(conn_local)
        cur.execute('select count(*) from stock_basic')
        n = cur.fetchone()[0] if cur.rowcount != -1 else 0
        if conn is None:
            conn_local.close()
        return int(n or 0)
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return 0

def qdb_list_codes(conn=None):
    conn_local = conn or qdb_connect()
    if not conn_local:
        return []
    try:
        cur = conn_local.cursor()
        qdb_ensure_tables(conn_local)
        cur.execute('select code from stock_basic')
        rows = cur.fetchall() or []
        if conn is None:
            conn_local.close()
        return [r[0] for r in rows]
    except Exception:
        try:
            if conn is None:
                conn_local.close()
        except Exception:
            pass
        return []

def qdb_get_market(code: str):
    conn = qdb_connect()
    if not conn:
        return None
    try:
        cur = conn.cursor()
        qdb_ensure_tables()
        cur.execute('select market from stock_basic where code=$1 limit 1', (code,))
        row = cur.fetchone()
        conn.close()
        return row[0] if row else None
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return None


def write_daily_to_db(code: str, market='', conn=None):
    total_saved = 0
    latest_date = None
    def make_symbol(c, m):
        m = (m or '').upper()
        if m == 'SH':
            return 'sh' + c
        if m == 'SZ':
            return 'sz' + c
        if m == 'BJ':
            return 'bj' + c
        return 'sz' + c
    symbol = make_symbol(code, market)
    for adj in ['', 'qfq', 'hfq']:
        if not ak:
            df = None
        else:
            try:
                df = ak.stock_zh_a_daily(symbol=symbol, start_date='19900101', end_date=datetime.now().strftime('%Y%m%d'), adjust=adj)
            except Exception:
                df = None
        try:
            saved = qdb_insert_daily(code, df, adj, conn=conn)#把akshare读出来的数据保存到数据库中
            total_saved += saved
            if df is not None and not df.empty:
                df2 = df.rename(columns={'日期': 'date'})
                for _, r in df2.iterrows():
                    d = r.get('date')
                    if not d:
                        continue
                    try:
                        trade_date = d if isinstance(d, datetime) else datetime.strptime(str(d), '%Y-%m-%d')
                        latest_date = max(latest_date or trade_date.date(), trade_date.date())
                    except Exception:
                        pass
        except Exception:
            pass
    return {'code': code, 'saved': total_saved, 'latest_date': latest_date}


def run_once(code: str):
    """单只股票更新（替换原先的占位实时行情）。"""
    return write_daily_to_db(code)


def populate_stock_basic_if_empty(conn=None):
    """若基础股票表为空，则通过akshare拉取并填充（SH/SZ/BJ），写入QuestDB。"""
    if qdb_basic_count(conn=conn) > 0:
        return 0
    inserted = 0
    qdb_rows = []
    if ak:
        try:
            dfs = []
            try:
                dfs.append(('SH', ak.stock_info_sh_name_code()))
            except Exception:
                pass
            try:
                dfs.append(('SZ', ak.stock_info_sz_name_code()))
            except Exception:
                pass
            try:
                dfs.append(('BJ', ak.stock_info_bj_name_code()))
            except Exception:
                pass
            for market, df in dfs:
                if df is None or df.empty:
                    continue
                for _, r in df.iterrows():
                    code = None
                    for key in ['代码', '证券代码', 'A股代码', '股票代码']:
                        v = r.get(key)
                        if v:
                            code = str(v).strip()
                            break
                    name = None
                    for key in [ '证券简称', 'A股简称', '股票简称','证券简称']:
                        v = r.get(key)
                        if v:
                            name = str(v).strip()
                            break
                    company_name = None
                    for key in ['公司名称', '公司全称', '企业名称','证券简称','A股简称']:
                        v = r.get(key)
                        if v:
                            company_name = str(v).strip()
                            break
                    listing_date = None
                    for key in ['上市日期', '上市时间','A股上市日期']:
                        v = r.get(key)
                        if v:
                            try:
                                s = str(v).strip()
                                if '-' in s:
                                    listing_date = datetime.strptime(s, '%Y-%m-%d')
                                else:
                                    listing_date = datetime.strptime(s, '%Y%m%d')
                            except Exception:
                                listing_date = None
                            break
                    if not code:
                        continue
                    qdb_rows.append({
                        'code': code,
                        'name': name or code,
                        'company_name': company_name or name or code,
                        'market': market,
                        'listing_date': listing_date
                    })
                    inserted += 1
        except Exception:
            pass
    try:
        qdb_insert_basic(qdb_rows, conn=conn)
    except Exception:
        pass
    return inserted

