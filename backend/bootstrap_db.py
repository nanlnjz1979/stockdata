import os
import django
from django.db import connection

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stockserver.settings')
django.setup()

from stocks.models import StockBasic, StockFinance, UserFollow, StockDaily, StockUpdateStatus

models = [StockBasic, StockFinance, UserFollow, StockDaily, StockUpdateStatus]

existing = set(connection.introspection.table_names())
created = []

with connection.schema_editor() as schema:
    for m in models:
        table = m._meta.db_table
        if table not in existing:
            schema.create_model(m)
            created.append(table)

print('Created tables:', created)
print('All tables:', connection.introspection.table_names())