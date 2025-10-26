import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stockserver.settings')
django.setup()
from django.db import connection
from stocks.models import StockDaily, StockUpdateStatus

def main():
    existing = set(connection.introspection.table_names())
    print('Existing tables count:', len(existing))
    with connection.schema_editor() as editor:
        if StockDaily._meta.db_table not in existing:
            print('Creating table:', StockDaily._meta.db_table)
            editor.create_model(StockDaily)
        else:
            print('Table exists:', StockDaily._meta.db_table)
        if StockUpdateStatus._meta.db_table not in existing:
            print('Creating table:', StockUpdateStatus._meta.db_table)
            editor.create_model(StockUpdateStatus)
        else:
            print('Table exists:', StockUpdateStatus._meta.db_table)
    print('Done.')

if __name__ == '__main__':
    main()