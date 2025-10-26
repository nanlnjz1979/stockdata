from django.db import migrations, connection


def drop_industry_if_exists(apps, schema_editor):
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA table_info(stocks_stockbasic)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'industry' in cols:
            try:
                cursor.execute("ALTER TABLE stocks_stockbasic DROP COLUMN industry")
            except Exception:
                # 如果 SQLite 版本不支持 DROP COLUMN，则忽略（列已不存在或不需要）
                pass


def add_industry_if_missing(apps, schema_editor):
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA table_info(stocks_stockbasic)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'industry' not in cols:
            cursor.execute("ALTER TABLE stocks_stockbasic ADD COLUMN industry varchar(50)")


class Migration(migrations.Migration):
    dependencies = [
        ('stocks', '0002_add_company_name'),
    ]

    operations = [
        migrations.RunPython(drop_industry_if_exists, add_industry_if_missing),
    ]