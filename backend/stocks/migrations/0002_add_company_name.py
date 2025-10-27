from django.db import migrations, connection


def add_company_name_if_missing(apps, schema_editor):
    with connection.cursor() as cursor:
        cursor.execute("PRAGMA table_info(stocks_stockbasic)")
        cols = [row[1] for row in cursor.fetchall()]
        if 'company_name' not in cols:
            cursor.execute("ALTER TABLE stocks_stockbasic ADD COLUMN company_name varchar(100)")


class Migration(migrations.Migration):
    dependencies = [
        ('stocks', '0001_initial'),
    ]

    operations = [
        migrations.RunPython(add_company_name_if_missing, migrations.RunPython.noop),
    ]