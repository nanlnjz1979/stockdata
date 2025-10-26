from django.db import migrations

class Migration(migrations.Migration):
    dependencies = [
        ('stocks', '0001_initial'),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE stocks_stockbasic ADD COLUMN company_name varchar(100);",
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]