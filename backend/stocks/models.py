from django.db import models
from django.contrib.auth.models import User


class StockBasic(models.Model):
    stock_code = models.CharField(max_length=10, primary_key=True)
    stock_name = models.CharField(max_length=50)
    company_name = models.CharField(max_length=100, blank=True)
    market = models.CharField(max_length=10)
    listing_date = models.DateField(null=True, blank=True)

    def __str__(self):
        return f"{self.stock_code} {self.stock_name}"


class StockFinance(models.Model):
    stock = models.ForeignKey(StockBasic, on_delete=models.CASCADE)
    report_date = models.DateField()
    revenue = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    net_profit = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    pe = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)
    pb = models.DecimalField(max_digits=8, decimal_places=2, null=True, blank=True)


class UserFollow(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    stock = models.ForeignKey(StockBasic, on_delete=models.CASCADE)
    follow_time = models.DateTimeField(auto_now_add=True)

    class Meta:
        unique_together = ('user', 'stock')


class StockDaily(models.Model):
    stock = models.ForeignKey(StockBasic, on_delete=models.CASCADE)
    trade_date = models.DateField()
    adjust_type = models.CharField(max_length=10, choices=(
        ('', '不复权'),
        ('qfq', '前复权'),
        ('hfq', '后复权'),
    ))
    open = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    close = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    high = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    low = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    volume = models.BigIntegerField(null=True, blank=True)
    amount = models.DecimalField(max_digits=20, decimal_places=2, null=True, blank=True)
    turnover = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)
    change = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)
    change_rate = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)
    amplitude = models.DecimalField(max_digits=10, decimal_places=4, null=True, blank=True)

    class Meta:
        unique_together = ('stock', 'trade_date', 'adjust_type')
        indexes = [
            models.Index(fields=['stock', 'trade_date']),
            models.Index(fields=['adjust_type']),
        ]


class StockUpdateStatus(models.Model):
    stock = models.OneToOneField(StockBasic, on_delete=models.CASCADE)
    last_updated_date = models.DateField(null=True, blank=True)
    last_run_time = models.DateTimeField(null=True, blank=True)
    note = models.CharField(max_length=200, blank=True)

    def __str__(self):
        return f"{self.stock.stock_code} 更新至 {self.last_updated_date}"


# 还原并完善 Task 模型，添加任务状态字段
class Task(models.Model):
    task_id = models.CharField(max_length=50, unique=True)
    task_type = models.CharField(max_length=50)
    task_desc = models.CharField(max_length=100, blank=True)
    task_params = models.JSONField(null=True, blank=True)
    priority = models.IntegerField(default=0)
    status = models.CharField(
        max_length=10,
        choices=(
            ('待处理', '待处理'),
            ('处理中', '处理中'),
            ('成功', '成功'),
            ('失败', '失败'),
            ('重试中', '重试中'),
            ('已取消', '已取消'),
        ),
        default='待处理'
    )

    class Meta:
        db_table = 'tasks'
        indexes = [
            models.Index(fields=['task_type']),
            models.Index(fields=['priority']),
        ]

    def __str__(self):
        return f"{self.task_id} ({self.task_type})"
