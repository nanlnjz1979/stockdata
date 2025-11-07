from rest_framework import serializers
from .models import StockBasic, StockFinance


class StockBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = StockBasic
        fields = '__all__'


class StockFinanceSerializer(serializers.ModelSerializer):
    stock_code = serializers.CharField(source='stock.stock_code', read_only=True)

    class Meta:
        model = StockFinance
        fields = ['id', 'stock_code', 'report_date', 'revenue', 'net_profit', 'pe', 'pb']


# UserFollowSerializer已移除