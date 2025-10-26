from rest_framework import serializers
from .models import StockBasic, StockFinance, UserFollow


class StockBasicSerializer(serializers.ModelSerializer):
    class Meta:
        model = StockBasic
        fields = '__all__'


class StockFinanceSerializer(serializers.ModelSerializer):
    stock_code = serializers.CharField(source='stock.stock_code', read_only=True)

    class Meta:
        model = StockFinance
        fields = ['id', 'stock_code', 'report_date', 'revenue', 'net_profit', 'pe', 'pb']


class UserFollowSerializer(serializers.ModelSerializer):
    stock_code = serializers.CharField(source='stock.stock_code')

    class Meta:
        model = UserFollow
        fields = ['id', 'user', 'stock_code', 'follow_time']