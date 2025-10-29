import os
from django.core.wsgi import get_wsgi_application
# 根据当前机器逻辑 CPU 数量自动设置最大线程数，提升并发处理效率
import multiprocessing
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stockserver.settings')
os.environ.setdefault('WSGI_MAX_THREADS', str(multiprocessing.cpu_count() * 2 + 1))
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'stockserver.settings')

application = get_wsgi_application()