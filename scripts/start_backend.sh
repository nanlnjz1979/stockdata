#!/bin/bash

# 后端启动脚本 - 禁用代理
cd /Users/nan/git/stockdata/backend
source venv/bin/activate

export no_proxy=127.0.0.1,localhost,0.0.0.0
export NO_PROXY=127.0.0.1,localhost,0.0.0.0
export http_proxy=
export https_proxy=
export HTTP_PROXY=
export HTTPS_PROXY=

echo "启动后端服务 (禁用代理)..."
echo "后端地址: http://0.0.0.0:8000"
python manage.py runserver 0.0.0.0:8000
