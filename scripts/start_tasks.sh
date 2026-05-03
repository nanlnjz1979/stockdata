#!/bin/bash

# 定时任务启动脚本 - 禁用代理
cd /Users/nan/git/stockdata/backend
source venv/bin/activate

export no_proxy=127.0.0.1,localhost,0.0.0.0
export NO_PROXY=127.0.0.1,localhost,0.0.0.0
export http_proxy=
export https_proxy=
export HTTP_PROXY=
export HTTPS_PROXY=

echo "启动定时任务服务 (禁用代理)..."
python manage.py qcluster
