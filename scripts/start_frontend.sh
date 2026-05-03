#!/bin/bash

# 前端启动脚本 - 禁用代理
cd /Users/nan/git/stockdata/frontend
export no_proxy=127.0.0.1,localhost,0.0.0.0
export NO_PROXY=127.0.0.1,localhost,0.0.0.0
export http_proxy=
export https_proxy=
export HTTP_PROXY=
export HTTPS_PROXY=

echo "启动前端服务 (禁用代理)..."
echo "前端地址: http://localhost:5500"
python3 -m http.server 5500
