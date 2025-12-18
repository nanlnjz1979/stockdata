#!/bin/bash

set -e


echo "=== Step 1: 安装必要依赖 ==="
sudo apt update
sudo apt install -y apt-transport-https ca-certificates curl gnupg lsb-release


echo "=== Step 2: 下载并 dearmor ClickHouse GPG Key ==="
sudo mkdir -p /usr/share/keyrings
curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key  -o repomd.xml.key
sudo gpg --batch --yes --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg repomd.xml.key
rm -f repomd.xml.key
echo "Key successfully installed at /usr/share/keyrings/clickhouse-keyring.gpg"


echo "=== Step 3: 添加 ClickHouse APT 源 ==="
ARCH=$(dpkg --print-architecture)
echo "deb [arch=${ARCH} signed-by=/usr/share/keyrings/clickhouse-keyring.gpg] https://packages.clickhouse.com/deb  stable main" \
    | sudo tee /etc/apt/sources.list.d/clickhouse.list


echo "=== Step 4: 更新源并安装 ClickHouse ==="
sudo apt update
sudo apt install -y clickhouse-server clickhouse-client


echo "=== Step 5: 启动并启用 ClickHouse 服务 ==="
sudo systemctl enable clickhouse-server
sudo systemctl start clickhouse-server
sudo systemctl status clickhouse-server --no-pager


echo "=== Step 6: 测试 ClickHouse 客户端 ==="
echo "尝试登录 ClickHouse 客户端..."
clickhouse-client --query "SELECT version();"


echo "=== ClickHouse 安装完成 ==="