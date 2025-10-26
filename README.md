# 股票数据管理系统（项目骨架）

本仓库基于你的架构设计，提供可扩展的项目骨架与首批基础模块，涵盖：后端服务（Django+DRF+JWT+Channels）、数据采集脚本（Akshare占位）、前端占位页面（ECharts）。

## 目录结构
- `backend/` 后端服务（Django 项目）
- `data_pipeline/` 数据采集与预处理脚本（APSheduler 占位）
- `frontend/` 前端占位页（ECharts，可后续切换到 React+Vite）
- `doc/` 设计文档（原始）

## 快速开始
1) 后端依赖安装（建议 Python 3.10+）
- Windows PowerShell：
```
cd d:\stockdata\backend
python -m venv venv
venv\Scripts\Activate.ps1
pip install -r requirements.txt
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```
2) 前端预览（静态占位页）
```
cd d:\stockdata\frontend
python -m http.server 5500
```
访问：`http://localhost:5500/`

3) 数据采集示例运行
```
cd d:\stockdata\data_pipeline
venv\Scripts\Activate.ps1  # 如已在后端 venv 中可复用环境
python collector.py
```

## 设计对齐
- 用户层：后续将增加 RBAC 权限，当前提供 JWT 占位配置。
- 前端层：先提供 ECharts 静态占位页，后续迁移到 React + WebSocket 实时推送。
- 后端服务层：Django + DRF + SimpleJWT；Channels 作为 WebSocket 能力入口；Redis 用于缓存（后续接入）。
- 数据处理层：`data_pipeline/collector.py` 提供采集/清洗/指标计算占位。
- 数据存储层：默认 SQLite 便于快速跑通；后续切换到 PostgreSQL/TimescaleDB（时序）+ MongoDB（基础信息/用户）+ Redis（缓存）。
- 数据源层：Akshare/Tushare 可插拔；付费数据源仅保留界面入口，不做实现。

## 下一步建议
- 切换数据库到 PostgreSQL + TimescaleDB，接入 `stock_quote` 时序表。
- 接入 Redis 缓存与 WebSocket 实时推送。
- 前端迁移到 React + Vite，完成查询筛选、指标面板与个性化功能界面。
- 增加单元测试与集成测试（后端 API、数据任务）。

## 免责声明
当前为最小可运行骨架，未包含生产环境安全与监控项（请按需增强）。