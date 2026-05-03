#!/bin/bash

# 股票数据管理系统启动脚本
# 作者：AI Assistant
# 日期：2026-04-28
# 功能：在终端中启动前端、后端和定时任务，显示实时日志

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# 项目根目录（脚本位于 scripts/ 目录下，项目根目录是其父目录）
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BACKEND_DIR="$PROJECT_DIR/backend"
FRONTEND_DIR="$PROJECT_DIR/frontend"
VENV_DIR="$BACKEND_DIR/venv"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# 检查项目目录结构是否正确
check_project_structure() {
    if [ ! -d "$BACKEND_DIR" ] || [ ! -d "$FRONTEND_DIR" ]; then
        log_error "项目目录结构不正确，请确保脚本在股票数据管理系统根目录下执行"
        log_error "正确的目录结构应该包含 backend/ 和 frontend/ 文件夹"
        exit 1
    fi
}

# 检查虚拟环境是否存在
check_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        log_info "创建 Python 虚拟环境..."
        cd "$BACKEND_DIR" || exit 1
        python3 -m venv venv
        if [ $? -ne 0 ]; then
            log_error "创建虚拟环境失败"
            exit 1
        fi
        log_info "虚拟环境创建成功"
    fi
}

# 激活虚拟环境
activate_venv() {
    source "$VENV_DIR/bin/activate"
}

# 检查端口是否被占用
check_port() {
    local port="$1"
    local description="$2"
    
    if lsof -ti :$port >/dev/null 2>&1; then
        log_warning "端口 $port 已被占用（$description），正在清理..."
        local pids=$(lsof -ti :$port)
        for pid in $pids; do
            log_warning "正在终止进程 $pid"
            kill -9 $pid >/dev/null 2>&1
        done
    fi
}

# 启动后端服务
start_backend() {
    log_info "启动后端服务..."
    
    activate_venv
    
    cd "$BACKEND_DIR" || exit 1
    
    python manage.py runserver 0.0.0.0:8000
}

# 启动前端服务
start_frontend() {
    log_info "启动前端服务..."
    
    cd "$FRONTEND_DIR" || exit 1
    
    python3 -m http.server 5500
}

# 启动后端定时任务
start_scheduler() {
    log_info "启动后端定时任务..."
    
    activate_venv
    
    cd "$BACKEND_DIR" || exit 1
    
    python manage.py qcluster
}

# 停止所有服务
stop_services() {
    log_info "停止所有服务..."
    
    check_port 8000 "后端服务"
    check_port 5500 "前端服务"
    
    # 停止定时任务调度器
    if pgrep -f "python manage.py qcluster" >/dev/null 2>&1; then
        log_info "停止定时任务调度器..."
        pkill -f "python manage.py qcluster" >/dev/null 2>&1
        sleep 1
    fi
    
    log_info "所有服务已停止"
}

# 显示帮助信息
show_help() {
    echo "股票数据管理系统启动脚本"
    echo "用法: $0 [命令]"
    echo ""
    echo "命令列表:"
    echo "  start     启动前端、后端和定时任务（在新的终端窗口中）"
    echo "  stop      停止所有服务"
    echo "  status    检查服务状态"
    echo "  help      显示此帮助信息"
    echo ""
    echo "单独启动命令:"
    echo "  start_backend    启动后端服务"
    echo "  start_frontend   启动前端服务"
    echo "  start_scheduler  启动定时任务调度器"
    echo ""
    echo "单独停止命令:"
    echo "  stop_backend     停止后端服务"
    echo "  stop_frontend    停止前端服务"
    echo "  stop_scheduler   停止定时任务调度器"
    echo ""
    echo "示例:"
    echo "  $0 start                # 启动所有服务"
    echo "  $0 stop                 # 停止所有服务"
    echo "  $0 start_backend        # 只启动后端服务"
    echo "  $0 stop_frontend        # 只停止前端服务"
    echo "  $0 start_scheduler      # 只启动定时任务调度器"
}

# 检查服务状态
check_services() {
    log_info "检查服务状态..."
    
    if lsof -ti :8000 >/dev/null 2>&1; then
        log_info "后端服务正在运行（端口 8000）"
    else
        log_warning "后端服务未运行（端口 8000）"
    fi
    
    if lsof -ti :5500 >/dev/null 2>&1; then
        log_info "前端服务正在运行（端口 5500）"
    else
        log_warning "前端服务未运行（端口 5500）"
    fi
    
    if pgrep -f "python manage.py qcluster" >/dev/null 2>&1; then
        log_info "定时任务调度器正在运行"
    else
        log_warning "定时任务调度器未运行"
    fi
}

# 停止后端服务
stop_backend() {
    log_info "停止后端服务..."
    
    check_port 8000 "后端服务"
}

# 停止前端服务
stop_frontend() {
    log_info "停止前端服务..."
    
    check_port 5500 "前端服务"
}

# 停止定时任务调度器
stop_scheduler() {
    log_info "停止定时任务调度器..."
    
    if pgrep -f "python manage.py qcluster" >/dev/null 2>&1; then
        local pids=$(pgrep -f "python manage.py qcluster")
        for pid in $pids; do
            log_warning "正在终止进程 $pid"
            kill -9 $pid >/dev/null 2>&1
        done
        sleep 1
        log_info "定时任务调度器已停止"
    else
        log_warning "定时任务调度器未运行"
    fi
}

# 单独启动后端服务
start_backend_service() {
    log_info "开始启动后端服务..."
    
    check_venv
    
    # 检查并清理端口
    check_port 8000 "后端服务"
    
    # 启动虚拟环境并安装依赖（如果需要）
    activate_venv
    log_info "检查项目依赖..."
    pip install --upgrade pip >/dev/null 2>&1
    pip install -r "$BACKEND_DIR/requirements.txt" >/dev/null 2>&1
    
    log_info "初始化数据库..."
    python "$BACKEND_DIR/manage.py" migrate >/dev/null 2>&1
    
    log_info "后端服务将在新的终端窗口中启动..."
    osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_backend"' >/dev/null 2>&1
    
    log_info "后端服务启动完成！"
    log_info "后端地址: http://localhost:8000"
}

# 单独启动前端服务
start_frontend_service() {
    log_info "开始启动前端服务..."
    
    # 检查并清理端口
    check_port 5500 "前端服务"
    
    log_info "前端服务将在新的终端窗口中启动..."
    osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_frontend"' >/dev/null 2>&1
    
    log_info "前端服务启动完成！"
    log_info "前端地址: http://localhost:5500"
}

# 单独启动定时任务调度器
start_scheduler_service() {
    log_info "开始启动定时任务调度器..."
    
    check_venv
    
    log_info "定时任务调度器将在新的终端窗口中启动..."
    osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_scheduler"' >/dev/null 2>&1
    
    log_info "定时任务调度器启动完成！"
}

# 主函数
main() {
    # 检查项目目录结构
    check_project_structure
    
    if ! command_exists python3; then
        log_error "Python 3 未找到，请先安装 Python 3"
        exit 1
    fi
    
    case "$1" in
        start)
            log_info "开始启动股票数据管理系统..."
            
            check_venv
            
            # 检查并清理端口
            check_port 8000 "后端服务"
            check_port 5500 "前端服务"
            
            # 启动虚拟环境并安装依赖（如果需要）
            activate_venv
            log_info "检查项目依赖..."
            pip install --upgrade pip >/dev/null 2>&1
            pip install -r "$BACKEND_DIR/requirements.txt" >/dev/null 2>&1
            
            log_info "初始化数据库..."
            python "$BACKEND_DIR/manage.py" migrate >/dev/null 2>&1
            
            log_info "所有服务将在新的终端窗口中启动..."
            
            # 在新窗口中启动后端服务
            log_info "在新窗口中启动后端服务..."
            osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_backend"' >/dev/null 2>&1
            
            # 在新窗口中启动前端服务
            log_info "在新窗口中启动前端服务..."
            osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_frontend"' >/dev/null 2>&1
            
            # 在新窗口中启动定时任务调度器
            log_info "在新窗口中启动定时任务调度器..."
            osascript -e 'tell application "Terminal" to do script "cd /Users/nan/git/stockdata/scripts && ./start_services_with_logs.sh run_scheduler"' >/dev/null 2>&1
            
            log_info "系统启动完成！"
            log_info "前端地址: http://localhost:5500"
            log_info "后端地址: http://localhost:8000"
            ;;
            
        run_backend)
            check_venv
            check_port 8000 "后端服务"
            start_backend
            ;;
            
        run_frontend)
            check_port 5500 "前端服务"
            start_frontend
            ;;
            
        run_scheduler)
            check_venv
            start_scheduler
            ;;
            
        start_backend)
            start_backend_service
            ;;
            
        start_frontend)
            start_frontend_service
            ;;
            
        start_scheduler)
            start_scheduler_service
            ;;
            
        stop)
            stop_services
            ;;
            
        stop_backend)
            stop_backend
            ;;
            
        stop_frontend)
            stop_frontend
            ;;
            
        stop_scheduler)
            stop_scheduler
            ;;
            
        status)
            check_services
            ;;
            
        help)
            show_help
            ;;
            
        *)
            log_error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

if [ ! -x "$0" ]; then
    log_warning "脚本缺少执行权限，正在添加..."
    chmod +x "$0"
fi

trap stop_services SIGINT SIGTERM

main "$@"
