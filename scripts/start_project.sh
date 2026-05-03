#!/bin/bash

# 股票数据管理系统启动脚本
# 作者：AI Assistant
# 日期：2026-04-25

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
    log_info "激活虚拟环境..."
    source "$VENV_DIR/bin/activate"
}

# 安装依赖
install_dependencies() {
    log_info "检查并安装后端依赖..."
    
    activate_venv
    
    pip install --upgrade pip
    
    pip install -r "$BACKEND_DIR/requirements.txt"
    if [ $? -ne 0 ]; then
        log_error "安装依赖失败"
        exit 1
    fi
    
    log_info "依赖安装成功"
}

# 初始化数据库
init_database() {
    log_info "初始化数据库..."
    
    activate_venv
    
    cd "$BACKEND_DIR" || exit 1
    
    python manage.py migrate
    if [ $? -ne 0 ]; then
        log_error "数据库迁移失败"
        exit 1
    fi
    
    log_info "数据库初始化成功"
}

# 启动后端服务
start_backend() {
    log_info "启动后端服务..."
    
    activate_venv
    
    cd "$BACKEND_DIR" || exit 1
    
    python manage.py runserver 0.0.0.0:8000 &
    BACKEND_PID=$!
    echo $BACKEND_PID > "$PROJECT_DIR/backend.pid"
    
    log_info "后端服务启动成功，PID: $BACKEND_PID"
}

# 启动前端服务
start_frontend() {
    log_info "启动前端服务..."
    
    cd "$FRONTEND_DIR" || exit 1
    
    python3 -m http.server 5500 &
    FRONTEND_PID=$!
    echo $FRONTEND_PID > "$PROJECT_DIR/frontend.pid"
    
    log_info "前端服务启动成功，PID: $FRONTEND_PID"
}

# 启动后端定时任务
start_scheduler() {
    log_info "启动后端定时任务..."
    
    activate_venv
    
    cd "$BACKEND_DIR" || exit 1
    
    python manage.py qcluster &
    SCHEDULER_PID=$!
    echo $SCHEDULER_PID > "$PROJECT_DIR/scheduler.pid"
    
    log_info "定时任务调度器启动成功，PID: $SCHEDULER_PID"
}

# 停止所有服务
stop_services() {
    log_info "停止所有服务..."
    
    if [ -f "$PROJECT_DIR/backend.pid" ]; then
        BACKEND_PID=$(cat "$PROJECT_DIR/backend.pid")
        if kill -0 $BACKEND_PID 2>/dev/null; then
            kill $BACKEND_PID 2>/dev/null
            log_info "后端服务已停止"
        fi
        rm -f "$PROJECT_DIR/backend.pid"
    fi
    
    if [ -f "$PROJECT_DIR/frontend.pid" ]; then
        FRONTEND_PID=$(cat "$PROJECT_DIR/frontend.pid")
        if kill -0 $FRONTEND_PID 2>/dev/null; then
            kill $FRONTEND_PID 2>/dev/null
            log_info "前端服务已停止"
        fi
        rm -f "$PROJECT_DIR/frontend.pid"
    fi
    
    if [ -f "$PROJECT_DIR/scheduler.pid" ]; then
        SCHEDULER_PID=$(cat "$PROJECT_DIR/scheduler.pid")
        if kill -0 $SCHEDULER_PID 2>/dev/null; then
            kill $SCHEDULER_PID 2>/dev/null
            log_info "定时任务调度器已停止"
        fi
        rm -f "$PROJECT_DIR/scheduler.pid"
    fi
    
    pkill -f "python manage.py runserver" 2>/dev/null
    pkill -f "python manage.py qcluster" 2>/dev/null
    pkill -f "python3 -m http.server" 2>/dev/null
    
    log_info "所有服务已停止"
}

# 检查服务是否正在运行
check_services() {
    log_info "检查服务状态..."
    
    if [ -f "$PROJECT_DIR/backend.pid" ]; then
        BACKEND_PID=$(cat "$PROJECT_DIR/backend.pid")
        if kill -0 $BACKEND_PID 2>/dev/null; then
            log_info "后端服务正在运行，PID: $BACKEND_PID"
        else
            log_warning "后端服务 PID 文件存在但进程不存在"
            rm -f "$PROJECT_DIR/backend.pid"
        fi
    else
        log_warning "后端服务未运行"
    fi
    
    if [ -f "$PROJECT_DIR/frontend.pid" ]; then
        FRONTEND_PID=$(cat "$PROJECT_DIR/frontend.pid")
        if kill -0 $FRONTEND_PID 2>/dev/null; then
            log_info "前端服务正在运行，PID: $FRONTEND_PID"
        else
            log_warning "前端服务 PID 文件存在但进程不存在"
            rm -f "$PROJECT_DIR/frontend.pid"
        fi
    else
        log_warning "前端服务未运行"
    fi
    
    if [ -f "$PROJECT_DIR/scheduler.pid" ]; then
        SCHEDULER_PID=$(cat "$PROJECT_DIR/scheduler.pid")
        if kill -0 $SCHEDULER_PID 2>/dev/null; then
            log_info "定时任务调度器正在运行，PID: $SCHEDULER_PID"
        else
            log_warning "定时任务调度器 PID 文件存在但进程不存在"
            rm -f "$PROJECT_DIR/scheduler.pid"
        fi
    else
        log_warning "定时任务调度器未运行"
    fi
}

# 显示帮助信息
show_help() {
    echo "股票数据管理系统启动脚本"
    echo "用法: $0 [命令]"
    echo ""
    echo "命令列表:"
    echo "  start     启动前端、后端和定时任务"
    echo "  stop      停止所有服务"
    echo "  restart   重启所有服务"
    echo "  status    检查服务状态"
    echo "  install   安装项目依赖"
    echo "  init      初始化数据库"
    echo "  help      显示此帮助信息"
    echo ""
    echo "注意: 脚本会自动检测项目目录结构，确保脚本位于项目根目录下"
    echo ""
    echo "示例:"
    echo "  $0 start    # 启动所有服务"
    echo "  $0 stop     # 停止所有服务"
}

# 主函数
main() {
    # 检查项目目录结构
    check_project_structure
    
    if ! command_exists python3; then
        log_error "Python 3 未找到，请先安装 Python 3"
        exit 1
    fi
    
    # 如果没有参数，显示 SwiftBar 菜单格式
    if [ -z "$1" ]; then
        # SwiftBar 插件格式：
        # 第一行是菜单标题
        # 后续每行是菜单选项，格式："显示文本 | bash=脚本路径 param1=值 param2=值"
        # 空行表示分隔线
        
        # 检查服务状态
        echo "股票数据管理系统"
        echo "---"
        
        if [ -f "$PROJECT_DIR/backend.pid" ]; then
            BACKEND_PID=$(cat "$PROJECT_DIR/backend.pid")
            if kill -0 $BACKEND_PID 2>/dev/null; then
                echo "后端服务运行中 (PID: $BACKEND_PID) | color=green"
            else
                echo "后端服务 PID 文件存在但进程不存在 | color=red"
                rm -f "$PROJECT_DIR/backend.pid"
            fi
        else
            echo "后端服务未运行 | color=red"
        fi
        
        if [ -f "$PROJECT_DIR/frontend.pid" ]; then
            FRONTEND_PID=$(cat "$PROJECT_DIR/frontend.pid")
            if kill -0 $FRONTEND_PID 2>/dev/null; then
                echo "前端服务运行中 (PID: $FRONTEND_PID) | color=green"
            else
                echo "前端服务 PID 文件存在但进程不存在 | color=red"
                rm -f "$PROJECT_DIR/frontend.pid"
            fi
        else
            echo "前端服务未运行 | color=red"
        fi
        
        if [ -f "$PROJECT_DIR/scheduler.pid" ]; then
            SCHEDULER_PID=$(cat "$PROJECT_DIR/scheduler.pid")
            if kill -0 $SCHEDULER_PID 2>/dev/null; then
                echo "定时任务运行中 (PID: $SCHEDULER_PID) | color=green"
            else
                echo "定时任务 PID 文件存在但进程不存在 | color=red"
                rm -f "$PROJECT_DIR/scheduler.pid"
            fi
        else
            echo "定时任务未运行 | color=red"
        fi
        
        echo "---"
        echo "启动系统 | bash=$0 param1=start terminal=false refresh=true"
        echo "停止系统 | bash=$0 param1=stop terminal=false refresh=true"
        echo "重启系统 | bash=$0 param1=restart terminal=false refresh=true"
        echo "---"
        echo "检查服务状态 | bash=$0 param1=status terminal=true"
        echo "安装依赖 | bash=$0 param1=install terminal=true"
        echo "初始化数据库 | bash=$0 param1=init terminal=true"
        echo "---"
        echo "显示帮助 | bash=$0 param1=help terminal=true"
        
        exit 0
    fi
    
    case "$1" in
        start)
            log_info "开始启动股票数据管理系统..."
            
            check_venv
            
            install_dependencies
            
            init_database
            
            start_backend
            start_frontend
            start_scheduler
            
            log_info "系统启动完成！"
            log_info "前端地址: http://localhost:5500"
            log_info "后端地址: http://localhost:8000"
            ;;
            
        stop)
            stop_services
            ;;
            
        restart)
            stop_services
            sleep 2
            main start
            ;;
            
        status)
            check_services
            ;;
            
        install)
            check_venv
            install_dependencies
            ;;
            
        init)
            check_venv
            install_dependencies
            init_database
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
