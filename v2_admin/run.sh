#!/bin/bash
# run.sh · 侨联频道发布后台 · 启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 检查 .env
if [ ! -f ".env" ]; then
  echo "❌ 未找到 .env 文件，请先复制 .env.example 并填写配置"
  echo "   cp .env.example .env && nano .env"
  exit 1
fi

# 检查虚拟环境
if [ -d "../venv" ]; then
  source ../venv/bin/activate
elif [ -d "venv" ]; then
  source venv/bin/activate
else
  echo "⚠️  未找到虚拟环境，使用系统 Python"
fi

# 初始化数据库 + 启动
echo "🚀 启动侨联频道发布后台..."
source .env
PORT="${ADMIN_PORT:-5005}"

python -m waitress \
  --host=127.0.0.1 \
  --port="$PORT" \
  --threads=4 \
  admin_server:app
