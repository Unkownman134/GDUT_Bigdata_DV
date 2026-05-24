#!/bin/bash
# Streamlit Dashboard 启动脚本 (Linux/Mac)

# 设置环境变量
export PYTHONPATH=.
export PATH=$PATH:$HOME/.local/bin

# 启动 Streamlit
echo "=== 启动 Dashboard ==="
streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0