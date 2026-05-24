#!/bin/bash
# 微博爬虫启动脚本 (Linux/Mac)

# 设置环境变量
export PYTHONPATH=.

# 启动爬虫
echo "=== 启动微博爬虫 ==="
python3 crawler/weibo_crawler.py