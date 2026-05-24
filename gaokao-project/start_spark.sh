#!/bin/bash
# Spark Streaming 启动脚本 (Linux/Mac)

# 检查 JAVA_HOME 是否配置
if [ -z "$JAVA_HOME" ]; then
    echo "警告: JAVA_HOME 环境变量未设置"
    echo "请先配置 JAVA_HOME，示例:"
    echo "  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64"
    echo "  或 export JAVA_HOME=/usr/lib/jvm/java-11-openjdk"
    exit 1
fi

export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# 清除旧的 checkpoint
rm -rf ../spark-checkpoint

# 运行 Spark
echo "=== 启动 Spark Streaming ==="
python3 streaming/spark_consumer.py