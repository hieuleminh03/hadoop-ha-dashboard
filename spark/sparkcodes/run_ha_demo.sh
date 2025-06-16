#!/bin/bash

# Hadoop HA Demo - Quick Start Script
# This script sets up and runs the Spark HA demonstration

echo "==============================================="
echo "🔥 HADOOP HA DEMO - SPARK EDITION"
echo "==============================================="

# Check if Spark is available
if ! command -v spark-submit &> /dev/null; then
    echo "❌ Error: spark-submit not found in PATH"
    echo "   Please ensure Spark is properly installed and configured"
    exit 1
fi

# Create demo directories in HDFS
echo "📁 Setting up HDFS directories..."
hdfs dfs -mkdir -p /demo/data
hdfs dfs -mkdir -p /demo/output

# Upload sample data to HDFS
echo "📤 Uploading demo data to HDFS..."
hdfs dfs -put -f data/sales_data.csv /demo/data/
hdfs dfs -put -f data/user_events.csv /demo/data/

echo "✅ HDFS setup completed"

# Verify data upload
echo "📂 Verifying HDFS contents:"
hdfs dfs -ls /demo/data/

echo ""
echo "🚀 Starting Hadoop HA Demo..."
echo ""
echo "📋 DEMO INSTRUCTIONS:"
echo "   1. The job will run for approximately 5 minutes"
echo "   2. During Phase 1 (~1-2 minutes): Trigger NameNode failover via dashboard"
echo "   3. During Phase 2 (~3-4 minutes): Trigger ResourceManager failover via dashboard"
echo "   4. Watch the job complete successfully despite failovers!"
echo ""
echo "🎯 Dashboard URL: http://localhost:3000"
echo ""

# Run the Spark demo
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 2 \
    --executor-memory 1g \
    --executor-cores 1 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    ha_demo.py

echo ""
echo "🏆 Demo completed! Check results in HDFS:"
echo "   hdfs dfs -ls /demo/output/"
echo ""
echo "📊 View results:"
echo "   hdfs dfs -cat /demo/output/summary_report/part-*.csv"
echo ""
echo "🎉 Hadoop HA successfully demonstrated!"
