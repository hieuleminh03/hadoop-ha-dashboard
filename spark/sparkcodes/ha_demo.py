#!/usr/bin/env python3
"""
Hadoop HA Demo - Simple File Processing with Spark
This demo showcases Hadoop High Availability during Spark job execution.

The job is designed to run for approximately 5 minutes to allow
for NameNode and ResourceManager failover demonstrations.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import sys

def create_spark_session():
    """Create Spark session configured for YARN cluster"""
    print("🚀 Creating Spark session...")
    return SparkSession.builder \
        .appName("Hadoop-HA-Demo") \
        .master("yarn") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "2") \
        .getOrCreate()

def process_sales_data(spark):
    """Process sales data with aggregations"""
    print("📊 Processing sales data...")
    
    # Read sales data from HDFS
    sales_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/demo/data/sales_data.csv")
    
    print(f"   ✓ Loaded {sales_df.count()} sales records")
    
    # Add strategic delay for demo timing
    time.sleep(30)
    
    # Calculate daily sales by region
    daily_sales = sales_df.groupBy("date", "region") \
        .agg(
            sum("quantity").alias("total_quantity"),
            round(sum(col("quantity") * col("price")), 2).alias("total_revenue"),
            count("*").alias("transaction_count")
        ) \
        .orderBy("date", "region")
    
    # Cache for multiple operations
    daily_sales.cache()
    
    print(f"   ✓ Calculated daily sales aggregations")
    
    # Write daily sales summary
    daily_sales.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/demo/output/daily_sales_summary")
    
    print("   ✓ Saved daily sales summary to HDFS")
    
    return daily_sales

def process_user_events(spark):
    """Process user events data"""
    print("👥 Processing user events...")
    
    # Add delay to allow for ResourceManager failover demo
    time.sleep(30)
    
    # Read user events
    events_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv("/demo/data/user_events.csv")
    
    print(f"   ✓ Loaded {events_df.count()} user events")
    
    # Analyze user behavior by device type
    device_analysis = events_df.groupBy("device_type", "event_type") \
        .agg(
            count("*").alias("event_count"),
            countDistinct("user_id").alias("unique_users"),
            countDistinct("session_id").alias("unique_sessions")
        ) \
        .orderBy("device_type", "event_type")
    
    # Cache the results
    device_analysis.cache()
    
    print("   ✓ Analyzed user behavior by device")
    
    # Write device analysis
    device_analysis.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/demo/output/device_analysis")
    
    print("   ✓ Saved device analysis to HDFS")
    
    return device_analysis

def generate_summary_report(spark, daily_sales, device_analysis):
    """Generate a combined summary report"""
    print("📈 Generating summary report...")
    
    # Add final delay for demo completion
    time.sleep(20)
    
    # Calculate total metrics
    total_sales = daily_sales.agg(
        sum("total_quantity").alias("grand_total_quantity"),
        sum("total_revenue").alias("grand_total_revenue"),
        count("*").alias("total_daily_records")
    ).collect()[0]
    
    total_events = device_analysis.agg(
        sum("event_count").alias("grand_total_events"),
        sum("unique_users").alias("total_unique_users")
    ).collect()[0]
    
    # Create summary data
    summary_data = [
        ("total_sales_quantity", total_sales["grand_total_quantity"]),
        ("total_sales_revenue", total_sales["grand_total_revenue"]),
        ("total_user_events", total_events["grand_total_events"]),
        ("total_unique_users", total_events["total_unique_users"]),
        ("processing_timestamp", str(int(time.time())))
    ]
    
    summary_schema = StructType([
        StructField("metric", StringType(), True),
        StructField("value", StringType(), True)
    ])
    
    summary_df = spark.createDataFrame(summary_data, summary_schema)
    
    # Write summary report
    summary_df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv("/demo/output/summary_report")
    
    print("   ✓ Generated and saved summary report")
    
    return summary_df

def main():
    """Main execution function"""
    start_time = time.time()
    
    print("=" * 60)
    print("🔥 HADOOP HIGH AVAILABILITY DEMO - SPARK EDITION")
    print("=" * 60)
    print("📋 This demo will:")
    print("   • Process multiple data files from HDFS")
    print("   • Perform data transformations and aggregations")
    print("   • Write results back to HDFS")
    print("   • Run for ~5 minutes to allow HA failover testing")
    print("=" * 60)
    
    try:
        # Create Spark session
        spark = create_spark_session()
        spark.sparkContext.setLogLevel("WARN")
        
        print(f"✅ Spark session created successfully")
        print(f"   Application ID: {spark.sparkContext.applicationId}")
        print(f"   Master: {spark.sparkContext.master}")
        print("-" * 60)
        
        # Process sales data (first 2 minutes)
        print("🎯 Phase 1: Sales Data Processing (Perfect time for NameNode failover!)")
        daily_sales = process_sales_data(spark)
        print("-" * 60)
        
        # Process user events (next 2 minutes)
        print("🎯 Phase 2: User Events Processing (Perfect time for ResourceManager failover!)")
        device_analysis = process_user_events(spark)
        print("-" * 60)
        
        # Generate summary (final minute)
        print("🎯 Phase 3: Summary Report Generation")
        summary_report = generate_summary_report(spark, daily_sales, device_analysis)
        print("-" * 60)
        
        # Display final results
        print("🏆 DEMO COMPLETED SUCCESSFULLY!")
        print("📊 Results Summary:")
        summary_report.show()
        
        elapsed_time = time.time() - start_time
        print(f"⏱️  Total execution time: {elapsed_time:.2f} seconds")
        print("💪 Hadoop HA survived all operations!")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error during demo execution: {str(e)}")
        sys.exit(1)
    
    finally:
        if 'spark' in locals():
            spark.stop()
            print("🛑 Spark session stopped")

if __name__ == "__main__":
    main()
