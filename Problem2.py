import os
import argparse
import shutil
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, col, concat, lit,
    try_to_timestamp, when, min as spark_min, max as spark_max, countDistinct
)


def write_single_csv_spark(df, path):
    """Save Spark DataFrame as a single CSV file."""
    tmp = path + "_tmp"
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp)
    part = None
    for f in os.listdir(tmp):
        if f.startswith("part-") and f.endswith(".csv"):
            part = f
            break
    if part is None:
        raise RuntimeError(f"No CSV part found in {tmp}")
    if os.path.exists(path):
        os.remove(path)
    shutil.move(os.path.join(tmp, part), path)
    shutil.rmtree(tmp, ignore_errors=True)


def run_spark(master, input_dir, output_dir):
    print(f"[INFO] Using Spark master: {master}")
    print(f"[INFO] Reading logs recursively from: {input_dir}")

    spark = (
        SparkSession.builder
        .appName("Problem2_ClusterUsageAnalysis")
        .master(master)
        .getOrCreate()
    )

    # 递归读取所有日志文件
    logs = (
        spark.read
        .option("recursiveFileLookup", "true")
        .text(input_dir)
        .withColumn("path", input_file_name())
    )

    # 提取 cluster_id, app_number
    logs = logs.withColumn("cluster_id", regexp_extract(col("path"), r"application_(\d+)_", 1))
    logs = logs.withColumn("app_number", regexp_extract(col("path"), r"application_\d+_(\d+)", 1))
    logs = logs.withColumn(
        "application_id",
        concat(lit("application_"), col("cluster_id"), lit("_"), col("app_number"))
    )

    # 提取日志时间戳 (支持 yy/MM/dd HH:mm:ss)
    logs = logs.withColumn(
        "timestamp_raw",
        regexp_extract(col("value"), r"(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    )

    # 解析时间戳
    logs = logs.withColumn("ts", try_to_timestamp(col("timestamp_raw"), lit("yy/MM/dd HH:mm:ss")))

    # 过滤掉解析失败的行
    logs = logs.filter(
        (col("ts").isNotNull()) &
        (col("cluster_id") != "") &
        (col("app_number") != "")
    )

    # 统计每个 application 的起止时间
    app_times = (
        logs.groupBy("cluster_id", "application_id", "app_number")
            .agg(
                spark_min("ts").alias("start_time"),
                spark_max("ts").alias("end_time")
            )
            .orderBy(col("cluster_id"), col("app_number").cast("int"))
    )

    # 每个集群的汇总统计
    cluster_summary = (
        app_times.groupBy("cluster_id")
                 .agg(
                     countDistinct("application_id").alias("num_applications"),
                     spark_min("start_time").alias("cluster_first_app"),
                     spark_max("end_time").alias("cluster_last_app")
                 )
                 .orderBy(col("num_applications").desc())
    )

    # 输出目录
    os.makedirs(output_dir, exist_ok=True)
    timeline_csv = os.path.join(output_dir, "problem2_timeline.csv")
    summary_csv = os.path.join(output_dir, "problem2_cluster_summary.csv")
    stats_txt = os.path.join(output_dir, "problem2_stats.txt")
    bar_png = os.path.join(output_dir, "problem2_bar_chart.png")

    write_single_csv_spark(app_times, timeline_csv)
    print(f"[INFO] Saved timeline: {timeline_csv}")

    write_single_csv_spark(cluster_summary, summary_csv)
    print(f"[INFO] Saved cluster summary: {summary_csv}")

    # 写入 stats
    summary_pd = cluster_summary.toPandas()
    total_clusters = int(summary_pd.shape[0])
    total_apps = int(summary_pd["num_applications"].sum()) if total_clusters > 0 else 0
    avg_apps = (total_apps / total_clusters) if total_clusters > 0 else 0.0

    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, row in summary_pd.sort_values("num_applications", ascending=False).iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications\n")
    print(f"[INFO] Saved stats: {stats_txt}")

    # 绘图
    if total_clusters > 0:
        plt.figure(figsize=(10, 6))
        plt.bar(summary_pd["cluster_id"].astype(str), summary_pd["num_applications"].astype(int))
        plt.xticks(rotation=30, ha="right")
        plt.xlabel("Cluster ID")
        plt.ylabel("Number of Applications")
        plt.title("Applications per Cluster")
        plt.tight_layout()
        plt.savefig(bar_png)
        print(f"[INFO] Saved bar chart: {bar_png}")
    else:
        print("[WARN] No clusters found; bar chart skipped.")

    spark.stop()


def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master", nargs="?", default="local[*]", help="Spark master URL, e.g. spark://<ip>:7077 or local[*]")
    parser.add_argument("--net-id", required=True)
    parser.add_argument("--input", default="data/sample")
    parser.add_argument("--output", default="data/output")
    parser.add_argument("--skip-spark", action="store_true")
    args = parser.parse_args()

    if args.skip_spark:
        print("[INFO] Skipping Spark execution (--skip-spark).")
        return

    run_spark(args.master, args.input, args.output)


if __name__ == "__main__":
    main()




