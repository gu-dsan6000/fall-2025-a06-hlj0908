from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, rand
import os
import shutil

def write_single_csv(df, output_path):
    """Write DataFrame as a single CSV file (with header)."""
    tmp_dir = output_path + "_tmp"
    df.coalesce(1).write.option("header", True).mode("overwrite").csv(tmp_dir)
    # find the part file and move it
    for root, _, files in os.walk(tmp_dir):
        for f in files:
            if f.startswith("part-") and f.endswith(".csv"):
                shutil.move(os.path.join(root, f), output_path)
    shutil.rmtree(tmp_dir)

def main():
    spark = SparkSession.builder.appName("Problem1_LogLevelDistribution").getOrCreate()

    input_path = "data/raw"  
    output_dir = "data/output/"
    os.makedirs(output_dir, exist_ok=True)

    # read logs recursively
    logs_df = spark.read.option("recursiveFileLookup", "true").text(input_path)

    # extract log levels
    pattern = r"(INFO|WARN|ERROR|DEBUG)"
    logs_df = logs_df.withColumn("log_level", regexp_extract(col("value"), pattern, 1))
    logs_with_level = logs_df.filter(col("log_level") != "")

    # (1) count occurrences
    counts_df = logs_with_level.groupBy("log_level").count().orderBy("log_level")
    write_single_csv(counts_df, os.path.join(output_dir, "problem1_counts.csv"))

    # (2) sampling
    sample_df = logs_with_level.orderBy(rand()).limit(10)
    sample_df = sample_df.select(col("value").alias("log_entry"), col("log_level"))
    write_single_csv(sample_df, os.path.join(output_dir, "problem1_sample.csv"))

    # (3) summary information
    total_lines = logs_df.count()
    total_with_level = logs_with_level.count()
    counts = counts_df.collect()

    summary_lines = [
        f"Total log lines processed: {total_lines}",
        f"Total lines with log levels: {total_with_level}",
        f"Unique log levels found: {len(counts)}",
        "",
        "Log level distribution:"
    ]
    total = sum([r["count"] for r in counts])
    for r in counts:
        lvl, cnt = r["log_level"], r["count"]
        pct = cnt / total * 100 if total > 0 else 0
        summary_lines.append(f"  {lvl:<6}: {cnt:>10,} ({pct:6.2f}%)")

    with open(os.path.join(output_dir, "problem1_summary.txt"), "w") as f:
        f.write("\n".join(summary_lines))

    spark.stop()

if __name__ == "__main__":
    main()

