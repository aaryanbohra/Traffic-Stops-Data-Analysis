from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import pyspark.sql.functions as F
import time
import csv

# Start Spark session
spark = SparkSession.builder \
    .master("local[5]") \
    .appName("TrafficStopTimeAnalysis") \
    .getOrCreate()

start = time.time()

# Load cleaned CSV
df = spark.read.option("header", "true").csv("combined_cleaned_csv/Q5 Cleaned.csv")

# Print schema to verify
df.printSchema()

# Select relevant columns
df = df.select("stop_hour", "subject_race", "state")

# Filter nulls and unknowns
df_clean = df.filter(
    (col("stop_hour").isNotNull()) &
    (col("subject_race").isNotNull()) &
    (~col("subject_race").isin("unknown", "N/A"))
)

# Add 'region' column
df_clean = df_clean.withColumn(
    "region",
    when(col("state").isin("CA", "WA", "OR", "AZ", "TX"), "West Coast")
    .when(col("state").isin("NC", "RI", "VT", "NY", "MA"), "East Coast")
    .otherwise("Other")
)

# Add 'time_of_day' column for better insights
df_clean = df_clean.withColumn(
    "time_of_day",
    when((col("stop_hour") >= 0) & (col("stop_hour") <= 5), "Night")
    .when((col("stop_hour") >= 6) & (col("stop_hour") <= 11), "Morning")
    .when((col("stop_hour") >= 12) & (col("stop_hour") <= 17), "Afternoon")
    .when((col("stop_hour") >= 18) & (col("stop_hour") <= 23), "Evening")
    .otherwise("Unknown")
)

# Preview after adding columns
print("Preview after adding 'region' and 'time_of_day':")
df_clean.show(10)

# Count stops by region, race, and time category
time_category_counts = df_clean.groupBy("region", "subject_race", "time_of_day") \
    .agg(count("*").alias("stop_count"))

# Show for inspection
time_category_counts.orderBy("region", "subject_race", "time_of_day").show(20)

# Save to CSV
time_category_counts.coalesce(1).write.option("header", "true").csv("output/time_of_day_patterns")

# Export to CSV for use in report
rows = time_category_counts.collect()
with open("time_of_day_patterns.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["region", "subject_race", "time_of_day", "stop_count"])
    for row in rows:
        writer.writerow(row)

# End timing
end = time.time()
print("Execution time:", round(end - start, 2), "seconds")

