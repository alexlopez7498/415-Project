import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder \
    .getOrCreate()

# Measure execution time
start_time = time.time()

# Read the CSV file, inferring schema and headers
df = spark.read.csv("Motor_Vehicle_Collisions_-_Full.csv", header=True, inferSchema=True)

# Filter out years greater than 2024
df_filtered = df.filter(col("VEHICLE_YEAR") <= 2024)

# Group by VEHICLE_YEAR and count the number of crashes for each year
yearly_crash_summary = df_filtered.groupBy("VEHICLE_YEAR").agg(
    count("COLLISION_ID").alias("Crash_Count")
)

# Sort by Crash_Count in descending order
yearly_crash_summary = yearly_crash_summary.orderBy(col("Crash_Count").desc())

# Validate results
filtered_count = df_filtered.count()
grouped_count = yearly_crash_summary.count()

print(f"Filtered Rows (years <= 2024): {filtered_count}")
print(f"Number of Groups (VEHICLE_YEAR): {grouped_count}")

# Write the output to a CSV file
yearly_crash_summary.write.csv("output/vehicle_year_crash_summary", header=True)

# Measure end time and calculate execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Stop the Spark session
spark.stop()
