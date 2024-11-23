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

# Group by VEHICLE_TYPE and VEHICLE_MAKE, then count the number of crashes for each combination
crash_summary = df.groupBy("VEHICLE_TYPE", "VEHICLE_MAKE").agg(
    count("COLLISION_ID").alias("Crash_Count")
)

# Sort by Crash_Count in descending order for better insights
crash_summary = crash_summary.orderBy(col("Crash_Count").desc())

# Validate results
total_rows = df.count()
grouped_count = crash_summary.count()

print(f"Total Rows in Dataset: {total_rows}")
print(f"Number of Groups (VEHICLE_TYPE & VEHICLE_MAKE): {grouped_count}")

# Write the output to a CSV file
crash_summary.write.csv("output/vehicle_crash_summary", header=True)

# Measure end time and calculate execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Stop the Spark session
spark.stop()
