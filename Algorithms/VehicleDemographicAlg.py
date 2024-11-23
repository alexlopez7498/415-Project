import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Measure execution time
start_time = time.time()

# Load CSV file
df = spark.read.csv("Motor_Vehicle_Collisions_-_Full.csv", header=True, inferSchema=True)

# Group by DRIVER_SEX, DRIVER_LICENSE_STATUS, and DRIVER_LICENSE_JURISDICTION
# and calculate the sum of crashes for each demographic group
demographic_crash_counts = df.groupBy("DRIVER_SEX", "DRIVER_LICENSE_STATUS", "DRIVER_LICENSE_JURISDICTION") \
    .agg(count("*").alias("Crash_Count"))

# Sort results by crash count in descending order for easy analysis
demographic_crash_counts = demographic_crash_counts.orderBy(col("Crash_Count").desc())

# Validate results
total_rows = df.count()
grouped_count = demographic_crash_counts.count()

print(f"Total Rows in Dataset: {total_rows}")
print(f"Number of Groups (Driver Demographics): {grouped_count}")

# Save the result to a CSV file
demographic_crash_counts.write.csv("output/driver_demographics_crash_counts", header=True)

# Measure end time and calculate execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution Time: {execution_time:.2f} seconds")

# Stop the Spark session
spark.stop()
