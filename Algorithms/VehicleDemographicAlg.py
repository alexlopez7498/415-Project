import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import sys

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

# Collect the results as a list of rows
results = demographic_crash_counts.collect()

# Format results as a readable string
result_str = "\n".join(
    [f"{row['DRIVER_SEX']},{row['DRIVER_LICENSE_STATUS']}, "
     f"{row['DRIVER_LICENSE_JURISDICTION']}, {row['Crash_Count']}" 
     for row in results]
)

# Print detailed results for the GUI to capture
print("Detailed Results:Driver Sex, License Status, License Jurisdiction, Crashes")
print(result_str)

# Validate results
total_rows = df.count()
grouped_count = demographic_crash_counts.count()

# Print additional information
print(f"\nTotal Rows in Dataset: {total_rows}")
print(f"Number of Groups (Driver Demographics): {grouped_count}")

# Measure end time and calculate execution time
end_time = time.time()
execution_time = end_time - start_time
print(f"\nExecution Time: {execution_time:.2f} seconds")

# Stop the Spark session
spark.stop()
