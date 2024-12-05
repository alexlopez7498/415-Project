import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("MongoDB CarCrash Analysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2') \
    .getOrCreate()

# Read data from MongoDB into a Spark DataFrame
df = spark.read.format("mongo").load()

# Group by DRIVER_SEX, DRIVER_LICENSE_STATUS, and DRIVER_LICENSE_JURISDICTION
# and calculate the sum of crashes for each demographic group
demographic_crash_counts = df.groupBy("DRIVER_SEX", "DRIVER_LICENSE_STATUS", "DRIVER_LICENSE_JURISDICTION") \
    .agg(count("*").alias("Crash_Count"))

# Sort results by crash count in descending order for easy analysis
demographic_crash_counts = demographic_crash_counts.orderBy(col("Crash_Count").desc())

# Limit to the top 50 groups
top_50_crash_counts = demographic_crash_counts.limit(50)

# Collect the results as a list of rows
results = top_50_crash_counts.collect()

# Format results as a readable string
result_str = "\n".join(
    [f"{row['DRIVER_SEX']}, {row['DRIVER_LICENSE_STATUS']}, "
     f"{row['DRIVER_LICENSE_JURISDICTION']}, {row['Crash_Count']}"
     for row in results]
)

# Create the bar plot
labels = [
    f"{row['DRIVER_SEX']}, {row['DRIVER_LICENSE_STATUS']}, {row['DRIVER_LICENSE_JURISDICTION']}"
    for row in results
]
crash_counts = [row['Crash_Count'] for row in results]

plt.figure(figsize=(20, 12))  # Adjust figure size for readability
plt.bar(labels, crash_counts, color='blue', width=0.8)  # Only the top 50 are plotted
plt.xlabel('Driver Demographics (Sex, License Status, Jurisdiction)')
plt.ylabel('Crash Count')
plt.title('Top 50 Crash Counts by Driver Demographics')
plt.xticks( rotation=90)
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig("top_50_demographic_crash_count.png")

# Print detailed results for the GUI to capture
print("Detailed Results: Driver Sex, License Status, License Jurisdiction, Crashes")
print(result_str)

plt.show(block=False)

# Keep the plot window open
plt.show(block=True)
