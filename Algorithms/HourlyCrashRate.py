from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("MongoDB CarCrash Analysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2') \
    .getOrCreate()

# Read data from MongoDB into a Spark DataFrame
df = spark.read.format("mongo").load()

# Ensure CRASH TIME is in the correct format and extract the hour
df = df.withColumn("HOUR", F.hour(F.to_timestamp(F.col("CRASH TIME"), "H:mm")))

# Group by HOUR and count the number of crashes
hourly_crashes = df.groupBy("HOUR").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by HOUR for better readability
hourly_crashes = hourly_crashes.orderBy("HOUR")
results = hourly_crashes.collect()

# Prepare data for plotting
Hours = [row['HOUR'] for row in results]
crash_counts = [row['CRASH_COUNT'] for row in results]

# Create the bar plot
plt.figure(figsize=(16, 10))  # Increase the width and height
bar_width = 0.8  # Adjust this value as needed (default is 0.8)
plt.bar(Hours, crash_counts, color='blue', width=bar_width)
plt.xlabel('Hour of the Day')
plt.ylabel('Crash Count')
plt.title('Crash Count by Hourly Rate')
plt.xticks(ticks=range(len(Hours)), labels=Hours, rotation=90)

# Set the y-axis limit
plt.ylim(0, max(crash_counts) + 10000)
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig("hourly_crash_count.png")

plt.show()

# Print DataFrame results in console (optional, for debugging)
hourly_crashes.show(24, truncate=False)
