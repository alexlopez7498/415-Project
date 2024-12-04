from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Clean the ZIP CODE column by trimming and converting to uppercase
df = df.withColumn("ZIP CODE", F.upper(F.trim(F.col("ZIP CODE"))))

# Filter out rows where ZIP CODE is null, empty, or "UNSPECIFIED"
df = df.filter(
    (F.col("ZIP CODE").isNotNull()) &
    (F.col("ZIP CODE") != "") &
    (F.col("ZIP CODE") != "UNSPECIFIED")
)

# Group by ZIP CODE and count the number of crashes
zipcode_crashes = df.groupBy("ZIP CODE").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
zipcode_crashes = zipcode_crashes.orderBy(F.desc("CRASH_COUNT"))
results = zipcode_crashes.collect()

# Prepare data for plotting
Zipcodes = [row['ZIP CODE'] for row in results]
crash_counts = [row['CRASH_COUNT'] for row in results]

# Plot the data
plt.figure(figsize=(16, 10))  # Increase the width and height

# Create the bar plot
bar_width = 0.8  # Adjust this value as needed (default is 0.8)
plt.bar(Zipcodes[:100], crash_counts[:100], color='blue', width=bar_width)
plt.xlabel('ZIP CODE')
plt.ylabel('Crash Count')
plt.title('Crash Count by ZipCode')
plt.xticks(ticks=range(len(Zipcodes[:100])), labels=Zipcodes[:100], rotation=90, ha='right')

# Determine the maximum crash count for setting the y-axis limit
max_crash_count = max(crash_counts)
plt.ylim(0, max_crash_count + 1000)
plt.tight_layout()

# Save the plot as a PNG file
plt.savefig("zipcode_crash_count.png")

#plt.show()

plt.close()

# Display DataFrame results in console
zipcode_crashes.show(truncate=False)

# Stop the Spark session to free resources
spark