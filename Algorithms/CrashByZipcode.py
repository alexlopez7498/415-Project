from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Group by ZIP CODE and count the number of crashes
zipcode_crashes = df.groupBy("ZIP CODE").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
zipcode_crashes = zipcode_crashes.orderBy(F.desc("CRASH_COUNT"))
results = zipcode_crashes.collect()

Zipcodes = [row['ZIP CODE'] for row in results]
crash_counts = [row['CRASH_COUNT'] for row in results]

plt.ion()
plt.figure(figsize=(16, 10))  # Increase the width and height

    # Create the bar plot
bar_width = 0.8  # Adjust this value as needed (default is 0.8)
plt.bar(Zipcodes[:100], crash_counts[:100], color='blue', width=bar_width)
plt.xlabel('ZIP CODE')
plt.ylabel('Crash Count')
plt.title('Crash Count by ZipCode')
plt.xticks(ticks=range(len(Zipcodes[:100])), labels=Zipcodes[:100], rotation=90, ha='right')


plt.ylim(0, 600000)
plt.tight_layout()
    # Show the plot and allow interaction
plt.show(block=False)

    # Keep the plot window open
plt.show(block=True)
# Show the result for verification
zipcode_crashes.show(truncate=False)