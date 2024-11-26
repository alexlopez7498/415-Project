from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt
# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv" # change to Real
df = spark.read.option("header", "true").csv(file_path)

# Group by BOROUGH and count the number of crashes
borough_crashes = df.groupBy("BOROUGH").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
borough_crashes = borough_crashes.orderBy(F.desc("CRASH_COUNT"))
results = borough_crashes.collect()
Boroughs = [row['BOROUGH'] for row in results]
crash_counts = [row['CRASH_COUNT'] for row in results]
    # Enable interactive mode for the plot
plt.ion()
plt.figure(figsize=(8, 5))  # Increase the width and height

    # Create the bar plot
bar_width = 0.8  # Adjust this value as needed (default is 0.8)
plt.bar(Boroughs, crash_counts, color='blue', width=bar_width)
plt.xlabel('BOROUGH')
plt.ylabel('Crash Count')
plt.title('Crash Count by Vehicle Year')
plt.xticks(ticks=range(len(Boroughs)), labels=Boroughs, rotation=90, ha='right')


plt.ylim(0, 600000)
plt.tight_layout()
    # Show the plot and allow interaction
plt.show(block=False)

    # Keep the plot window open
plt.show(block=True)
# Write the result to a new CSV file
# Show the result for verification
borough_crashes.show(truncate=False)