from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv"  # Ensure the file path is correct
df = spark.read.option("header", "true").csv(file_path)

# Group by BOROUGH and count the number of crashes
borough_crashes = df.groupBy("BOROUGH").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
borough_crashes = borough_crashes.orderBy(F.desc("CRASH_COUNT"))
results = borough_crashes.collect()

# Extract data for plotting
Boroughs = [row['BOROUGH'] for row in results]
crash_counts = [row['CRASH_COUNT'] for row in results]

# Create the bar plot
plt.figure(figsize=(10, 6))  # Specify the figure size
bar_width = 0.8  # Bar width
plt.bar(Boroughs, crash_counts, color='blue', width=bar_width)
plt.xlabel('BOROUGH')
plt.ylabel('Crash Count')
plt.title('Crash Count by BOROUGH')
plt.xticks(ticks=range(len(Boroughs)), labels=Boroughs, rotation=45, ha='right')  # Adjust rotation and alignment
plt.ylim(0, max(crash_counts) + 10000)  # Set y limit to be slightly above the highest count
plt.tight_layout()

# Save the plot as an image file
plt.savefig("borough_crash_count.png")  # Save the figure to a file
plt.close()  # Close the plot to free up memory

# Optionally display the plot as well (uncomment the next line if you want to see the plot when running the script)
plt.show()

# Print DataFrame for verification in console (optional, for debugging)
borough_crashes.show(truncate=False)

# Stop the Spark session
spark.stop()