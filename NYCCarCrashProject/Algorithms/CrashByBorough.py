from pyspark.sql import SparkSession, functions as F

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "../Motor_Vehicle_Collisions_-_Full.csv"  # change to Real
df = spark.read.option("header", "true").csv(file_path)

# Group by BOROUGH and count the number of crashes
borough_crashes = df.groupBy("BOROUGH").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
borough_crashes = borough_crashes.orderBy(F.desc("CRASH_COUNT"))

# Write the result to a new CSV file
borough_output_path = "../borough_crash_counts.csv"
borough_crashes.write.mode("overwrite").option("header", "true").csv(borough_output_path)
# Show the result for verification
borough_crashes.show(truncate=False)