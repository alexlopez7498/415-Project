from pyspark.sql import SparkSession, functions as F

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "../Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Group by ZIP CODE and count the number of crashes
zipcode_crashes = df.groupBy("ZIP CODE").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by CRASH_COUNT in descending order
zipcode_crashes = zipcode_crashes.orderBy(F.desc("CRASH_COUNT"))

# Write the result to a new CSV file
zipcode_output_path = "../zipcode_crash_counts.csv"
zipcode_crashes.write.mode("overwrite").option("header", "true").csv(zipcode_output_path)
# Show the result for verification
zipcode_crashes.show(truncate=False)