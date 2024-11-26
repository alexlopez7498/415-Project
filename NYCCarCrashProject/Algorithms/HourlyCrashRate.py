from pyspark.sql import SparkSession, functions as F

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame from the parent directory
file_path = "../Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Print schema to debug column names
df.printSchema()

# Trim spaces or fix column name if necessary
df = df.withColumnRenamed("CRASH TIME ", "CRASH TIME")  # Example for renaming if needed

# Ensure CRASH TIME is in the correct format and extract the hour
df = df.withColumn("HOUR", F.hour(F.to_timestamp(F.col("CRASH TIME"), "H:mm")))

# Group by HOUR and count the number of crashes
hourly_crashes = df.groupBy("HOUR").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by HOUR for better readability
hourly_crashes = hourly_crashes.orderBy("HOUR")

# Write the result to a CSV file
output_path = "hourly_crash_counts.csv"
hourly_crashes.write.mode("overwrite").option("header", "true").csv(output_path)

# Show the result for verification
hourly_crashes.show(24, truncate=False)