from pyspark.sql import SparkSession, functions as F

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Convert CRASH DATE to proper date format and extract the day of the week
df = df.withColumn(
    "DAY_OF_WEEK",
    F.date_format(F.to_date(F.col("CRASH DATE"), "M/d/yy"), "EEEE")  # Extract day of the week as full name
)

# Group by DAY_OF_WEEK and count the number of crashes
daily_crashes = df.groupBy("DAY_OF_WEEK").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by the day of the week order (optional)
# Define a custom order for days of the week
day_order = F.when(F.col("DAY_OF_WEEK") == "Sunday", 1)\
             .when(F.col("DAY_OF_WEEK") == "Monday", 2)\
             .when(F.col("DAY_OF_WEEK") == "Tuesday", 3)\
             .when(F.col("DAY_OF_WEEK") == "Wednesday", 4)\
             .when(F.col("DAY_OF_WEEK") == "Thursday", 5)\
             .when(F.col("DAY_OF_WEEK") == "Friday", 6)\
             .when(F.col("DAY_OF_WEEK") == "Saturday", 7)

daily_crashes = daily_crashes.withColumn("DAY_ORDER", day_order).orderBy("DAY_ORDER").drop("DAY_ORDER")

# Write the result to a new CSV file
# Write the result to a new CSV file, overwriting if the file exists
output_path = "daily_crash_counts.csv"
daily_crashes.write.mode("overwrite").option("header", "true").csv(output_path)

# Show the result for verification
daily_crashes.show(truncate=False)