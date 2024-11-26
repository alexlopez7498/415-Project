from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

# Create Spark session
spark = SparkSession.builder \
    .appName("CrashAnalysis") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()


# Load CSV file into DataFrame
file_path = "Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Convert CRASH DATE to proper date format and extract the day of the week
df = df.withColumn(
    "DAY_OF_WEEK",
    F.date_format(F.to_date(F.col("CRASH DATE"), "M/d/yy"), "EEEE")
)

# Group by DAY_OF_WEEK and count the number of crashes
daily_crashes = df.groupBy("DAY_OF_WEEK").agg(F.count("*").alias("CRASH_COUNT"))

# Define a custom order for days of the week
day_order = F.when(F.col("DAY_OF_WEEK") == "Sunday", 1) \
             .when(F.col("DAY_OF_WEEK") == "Monday", 2) \
             .when(F.col("DAY_OF_WEEK") == "Tuesday", 3) \
             .when(F.col("DAY_OF_WEEK") == "Wednesday", 4) \
             .when(F.col("DAY_OF_WEEK") == "Thursday", 5) \
             .when(F.col("DAY_OF_WEEK") == "Friday", 6) \
             .when(F.col("DAY_OF_WEEK") == "Saturday", 7)

# Order the results and collect
daily_crashes = daily_crashes.withColumn("DAY_ORDER", day_order).orderBy("DAY_ORDER")
results = daily_crashes.collect()

# Prepare data for plotting
days = [row["DAY_OF_WEEK"] for row in results]
crash_counts = [row["CRASH_COUNT"] for row in results]

# Plot the data
plt.figure(figsize=(12, 6))
plt.bar(days, crash_counts, color='blue', width=0.8)
plt.xlabel("Day of the Week")
plt.ylabel("Crash Count")
plt.title("Crash Count by Day of the Week")
plt.xticks(rotation=45)
plt.ylim(0, 300000)
plt.tight_layout()

# Display the plot
plt.show()

# Display results in Spark
daily_crashes.show(truncate=False)
