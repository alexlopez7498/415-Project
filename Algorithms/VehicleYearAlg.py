import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import col, year, count
import matplotlib.pyplot as plt

mongo_jar_path = "C:/Users/a/Downloads/mongo-spark-connector_2.12-3.0.1.jar"

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("MongoDB CarCrash Analysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2') \
    .getOrCreate()

# Read data from MongoDB into a Spark DataFrame
df = spark.read.format("mongo").load()

# # Print the schema to check the format of 'CRASH DATE'
# df.printSchema()

# # Show the first 10 rows of 'CRASH DATE' to inspect its format
# df.select("CRASH DATE").show(10, truncate=False)

# Convert 'CRASH DATE' to datetime (matching the format in your data)
df = df.withColumn("CRASH DATE", to_timestamp(col("CRASH DATE"), "MM/dd/yyyy"))

# Check if the conversion works correctly
# df.select("CRASH DATE").show(10, truncate=False)

# Extract the year from the 'CRASH DATE' column
df = df.withColumn("YEAR", year(col("CRASH DATE")))

# Group by 'YEAR' and count the number of crashes
yearly_crashes = df.groupBy("YEAR").agg(count("*").alias("CRASH_COUNT"))

# Show the result
yearly_crashes.show()

# Convert the result to a Pandas DataFrame for plotting
yearly_crashes_pd = yearly_crashes.toPandas()

# Plot the result
plt.ion()
plt.figure(figsize=(12, 6))  # Adjust figure size if necessary
bar_width = 0.8  # Adjust the width of the bars
plt.bar(yearly_crashes_pd['YEAR'], yearly_crashes_pd['CRASH_COUNT'], bar_width,color='blue')
plt.xlabel('Year')
plt.ylabel('Number of Crashes')
plt.title('Crashes per Year')
plt.xlim(2011, 2025)
plt.xticks(yearly_crashes_pd['YEAR'], rotation=90)
plt.tight_layout()
    # Create the bar plot
plt.savefig("vehicle_year_bar_chart.png")
# Show the plot and allow interaction
plt.show(block=False)

# Keep the plot window open
plt.show(block=True)
