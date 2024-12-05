from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

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

# Group by 'VEHICLE_TYPE' and count the number of crashes
yearly_crashes = df.groupBy("VEHICLE_TYPE").agg(count("*").alias("CRASH_COUNT"))

# Sort by crash count in descending order and limit to the top 50
top_50_crashes = yearly_crashes.orderBy(col("CRASH_COUNT").desc()).limit(50)

# Show the top 50 results
top_50_crashes.show()

# Convert the Spark DataFrame to a Pandas DataFrame
top_50_crashes_pd = top_50_crashes.toPandas()

# Plot the top 50 results
import matplotlib.pyplot as plt

plt.figure(figsize=(12, 6))  # Adjust figure size if necessary
bar_width = 0.8  # Adjust this value as needed (default is 0.8)
plt.bar(top_50_crashes_pd['VEHICLE_TYPE'], top_50_crashes_pd['CRASH_COUNT'], bar_width, color='blue')
plt.xlabel('VEHICLE_TYPE')
plt.ylabel('Number of Crashes')
plt.title('Top 50 Vehicle Types with the Most Crashes')
plt.xticks(rotation=90)
plt.tight_layout()

plt.savefig("vehicle_type_bar_chart.png")
# Show the plot and allow interaction
plt.show(block=False)

# Keep the plot window open
plt.show(block=True)