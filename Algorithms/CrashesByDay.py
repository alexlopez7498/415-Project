from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .appName("MongoDB CarCrash Analysis") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/CarCrash.Crashes2") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:2.4.2') \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Read data from MongoDB into a Spark DataFrame
df = spark.read.format("mongo").load()

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

# Collect results into a Pandas DataFrame
daily_crashes_pd = daily_crashes.toPandas()


# Function to generate a pie chart and save it as an image
def generate_pie_chart(data):
    labels = data["DAY_OF_WEEK"]
    sizes = data["CRASH_COUNT"]
    total = sum(sizes)
    percentages = [count / total * 100 for count in sizes]

    # Create the figure and make the pie chart larger
    fig, ax = plt.subplots(figsize=(8, 8))
    wedges, texts, autotexts = ax.pie(
        percentages,
        labels=labels,
        autopct=lambda p: f'{p:.1f}%',
        startangle=90,
        textprops={'color': "black"}
    )
    ax.set_title("Crashes by Day of Week", fontsize=16)

    # Adjust percentage text size
    plt.setp(autotexts, size=12, weight="bold")
    plt.setp(texts, size=10)

    # Save the plot as an image
    plt.savefig("daily_crash_pie_chart.png")  # Save to the current working directory

    # Close the figure to prevent it from popping up
    plt.close(fig)

# Generate the pie chart
generate_pie_chart(daily_crashes_pd)


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

plt.show()

# Display results in Spark
daily_crashes.show(truncate=False)
