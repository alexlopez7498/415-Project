from pyspark.sql import SparkSession, functions as F
import matplotlib.pyplot as plt

# Create Spark session
spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()

# Load CSV file into DataFrame
file_path = "../Motor_Vehicle_Collisions_-_Full.csv"
df = spark.read.option("header", "true").csv(file_path)

# Convert CRASH DATE to proper date format and extract the day of the week
df = df.withColumn(
    "DAY_OF_WEEK",
    F.date_format(F.to_date(F.col("CRASH DATE"), "M/d/yy"), "EEEE")  # Extract day of the week as full name
)

# Group by DAY_OF_WEEK and count the number of crashes
daily_crashes = df.groupBy("DAY_OF_WEEK").agg(F.count("*").alias("CRASH_COUNT"))

# Sort by the day of the week order (optional)
day_order = F.when(F.col("DAY_OF_WEEK") == "Sunday", 1)\
             .when(F.col("DAY_OF_WEEK") == "Monday", 2)\
             .when(F.col("DAY_OF_WEEK") == "Tuesday", 3)\
             .when(F.col("DAY_OF_WEEK") == "Wednesday", 4)\
             .when(F.col("DAY_OF_WEEK") == "Thursday", 5)\
             .when(F.col("DAY_OF_WEEK") == "Friday", 6)\
             .when(F.col("DAY_OF_WEEK") == "Saturday", 7)

daily_crashes = daily_crashes.withColumn("DAY_ORDER", day_order).orderBy("DAY_ORDER").drop("DAY_ORDER")

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

# Generate the pie chart
generate_pie_chart(daily_crashes_pd)

# Prepare table output for Tkinter
table_output = daily_crashes_pd.to_string(index=False)

# Print the table output (this will go back to the Tkinter Text widget)
print(table_output)
