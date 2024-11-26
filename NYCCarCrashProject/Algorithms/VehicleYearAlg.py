import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import matplotlib.pyplot as plt

# Validate input arguments
if len(sys.argv) < 2:
    print("Error: No file path provided. Please specify a CSV file.")
    sys.exit(1)

file_path = sys.argv[1]

try:
    # Initialize Spark session
    spark = SparkSession.builder.getOrCreate()

    # Measure execution time
    start_time = time.time()

    # Read the CSV file
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Filter out years greater than 2024
    df_filtered = df.filter(col("VEHICLE_YEAR") <= 2024)

    # Group by VEHICLE_YEAR and count the number of crashes for each year
    yearly_crash_summary = df_filtered.groupBy("VEHICLE_YEAR").agg(
        count("COLLISION_ID").alias("Crash_Count")
    )

    # Sort by Crash_Count in descending order
    yearly_crash_summary = yearly_crash_summary.orderBy(col("Crash_Count").desc())

    # Collect the results as a list of rows
    results = yearly_crash_summary.collect()
    # Prepare data for plotting
    years = [row['VEHICLE_YEAR'] for row in results]
    crash_counts = [row['Crash_Count'] for row in results]
    # Format results as a readable string
    result_str = "\n".join(
        [f"Year: {row['VEHICLE_YEAR']}, Crashes: {row['Crash_Count']}" for row in results]
    )

    # Enable interactive mode for the plot
    plt.ion()

    # Create the bar plot
    plt.bar(years, crash_counts, color='blue')
    plt.xlabel('Vehicle Year')
    plt.ylabel('Crash Count')
    plt.title('Crash Count by Vehicle Year')
    plt.xticks(rotation=45)
    plt.xlim(1900, 2025)
    plt.ylim(0, 150000)
    plt.tight_layout()

    # Show the plot and allow interaction
    plt.show(block=False)

    # Print detailed results for the GUI to capture
    print("Detailed Results:")
    print(result_str)

    # Keep the plot window open
    plt.show(block=True)



    # Measure end time and calculate execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nExecution Time: {execution_time:.2f} seconds")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

finally:
    # Stop the Spark session
    spark.stop()
