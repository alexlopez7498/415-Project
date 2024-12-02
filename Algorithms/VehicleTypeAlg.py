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

    # Group by VEHICLE_TYPE and count occurrences
    vehicle_type_summary = df.groupBy("VEHICLE_TYPE").agg(
        count("COLLISION_ID").alias("Crash_Count")
    )

    # Sort by Crash_Count in descending order
    vehicle_type_summary = vehicle_type_summary.orderBy(col("Crash_Count").desc())

    # Collect results as a list
    results = vehicle_type_summary.collect()
    top_50_results = results[:50]
    Types = [row['VEHICLE_TYPE'] for row in top_50_results]
    crash_counts = [row['Crash_Count'] for row in top_50_results]
    # Format results for output
    result_str = "\n".join(
        [f"Type: {row['VEHICLE_TYPE']}, Crashes: {row['Crash_Count']}" for row in results]
    )

    # Enable interactive mode for the plot
    plt.ion()
    plt.figure(figsize=(8, 5))  # Increase the width and height

    # Create the bar plot
    bar_width = 0.8  # Adjust this value as needed (default is 0.8)
    plt.bar(Types, crash_counts, color='blue', width=bar_width)
    plt.xlabel('Vehicle Type')
    plt.ylabel('Crash Count')
    plt.title('Crash Count by Vehicle Type')
    plt.xticks(ticks=range(len(Types)), labels=Types, rotation=90, ha='right')


    plt.ylim(0, 600000)
    plt.tight_layout()
    # Show the plot and allow interaction
    plt.show(block=False)
    # Print results for GUI capture
    print("Vehicle Type Analysis Results:")
    print(result_str)

    # Keep the plot window open
    plt.show(block=True)
    # Measure and print execution time
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"\nExecution Time: {execution_time:.2f} seconds")

except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)

finally:
    spark.stop()
