import tkinter as tk
from tkinter import filedialog, messagebox
from pyspark.sql import SparkSession, functions as F
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg


# Function to process hourly crashes
def process_hourly_crashes(file_path):
    # Create Spark session
    spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()
    try:
        # Load CSV into DataFrame
        df = spark.read.option("header", "true").csv(file_path)

        # Extract the hour from CRASH TIME
        df = df.withColumn("HOUR", F.hour(F.to_timestamp(F.col("CRASH TIME"), "H:mm")))

        # Aggregate crash counts by hour
        hourly_crashes = df.groupBy("HOUR").agg(F.count("*").alias("CRASH_COUNT")).orderBy("HOUR")

        # Convert Spark DataFrame to Pandas DataFrame
        return hourly_crashes.toPandas()

    except Exception as e:
        raise e
    finally:
        spark.stop()


# Function to generate a pie chart
def generate_pie_chart(data):
    # Data for the pie chart
    labels = data["DAY_OF_WEEK"]
    sizes = data["CRASH_COUNT"]
    total = sum(sizes)
    percentages = [count / total * 100 for count in sizes]

    # Create the figure and make the pie chart larger
    fig, ax = plt.subplots(figsize=(8, 8))  # Increase the figure size
    wedges, texts, autotexts = ax.pie(
        percentages,
        labels=labels,
        autopct=lambda p: f'{p:.1f}%',  # Format percentage display
        startangle=90,
        textprops={'color': "black"}
    )
    ax.set_title("Crashes by Day of Week", fontsize=16)

    # Adjust percentage text size
    plt.setp(autotexts, size=8, weight="bold")  # Make percentage text smaller
    plt.setp(texts, size=10)  # Adjust label text size if needed

    return fig


# Function to process daily crashes
def process_daily_crashes(file_path):
    # Create Spark session
    spark = SparkSession.builder.appName("CrashAnalysis").getOrCreate()
    try:
        # Load CSV into DataFrame
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

        daily_crashes = daily_crashes.withColumn("DAY_ORDER", day_order).orderBy("DAY_ORDER").drop("DAY_ORDER")

        # Convert Spark DataFrame to Pandas DataFrame
        return daily_crashes.toPandas()

    except Exception as e:
        raise e
    finally:
        spark.stop()


# Function to run analysis
def run_analysis():
    file_path = "Motor_Vehicle_Collisions_-_Full.csv"  # Hardcoded file path

    try:
        # Process hourly crashes
        hourly_results_df = process_hourly_crashes(file_path)

        # Update Hourly Results
        hourly_result_text.config(state=tk.NORMAL)  # Enable editing temporarily
        hourly_result_text.delete(1.0, tk.END)
        if hourly_results_df.empty:
            hourly_result_text.insert(tk.END, "No data found.\n")
        else:
            hourly_result_text.insert(tk.END, hourly_results_df.to_string(index=False))
        hourly_result_text.config(state=tk.DISABLED)  # Disable editing again

        # Save hourly results to CSV
        hourly_results_df.to_csv("hourly_crash_counts.csv", index=False)

        # Process daily crashes
        daily_results_df = process_daily_crashes(file_path)

        # Update Daily Results
        daily_result_text.config(state=tk.NORMAL)  # Enable editing temporarily
        daily_result_text.delete(1.0, tk.END)
        if daily_results_df.empty:
            daily_result_text.insert(tk.END, "No data found.\n")
        else:
            daily_result_text.insert(tk.END, daily_results_df.to_string(index=False))
        daily_result_text.config(state=tk.DISABLED)  # Disable editing again

        # Save daily results to CSV
        daily_results_df.to_csv("daily_crash_counts.csv", index=False)

        # Generate pie chart
        fig = generate_pie_chart(daily_results_df)

        # Display the pie chart in the Tkinter GUI
        for widget in chart_frame.winfo_children():
            widget.destroy()  # Clear previous chart
        canvas = FigureCanvasTkAgg(fig, master=chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack()

        # Show success message
        messagebox.showinfo("Success", "Analysis complete. Results saved to CSV files.")

    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")


# GUI Setup
root = tk.Tk()
root.title("NYC Car Crash Analysis")

frame = tk.Frame(root)
frame.pack(padx=20, pady=20)

# Run analysis button
tk.Button(frame, text="Run Analysis", command=run_analysis).grid(row=0, column=0, pady=10)

# Hourly results display
tk.Label(frame, text="Hourly Crash Counts:").grid(row=1, column=0, sticky="w", padx=5, pady=5)
hourly_result_text = tk.Text(frame, height=10, width=50, state=tk.DISABLED)
hourly_result_text.grid(row=2, column=0, padx=5, pady=5)

# Daily results display
tk.Label(frame, text="Crashes by Day of Week:").grid(row=3, column=0, sticky="w", padx=5, pady=5)
daily_result_text = tk.Text(frame, height=10, width=50, state=tk.DISABLED)
daily_result_text.grid(row=4, column=0, padx=5, pady=5)

# Chart display frame
chart_frame = tk.Frame(root)
chart_frame.pack(padx=20, pady=20)

root.mainloop()