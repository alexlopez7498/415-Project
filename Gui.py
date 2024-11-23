import tkinter as tk
from tkinter import messagebox, filedialog
import subprocess

# Function to run the selected analysis
def run_analysis():
    selected_algorithm = algorithm_var.get()
    if not selected_algorithm:
        messagebox.showerror("Error", "Please select an algorithm")
        return

    try:
        if selected_algorithm == "Hourly Crash Counts":
            run_external_script("HourlyCrashRate.py", "Hourly Crash Counts")

        elif selected_algorithm == "Crash By Borough":
            run_external_script("CrashByBorough.py", "Crash By Borough")

        elif selected_algorithm == "Crash By Zipcode":
            run_external_script("CrashByZipcode.py", "Crash By Zipcode")

        elif selected_algorithm == "Crash By Day":
            run_external_script("CrashesByDay.py", "Crash By Day")

        elif selected_algorithm == "Vehicle Year Analysis":
            run_external_script("VehicleYearAlg.py", "Vehicle Year Analysis")

        elif selected_algorithm == "Vehicle Type Analysis":
            run_external_script("VehicleTypeAlg.py", "Vehicle Type Analysis")

        elif selected_algorithm == "Driver Demographic Analysis":
            run_external_script("VehicleDemographicAlg.py", "Driver Demographic Analysis")

        else:
            messagebox.showerror("Error", "Invalid algorithm selected")
            return
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")


def run_external_script(script_name, analysis_name):
    # File path is always "Motor_Vehicle_Collisions_-_Full.csv"
    file_path = "Motor_Vehicle_Collisions_-_Full.csv"

    try:
        # Call the external script
        result = subprocess.run(
            ["python", script_name, file_path],
            capture_output=True,
            text=True
        )

        # Display the script output in the Text widget
        result_text.delete(1.0, tk.END)
        result_text.insert(tk.END, result.stdout)

        if result.returncode == 0:
            messagebox.showinfo("Success", f"{analysis_name} completed successfully!")
        else:
            messagebox.showerror("Error", f"{analysis_name} failed:\n{result.stderr}")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred while running {analysis_name}: {e}")

# Create main window
root = tk.Tk()
root.title("NYC Car Crash Analysis")

# Create layout
frame = tk.Frame(root)
frame.pack(padx=20, pady=20)

# Dropdown menu for selecting the algorithm
algorithm_label = tk.Label(frame, text="Select Analysis Algorithm:")
algorithm_label.grid(row=0, column=0, sticky="w", padx=5, pady=5)

algorithm_var = tk.StringVar(value="")
algorithm_menu = tk.OptionMenu(
    frame, algorithm_var, 
    "Hourly Crash Counts", 
    "Top Contributing Factors", 
    "Vehicle Year Analysis", 
    "Vehicle Type Analysis", 
    "Driver Demographic Analysis",
    "Crash By Borough",
    "Crash By Zipcode",
    "Crash By Day"
)

algorithm_menu.grid(row=0, column=1, padx=5, pady=5)

# Run Analysis Button
run_button = tk.Button(frame, text="Run Analysis", command=run_analysis)
run_button.grid(row=1, column=0, columnspan=2, pady=10)

# Result display area (using Text widget for better formatting)
result_label = tk.Label(frame, text="Analysis Results:")
result_label.grid(row=2, column=0, columnspan=2, sticky="w", padx=5, pady=5)

result_text = tk.Text(frame, height=10, width=50)
result_text.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

# Start the main loop
root.mainloop()
