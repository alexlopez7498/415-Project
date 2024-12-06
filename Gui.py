import tkinter as tk
from tkinter import messagebox, filedialog
import subprocess
from PIL import Image, ImageTk
import os
# Function to run the selected analysis
def run_analysis():
    selected_algorithm = algorithm_var.get()
    if not selected_algorithm:
        messagebox.showerror("Error", "Please select an algorithm")
        return

    try:
        # Clear any existing images
        clear_displayed_images()

        if selected_algorithm == "Hourly Crash Counts":
            run_external_script("Algorithms/HourlyCrashRate.py", "Hourly Crash Counts")

        elif selected_algorithm == "Crash By Borough":
            run_external_script("Algorithms/CrashByBorough.py", "Crash By Borough")

        elif selected_algorithm == "Crash By Zipcode":
            run_external_script("Algorithms/CrashByZipcode.py", "Crash By Zipcode")

        elif selected_algorithm == "Crash By Day":
            run_external_script("Algorithms/CrashesByDay.py", "Crash By Day")

        elif selected_algorithm == "Vehicle Year Analysis":
            run_external_script("Algorithms/VehicleYearAlg.py", "Vehicle Year Analysis")

        elif selected_algorithm == "Vehicle Type Analysis":
            run_external_script("Algorithms/VehicleTypeAlg.py", "Vehicle Type Analysis")

        elif selected_algorithm == "Driver Demographic Analysis":
            run_external_script("Algorithms/VehicleDemographicAlg.py", "Driver Demographic Analysis")

        elif selected_algorithm == "Confusion Matrix":
            run_external_script("Algorithms/Matrix.py", "Confusion Matrix")


        else:
            messagebox.showerror("Error", "Invalid algorithm selected")
            return
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")


def clear_displayed_images():
    """Clears any displayed images in the Tkinter window."""
    global root
    # Define the attribute names for the images exactly as they are used in `run_external_script`
    image_attributes = [
        "pie_chart_image", "confusion_matrix_image", "vehicle_type_chart_image",
        "borough_chart_image", "zipcode_chart_image", "vehicle_year_chart_image",
        "demographic_chart_image", "hourly_crash_chart_image"
    ]

    # Iterate through all image attributes and clear them if they exist
    for attr in image_attributes:
        image_label_attr = f"{attr}_label"  # The label attribute name

        # Clear the image
        if hasattr(run_external_script, attr):
            delattr(run_external_script, attr)

        # Clear the image label and destroy the widget
        if hasattr(run_external_script, image_label_attr):
            label = getattr(run_external_script, image_label_attr)
            label.destroy()
            delattr(run_external_script, image_label_attr)

def run_external_script(script_name, analysis_name):

    # File path is always "Motor_Vehicle_Collisions_-_Full.csv"
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the CSV file (one level up)
    file_path = os.path.join(script_dir, "../Motor_Vehicle_Collisions_-_Full.csv")
    pie_chart_path = "daily_crash_pie_chart.png"
    confusion_matrix_path = "confusion_matrix.png"
    vehicle_type_chart_path = "vehicle_type_bar_chart.png"
    borough_chart_path = "borough_crash_count.png"
    #zipcode_char_path = "zipcode_crash_count.png"
    vehicle_year_chart_path = "vehicle_year_bar_chart.png"
    demographic_path = "demographic_crash_count.png"
    hourly_crash_chart = "hourly_crash_count.png"


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

            # Display the appropriate image based on the analysis
            if analysis_name == "Crash By Day":
                if os.path.exists(pie_chart_path):
                    display_image(pie_chart_path, "pie_chart_image")
                else:
                    messagebox.showerror("Error", "Pie chart file not found!")

            elif analysis_name == "Confusion Matrix":
                if os.path.exists(confusion_matrix_path):
                    display_image(confusion_matrix_path, "confusion_matrix_image")
                else:
                    messagebox.showerror("Error", "Confusion matrix file not found!")

            elif analysis_name == "Vehicle Type Analysis":
                if os.path.exists(vehicle_type_chart_path):
                    display_image(vehicle_type_chart_path, "vehicle_type_chart_image")
                else:
                    messagebox.showerror("Error", "Vehicle crash count bar chart not found!")

            elif analysis_name == "Crash By Borough":
                if os.path.exists(borough_chart_path):
                    display_image(borough_chart_path, "borough_chart_image")
                else:
                    messagebox.showerror("Error", "Crash by borough bar chart not found!")

            elif analysis_name == "Crash By Zipcode":
                zipcode_chart_path = "zipcode_crash_count.png"  # Ensure this path is correct
                if os.path.exists(zipcode_chart_path):
                    display_image(zipcode_chart_path, "zipcode_chart_image")
                else:
                    messagebox.showerror("Error", "Zipcode crash count bar chart not found!")


            elif analysis_name == "Vehicle Year Analysis":
                if os.path.exists(vehicle_year_chart_path):
                    display_image(vehicle_year_chart_path, "vehicle_year_chart_image")
                else:
                    messagebox.showerror("Error", "Vehicle year crash count bar chart not found!")

            elif analysis_name == "Driver Demographic Analysis":
                if os.path.exists(demographic_path):
                    display_image(demographic_path, "demographic_chart_image")
                else:
                    messagebox.showerror("Error", "Demographic crash count bar chart not found!")

            elif analysis_name == "Hourly Crash Counts":
                if os.path.exists(hourly_crash_chart):
                    display_image(hourly_crash_chart, "hourly_crash_chart_image")
                else:
                    messagebox.showerror("Error", "Hourly crash count bar chart not found!")


        else:
            messagebox.showerror("Error", f"{analysis_name} failed:\n{result.stderr}")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred while running {analysis_name}: {e}")


def display_image(image_path, image_attr_name):
    """Helper function to display an image in the Tkinter root window."""
    global root  # Ensure `root` is accessible
    img = Image.open(image_path)
    img = img.resize((400, 400), Image.Resampling.LANCZOS)
    photo = ImageTk.PhotoImage(img)

    # Keep a reference to the image to prevent garbage collection
    setattr(run_external_script, image_attr_name, photo)  # Save it on the run_external_script function for consistency

    # Create or update the Label widget with the image
    label_attr_name = f"{image_attr_name}_label"
    if hasattr(run_external_script, label_attr_name):
        label = getattr(run_external_script, label_attr_name)
        label.config(image=photo)
        label.image = photo  # Keep a reference in the label to prevent garbage collection
    else:
        label = tk.Label(root, image=photo)
        setattr(run_external_script, label_attr_name, label)
        label.pack()




# Create main window
root = tk.Tk()
root.title("NYC Car Crash Analysis")

# Create layout
frame = tk.Frame(root)
frame.pack(padx=100, pady=100)

# Dropdown menu for selecting the algorithm
algorithm_label = tk.Label(frame, text="Select Analysis Algorithm:")
algorithm_label.grid(row=0, column=0, sticky="w", padx=5, pady=5)

algorithm_var = tk.StringVar(value="")
algorithm_menu = tk.OptionMenu(
    frame, algorithm_var, 
    "Hourly Crash Counts", 
    "Vehicle Year Analysis", 
    "Vehicle Type Analysis", 
    "Driver Demographic Analysis",
    "Crash By Borough",
    "Crash By Zipcode",
    "Crash By Day",
    "Confusion Matrix"
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
