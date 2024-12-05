import sys
import pandas as pd
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

client = MongoClient('mongodb://localhost:27017/')
db = client['CarCrash']  
collection = db['Crashes2']  

# Fetch data from MongoDB
data = list(collection.find())  # convert cursor to a list

# Convert to pandas DataFrame
df = pd.DataFrame(data)

# Drop any irrelevant columns (adjust to match the columns in your MongoDB data)
df = df.drop(['LATITUDE', 'LONGITUDE', 'COLLISION_ID', 'VEHICLE_YEAR', 'CRASH DATE', 'CRASH TIME',
              'ZIP CODE', 'LOCATION', 'PERSON_ID', 'DRIVER_LICENSE_JURISDICTION', 'PED_LOCATION',
              'PED_ACTION', 'COMPLAINT', 'PED_ROLE', 'PERSON_SEX', 'VEHICLE_DAMAGE',
              'VEHICLE_DAMAGE_1', 'VEHICLE_DAMAGE_2', 'VEHICLE_DAMAGE_3', 'TRAVEL_DIRECTION',
              'PUBLIC_PROPERTY_DAMAGE'], axis=1)

# Replace 'Unspecified' and 'Injured' with 0, and 'Killed' with 1 in 'PERSON_INJURY'
df['PERSON_INJURY'] = df['PERSON_INJURY'].replace({'Unspecified': 0, 'Injured': 0, 'Killed': 1})

# Handle missing data, drop rows, etc.
df = df[df['PERSON_INJURY'].notna()]

# Generating df for testing where the proportion of killed to not killed is closer to 50-50
injury_df = df[df['PERSON_INJURY'] == 1]
no_injury_sample_df = df[df['PERSON_INJURY'] == 0].sample(n=4000, random_state=42)
df_t = pd.concat([injury_df, no_injury_sample_df])

# Shuffle the rows in case the order matters
df_t = df_t.sample(frac=1, random_state=42).reset_index(drop=True)
t_df = df_t

# Remove rows with empty strings
df_t = df_t.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
df_t = df_t[df_t.str.strip() != ""]  # Keep only rows with non-empty strings

# Reset indices to ensure alignment with 'PERSON_INJURY'
df_t = df_t.reset_index(drop=True)
df = df.loc[df_t.index].reset_index(drop=True)

# Vectorize the text data in df_t
vectorizer = CountVectorizer()
vec = vectorizer.fit_transform(df_t)

# Use TruncatedSVD to reduce dimensions instead of PCA to handle sparse data efficiently
svd = TruncatedSVD(n_components=23, random_state=42)  # n_components=23 provides highest accuracy
principal_components = svd.fit_transform(vec)

# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    principal_components,
    t_df['PERSON_INJURY'],
    test_size=0.20,
    stratify=t_df['PERSON_INJURY'],  # Ensure balanced stratified split
    random_state=42
)

# Train logistic regression model
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train, y_train)

# Predict on test set
y_pred = log_reg.predict(X_test)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f"Accuracy: {accuracy:.4f}")

# Convert classification report to a compact table
report = classification_report(y_test, y_pred, output_dict=True)
report_df = pd.DataFrame(report).transpose()

# Select relevant metrics and format
compact_report_df = report_df[['precision', 'recall', 'f1-score', 'support']].round(2)
compact_report_df.reset_index(inplace=True)
compact_report_df.rename(columns={'index': 'Class'}, inplace=True)

# Print compact report
print(compact_report_df.to_string(index=False))

# Generate the confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)

# Plot the confusion matrix
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=["Not Killed (0)", "Killed (1)"],
            yticklabels=["Not Killed (0)", "Killed (1)"])
plt.xlabel("Predicted Label")
plt.ylabel("True Label")
plt.title("Confusion Matrix")
plt.savefig("confusion_matrix.png")  # Save the figure
#plt.show()
plt.close()  # Close the figure to free resources
