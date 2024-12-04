import pandas as pd
from sklearn.decomposition import PCA, TruncatedSVD
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.feature_extraction.text import CountVectorizer
import matplotlib.pyplot as plt
import seaborn as sns

## GENERAL SETUP
# Load data
df = pd.read_csv("Motor_Vehicle_Collisions_-_Full.csv")

# Filter to keep only rows where 'PERSON_INJURY' is not null
df = df[df['PERSON_INJURY'].notna()]

# Replace 'Unspecified' and 'Injured' with 0, and 'Killed' with 1 in 'PERSON_INJURY'
df['PERSON_INJURY'] = df['PERSON_INJURY'].replace({'Unspecified': 0, 'Injured': 0, 'Killed': 1})

# Drop irrelevant columns for vectorization and PCA
df = df.drop(['LATITUDE', 'LONGITUDE', 'COLLISION_ID', 'VEHICLE_YEAR', 'CRASH DATE', 'CRASH TIME',
                'ZIP CODE', 'LOCATION', 'PERSON_ID', 'DRIVER_LICENSE_JURISDICTION', 'PED_LOCATION',
                'PED_ACTION', 'COMPLAINT', 'PED_ROLE', 'PERSON_SEX', 'VEHICLE_DAMAGE',
                'VEHICLE_DAMAGE_1','VEHICLE_DAMAGE_2', 'VEHICLE_DAMAGE_3', 'TRAVEL_DIRECTION',
                'PUBLIC_PROPERTY_DAMAGE'], axis=1)

# Generating df for testing where preportion of killed to not killed is closer to 5050

# Filter all rows where 'PERSON_INJURY' is 1
injury_df = df[df['PERSON_INJURY'] == 1]

# Randomly sample 4000 rows where 'PERSON_INJURY' is 0
no_injury_sample_df = df[df['PERSON_INJURY'] == 0].sample(n=4000, random_state=42)

# Concatenate the two DataFrames
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
X_train, X_test, y_train, y_test = train_test_split(principal_components, t_df['PERSON_INJURY'], test_size=0.20, random_state=42)

# Train logistic regression model
log_reg = LogisticRegression(max_iter=1000)
log_reg.fit(X_train, y_train)

# Predict on test set
y_pred = log_reg.predict(X_test)

# Evaluate the model
print("Accuracy:", accuracy_score(y_test, y_pred))
print(classification_report(y_test, y_pred))


# Generate the confusion matrix
conf_matrix = confusion_matrix(y_test, y_pred)

# Plot the confusion matrix
plt.figure(figsize=(8, 6))
sns.heatmap(conf_matrix, annot=True, fmt='d', cmap='Blues', xticklabels=["Not Killed (0)", "Killed (1)"],
            yticklabels=["Not Killed (0)", "Killed (1)"])
plt.xlabel("Predicted Label")
plt.ylabel("True Label")
plt.title("Confusion Matrix")
plt.show()