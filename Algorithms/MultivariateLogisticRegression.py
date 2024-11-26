import sys
import pandas as pd
from sklearn.decomposition import TruncatedSVD
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, accuracy_score

def main(file_path):
    # Load dataset
    df = pd.read_csv(file_path)
    
    # Filter relevant columns
    relevant_columns = [
        'CONTRIBUTING FACTOR VEHICLE 1', 'CONTRIBUTING FACTOR VEHICLE 2',
        'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2', 
        'DRIVER_SEX', 'PERSON_INJURY'
    ]
    
    df = df[relevant_columns].dropna()

    # One-hot encode categorical variables
    df_encoded = pd.get_dummies(df, columns=[
        'CONTRIBUTING FACTOR VEHICLE 1', 'CONTRIBUTING FACTOR VEHICLE 2',
        'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2', 
        'DRIVER_SEX'
    ])

    # Separate features and target
    X = df_encoded.drop('PERSON_INJURY', axis=1)
    y = df_encoded['PERSON_INJURY'].apply(lambda x: 1 if x == 'Killed' else 0)

    # Apply SVD
    svd = TruncatedSVD(n_components=10, random_state=42)
    X_svd = svd.fit_transform(X)

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X_svd, y, test_size=0.3, random_state=42)

    # Logistic Regression
    model = LogisticRegression(random_state=42)
    model.fit(X_train, y_train)

    # Predictions
    y_pred = model.predict(X_test)

    # Compute metrics
    accuracy = accuracy_score(y_test, y_pred)
    conf_matrix = confusion_matrix(y_test, y_pred)

    # Output results in a format for the GUI to capture
    print(f"Accuracy: {accuracy:.2f}")
    print("Confusion Matrix:")
    print(conf_matrix)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python MultivariateLogisticRegression.py <file_path>")
    else:
        main(sys.argv[1])
