import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score

def main():
    # Setup asset directory for plots
    os.makedirs('assets', exist_ok=True)

    print("--- 1. Loading Data ---")
    train_df = pd.read_csv(r"D:\project_3 insurance premium\train.csv")
    test_df = pd.read_csv(r"D:\project_3 insurance premium\test.csv")
    submission_df = pd.read_csv(r"D:\project_3 insurance premium\sample_submission.csv")
    
    print(f"Train Shape: {train_df.shape}")
    print(f"Test Shape: {test_df.shape}")

    print("\n--- 2. Exploratory Data Analysis ---")
    print(train_df.info())
    
    # Save a plot instead of displaying it inline
    plt.figure(figsize=(8, 5))
    sns.histplot(train_df['Premium Amount'], bins=50, kde=True)
    plt.title('Distribution of Premium Amount')
    plt.savefig('assets/premium_distribution.png')
    print("\n EDA Plot saved to assets/premium_distribution.png")

    print("\n--- 3. Preprocessing & Training Setup ---")
    # Drop unusable text columns AND the 'id' column
    train_clean = train_df.drop(columns=['Policy Start Date', 'Customer Feedback', 'id'], errors='ignore')
    test_clean = test_df.drop(columns=['Policy Start Date', 'Customer Feedback', 'id'], errors='ignore')

    X = train_clean.drop(columns=['Premium Amount'])
    # Apply log transformation to normalize the skewed target variable
    y = np.log1p(train_clean['Premium Amount']) 

    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

    numeric_features = X.select_dtypes(include=['int64', 'float64']).columns
    categorical_features = X.select_dtypes(include=['object']).columns

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', Pipeline([('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), numeric_features),
            ('cat', Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('onehot', OneHotEncoder(handle_unknown='ignore'))]), categorical_features)
        ])

    model = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('regressor', XGBRegressor(n_estimators=150, learning_rate=0.05, max_depth=6, random_state=42))
    ])

    print("\n--- 4. Local Evaluation ---")
    model.fit(X_train, y_train)
    y_val_pred_log = model.predict(X_val)
    
    # Inverse transform predictions from log scale
    y_val_pred = np.expm1(y_val_pred_log)
    y_val_actual = np.expm1(y_val)

    print(f"Validation RMSE: {np.sqrt(mean_squared_error(y_val_actual, y_val_pred)):.2f}")
    print(f"Validation R2 Score: {r2_score(y_val_actual, y_val_pred):.4f}")

    print("\n--- 5. Generating Final Predictions ---")
    # Retrain on the entire training set for maximum accuracy
    model.fit(X, y)
    test_predictions_log = model.predict(test_clean)
    test_predictions = np.expm1(test_predictions_log)

    submission_df['Premium Amount'] = test_predictions
    submission_df.to_csv(r'D:\project_3 insurance premium\final_submission.csv', index=False)
    print("Success! final_submission.csv generated in the data folder.")

if __name__ == "__main__":
    main()