import pandas as pd
import numpy as np
import mlflow
import mlflow.xgboost
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, r2_score
import joblib

def run_pipeline():
    print("--- 1. Initializing Pipeline ---")
    # Using your absolute path
    df = pd.read_csv(r"D:\project_3 insurance premium\train.csv")
    
    # Dropping the ID bug and text columns
    df = df.drop(columns=['Policy Start Date', 'Customer Feedback', 'id'], errors='ignore')
    
    X = df.drop(columns=['Premium Amount'])
    y = np.log1p(df['Premium Amount']) 
    
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)
    
    numeric_features = X.select_dtypes(include=['int64', 'float64']).columns
    categorical_features = X.select_dtypes(include=['object']).columns

    preprocessor = ColumnTransformer(
        transformers=[
            ('num', Pipeline([('imputer', SimpleImputer(strategy='median')), ('scaler', StandardScaler())]), numeric_features),
            ('cat', Pipeline([('imputer', SimpleImputer(strategy='most_frequent')), ('onehot', OneHotEncoder(handle_unknown='ignore'))]), categorical_features)
        ])
    
    print("--- 2. Starting MLflow Experiment ---")
    mlflow.set_experiment("SmartPremium_Experiment")
    
    with mlflow.start_run():
        # Hyperparameters we are tracking
        params = {"n_estimators": 150, "learning_rate": 0.05, "max_depth": 6}
        mlflow.log_params(params)
        
        model = Pipeline(steps=[
            ('preprocessor', preprocessor),
            ('regressor', XGBRegressor(**params, random_state=42))
        ])
        
        model.fit(X_train, y_train)
        
        y_pred = np.expm1(model.predict(X_val))
        y_val_actual = np.expm1(y_val)
        
        # Fixed the squared parameter issue
        rmse = np.sqrt(mean_squared_error(y_val_actual, y_pred))
        r2 = r2_score(y_val_actual, y_pred)
        
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("r2", r2)
        
        print(f"Logged to MLflow - RMSE: {rmse:.2f}, R2: {r2:.4f}")
        
        print("--- 3. Saving Model for Production ---")
        print("Logged to MLflow - RMSE: {:.2f}, R2: {:.4f}".format(rmse, r2))
        
        print("--- 3. Saving Model for Production ---")
        # We removed the model.fit(X, y) line to save your computer's memory!
        joblib.dump(model, 'smartpremium_model.pkl')
        print("Success! Model saved as 'smartpremium_model.pkl' in your current folder.")

if __name__ == "__main__":
    run_pipeline()