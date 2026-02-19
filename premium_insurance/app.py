import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os

# --- Page Configuration ---
st.set_page_config(page_title="SmartPremium Predictor", page_icon="💰", layout="wide")

st.title("💰 SmartPremium: Insurance Cost Predictor")
st.write("Enter the customer's details below. Our Machine Learning model will predict their annual insurance premium in real-time.")

# --- Load the Model ---
model_path = 'smartpremium_model.pkl'

if not os.path.exists(model_path):
    st.error("Model file not found! Please wait for pipeline.py to finish saving 'smartpremium_model.pkl'.")
    st.stop()

@st.cache_resource
def load_model():
    return joblib.load(model_path)

model = load_model()

# --- User Input Layout ---
st.markdown("### 🧑 Customer Demographics")
col1, col2, col3 = st.columns(3)

with col1:
    age = st.number_input("Age", min_value=18, max_value=100, value=30)
    gender = st.selectbox("Gender", ["Male", "Female"])
    marital_status = st.selectbox("Marital Status", ["Single", "Married", "Divorced"])

with col2:
    annual_income = st.number_input("Annual Income ($)", min_value=10000, max_value=500000, value=55000)
    dependents = st.number_input("Number of Dependents", min_value=0, max_value=10, value=1)
    education = st.selectbox("Education Level", ["High School", "Bachelor's", "Master's", "PhD"])

with col3:
    occupation = st.selectbox("Occupation", ["Employed", "Self-Employed", "Unemployed"])
    location = st.selectbox("Location", ["Urban", "Suburban", "Rural"])
    property_type = st.selectbox("Property Type", ["House", "Apartment", "Condo"])

st.markdown("### 🏥 Health & Policy Details")
col4, col5, col6 = st.columns(3)

with col4:
    health_score = st.slider("Health Score (1-100)", 1, 100, 75)
    smoking_status = st.selectbox("Smoking Status", ["No", "Yes"])
    exercise = st.selectbox("Exercise Frequency", ["Daily", "Weekly", "Monthly", "Rarely"])

with col5:
    policy_type = st.selectbox("Policy Type", ["Basic", "Comprehensive", "Premium"])
    insurance_duration = st.number_input("Insurance Duration (Years)", min_value=1, max_value=10, value=1)
    
with col6:
    vehicle_age = st.number_input("Vehicle Age (Years)", min_value=0, max_value=30, value=5)
    credit_score = st.number_input("Credit Score", min_value=300, max_value=850, value=700)
    previous_claims = st.number_input("Previous Claims", min_value=0, max_value=10, value=0)

# --- Prediction Logic ---
st.markdown("---")
if st.button("🚀 Predict Premium Amount", type="primary", use_container_width=True):
    
    # 1. Gather all inputs into a single row DataFrame
    input_data = pd.DataFrame({
        'Age': [age], 'Gender': [gender], 'Annual Income': [annual_income],
        'Marital Status': [marital_status], 'Number of Dependents': [dependents],
        'Education Level': [education], 'Occupation': [occupation], 
        'Health Score': [health_score], 'Location': [location], 
        'Policy Type': [policy_type], 'Previous Claims': [previous_claims], 
        'Vehicle Age': [vehicle_age], 'Credit Score': [credit_score], 
        'Insurance Duration': [insurance_duration], 'Smoking Status': [smoking_status], 
        'Exercise Frequency': [exercise], 'Property Type': [property_type]
    })
    
    # 2. Make prediction and reverse the Log Transformation
    predicted_log_premium = model.predict(input_data)
    predicted_premium = np.expm1(predicted_log_premium) # Reverses the np.log1p we did in training
    
    # 3. Display the result
    st.success(f"## Estimated Annual Premium: ${predicted_premium[0]:,.2f}")
    
    # Just a fun visual addition
    st.balloons()