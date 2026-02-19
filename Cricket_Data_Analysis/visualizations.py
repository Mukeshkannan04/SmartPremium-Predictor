import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import pymysql
import certifi

# --- DATABASE CONNECTION ---
DB_CONFIG = {
    "host": "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "n4Chu4sX3kRNKGe.root",
    "password": "wnhFzYTE7JytAfAl",
    "database": "MK"
}

connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

engine = create_engine(
    connection_string,
    connect_args={"ssl": {"ca": certifi.where(), "check_hostname": False}}
)

print("Fetching data for visualizations...")

try:
    # Query 1: Get Dismissal Types
    df_dismissals = pd.read_sql("SELECT wicket_type, COUNT(*) as count FROM deliveries WHERE is_wicket=1 GROUP BY wicket_type", engine)

    # Query 2: Runs per Over (FIXED: Added backticks around `over`)
    df_runs_over = pd.read_sql("SELECT `over`, SUM(total_runs) as runs FROM deliveries GROUP BY `over`", engine)

    # --- PLOT 1: Dismissal Types (Pie Chart) ---
    if not df_dismissals.empty:
        plt.figure(figsize=(10, 8))
        plt.pie(df_dismissals['count'], labels=df_dismissals['wicket_type'], autopct='%1.1f%%')
        plt.title('Distribution of Wicket Types')
        plt.show()
    else:
        print("No wicket data found to plot.")

    # --- PLOT 2: Runs Scored Across Overs (Bar Chart) ---
    if not df_runs_over.empty:
        plt.figure(figsize=(12, 6))
        sns.barplot(data=df_runs_over, x='over', y='runs', palette='magma')
        plt.title('Total Runs Scored in Each Over Number')
        plt.xlabel('Over Number')
        plt.ylabel('Total Runs')
        plt.show()
    else:
        print("No run data found to plot.")

except Exception as e:
    print(f"An error occurred: {e}")