import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
import pymysql
import certifi

# ==========================================
# 1. DATABASE CONNECTION
# ==========================================
DB_CONFIG = {
    "host": "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "n4Chu4sX3kRNKGe.root",
    "password": "wnhFzYTE7JytAfAl",
    "database": "MK"
}

print("🔌 Connecting to TiDB Cloud...")
connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

engine = create_engine(
    connection_string,
    connect_args={"ssl": {"ca": certifi.where(), "check_hostname": False}}
)

# Set the visual style for all plots
sns.set_theme(style="whitegrid")

# ==========================================
# VISUALIZATION 1: Top 10 Batsmen by Runs
# ==========================================
print("📊 Generating Plot 1: Top 10 Batsmen...")
query1 = """
SELECT batter, SUM(batter_runs) as total_runs 
FROM deliveries 
GROUP BY batter 
ORDER BY total_runs DESC 
LIMIT 10;
"""
df1 = pd.read_sql(query1, engine)

plt.figure(figsize=(12, 6))
sns.barplot(data=df1, x='total_runs', y='batter', palette='viridis')
plt.title('Top 10 Batsmen by Total Runs', fontsize=16)
plt.xlabel('Total Runs')
plt.ylabel('Batsman')
plt.tight_layout()
plt.show()

# ==========================================
# VISUALIZATION 2: Dismissal Types (Pie Chart)
# ==========================================
print("📊 Generating Plot 2: Dismissal Types...")
query2 = """
SELECT wicket_type, COUNT(*) as count 
FROM deliveries 
WHERE is_wicket=1 
GROUP BY wicket_type;
"""
df2 = pd.read_sql(query2, engine)

plt.figure(figsize=(10, 10))
plt.pie(df2['count'], labels=df2['wicket_type'], autopct='%1.1f%%', startangle=140, colors=sns.color_palette('pastel'))
plt.title('Distribution of Wicket Types', fontsize=16)
plt.show()

# ==========================================
# VISUALIZATION 3: Match Outcome (Bat First vs Chase)
# ==========================================
print("📊 Generating Plot 3: Bat First vs Chase Wins...")
query3 = """
SELECT 
    SUM(CASE WHEN win_by_runs > 0 THEN 1 ELSE 0 END) as bat_first,
    SUM(CASE WHEN win_by_wickets > 0 THEN 1 ELSE 0 END) as chase
FROM matches;
"""
df3 = pd.read_sql(query3, engine)

# Reshape for plotting
data = {'Outcome': ['Batting First Wins', 'Chasing Wins'], 'Count': [df3.iloc[0]['bat_first'], df3.iloc[0]['chase']]}
df_plot3 = pd.DataFrame(data)

plt.figure(figsize=(8, 6))
sns.barplot(data=df_plot3, x='Outcome', y='Count', palette='coolwarm')
plt.title('Match Outcomes: Batting First vs Chasing', fontsize=16)
plt.ylabel('Number of Wins')
plt.show()

# ==========================================
# VISUALIZATION 4: Runs Scored Per Over (Death Overs Analysis)
# ==========================================
print("📊 Generating Plot 4: Runs per Over...")
# Note: Using backticks for `over` because it is a reserved keyword
query4 = """
SELECT `over`, SUM(total_runs) as total_runs 
FROM deliveries 
GROUP BY `over` 
ORDER BY `over`;
"""
df4 = pd.read_sql(query4, engine)

plt.figure(figsize=(14, 6))
sns.lineplot(data=df4, x='over', y='total_runs', marker='o', color='crimson', linewidth=2.5)
plt.title('Total Runs Scored Across Overs (0-20/50)', fontsize=16)
plt.xlabel('Over Number')
plt.ylabel('Total Runs Scored')
plt.grid(True)
plt.show()

# ==========================================
# VISUALIZATION 5: Toss Decision Distribution
# ==========================================
print("📊 Generating Plot 5: Toss Decisions...")
query5 = """
SELECT toss_decision, COUNT(*) as count 
FROM matches 
GROUP BY toss_decision;
"""
df5 = pd.read_sql(query5, engine)

plt.figure(figsize=(8, 6))
sns.barplot(data=df5, x='toss_decision', y='count', palette='magma')
plt.title('Toss Decision Analysis (Bat vs Field)', fontsize=16)
plt.xlabel('Toss Decision')
plt.ylabel('Count')
plt.show()

print("✅ EDA Visualizations Complete!")