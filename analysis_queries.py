import pandas as pd
from sqlalchemy import create_engine
import pymysql
import certifi

# --- DATABASE CONNECTION (SAME AS BEFORE) ---
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

def run_query(title, sql_query):
    print(f"\n--- {title} ---")
    try:
        df = pd.read_sql(sql_query, engine)
        print(df.head(10)) # Print top 10 rows
        return df
    except Exception as e:
        print(f"Error: {e}")

# ==========================================
# 1. TOP 10 BATSMEN BY TOTAL RUNS (ALL FORMATS)
# ==========================================
q1 = """
SELECT batter, SUM(batter_runs) AS total_runs
FROM deliveries
GROUP BY batter
ORDER BY total_runs DESC
LIMIT 10;
"""
run_query("Top 10 Batsmen", q1)

# ==========================================
# 2. MOST WICKETS BY A BOWLER
# ==========================================
q2 = """
SELECT bowler, SUM(is_wicket) AS total_wickets
FROM deliveries
WHERE wicket_type NOT IN ('run out', 'retired hurt')
GROUP BY bowler
ORDER BY total_wickets DESC
LIMIT 10;
"""
run_query("Top 10 Bowlers", q2)

# ==========================================
# 3. HIGHEST TEAM TOTALS IN A SINGLE MATCH
# ==========================================
q3 = """
SELECT match_id, batting_team, SUM(total_runs) AS team_score
FROM deliveries
GROUP BY match_id, batting_team
ORDER BY team_score DESC
LIMIT 5;
"""
run_query("Highest Team Totals", q3)

# ==========================================
# 4. MATCHES WON BY THE LARGEST MARGIN (RUNS)
# ==========================================
q4 = """
SELECT date, team1, team2, winner, win_by_runs
FROM matches
WHERE win_by_runs > 0
ORDER BY win_by_runs DESC
LIMIT 5;
"""
run_query("Biggest Wins (Runs)", q4)

# ==========================================
# 5. NUMBER OF CENTURIES SCORED
# ==========================================
q5 = """
SELECT batter, COUNT(*) as centuries
FROM (
    SELECT match_id, batter, SUM(batter_runs) as score
    FROM deliveries
    GROUP BY match_id, batter
    HAVING score >= 100
) as innings_scores
GROUP BY batter
ORDER BY centuries DESC
LIMIT 5;
"""
run_query("Most Centuries", q5)