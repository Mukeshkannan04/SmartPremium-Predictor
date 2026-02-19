import pandas as pd
from sqlalchemy import create_engine, text
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

connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

engine = create_engine(
    connection_string,
    connect_args={"ssl": {"ca": certifi.where(), "check_hostname": False}}
)

def run_query(title, sql_query):
    print(f"\n--- {title} ---")
    try:
        # We use pandas to execute and format the output nicely
        df = pd.read_sql(sql_query, engine)
        if df.empty:
            print("No data found.")
        else:
            print(df.head(10).to_string(index=False)) # Print top 10 rows without index
    except Exception as e:
        print(f"Error: {e}")

# ==========================================
# CATEGORY A: BATTING ANALYSIS
# ==========================================

# 1. Top 10 Run Scorers (All Formats)
q1 = """
SELECT batter, SUM(batter_runs) AS total_runs
FROM deliveries
GROUP BY batter
ORDER BY total_runs DESC
LIMIT 10;
"""
run_query("1. Top 10 Run Scorers", q1)

# 2. Most Sixes Hit by a Batter
q2 = """
SELECT batter, COUNT(*) AS sixes
FROM deliveries
WHERE batter_runs = 6
GROUP BY batter
ORDER BY sixes DESC
LIMIT 10;
"""
run_query("2. Most Sixes Hit", q2)

# 3. Most Fours Hit by a Batter
q3 = """
SELECT batter, COUNT(*) AS fours
FROM deliveries
WHERE batter_runs = 4
GROUP BY batter
ORDER BY fours DESC
LIMIT 10;
"""
run_query("3. Most Fours Hit", q3)

# 4. Highest Individual Score in a Single Match
q4 = """
SELECT match_id, batter, SUM(batter_runs) AS score
FROM deliveries
GROUP BY match_id, batter
ORDER BY score DESC
LIMIT 10;
"""
run_query("4. Highest Individual Scores", q4)

# 5. Batters with Most Centuries
q5 = """
SELECT batter, COUNT(*) AS centuries
FROM (
    SELECT match_id, batter, SUM(batter_runs) AS score
    FROM deliveries
    GROUP BY match_id, batter
    HAVING score >= 100
) AS sub
GROUP BY batter
ORDER BY centuries DESC
LIMIT 10;
"""
run_query("5. Most Centuries", q5)

# 6. Best Strike Rate (Min 100 balls faced)
q6 = """
SELECT batter, 
       (SUM(batter_runs) / COUNT(*)) * 100 AS strike_rate,
       COUNT(*) as balls_faced
FROM deliveries
GROUP BY batter
HAVING balls_faced > 100
ORDER BY strike_rate DESC
LIMIT 10;
"""
run_query("6. Best Strike Rate (Min 100 balls)", q6)

# ==========================================
# CATEGORY B: BOWLING ANALYSIS
# ==========================================

# 7. Top 10 Wicket Takers
q7 = """
SELECT bowler, COUNT(*) AS wickets
FROM deliveries
WHERE is_wicket = 1 AND wicket_type NOT IN ('run out', 'retired hurt')
GROUP BY bowler
ORDER BY wickets DESC
LIMIT 10;
"""
run_query("7. Leading Wicket Takers", q7)

# 8. Best Economy Rate (Min 60 balls bowled)
q8 = """
SELECT bowler, 
       (SUM(total_runs) / (COUNT(*) / 6.0)) AS economy_rate,
       COUNT(*) as balls_bowled
FROM deliveries
GROUP BY bowler
HAVING balls_bowled > 60
ORDER BY economy_rate ASC
LIMIT 10;
"""
run_query("8. Best Economy Rate", q8)

# 9. Most Dot Balls Bowled
q9 = """
SELECT bowler, COUNT(*) AS dot_balls
FROM deliveries
WHERE total_runs = 0
GROUP BY bowler
ORDER BY dot_balls DESC
LIMIT 10;
"""
run_query("9. Most Dot Balls", q9)

# 10. Bowlers who conceded the most Extras
q10 = """
SELECT bowler, SUM(extra_runs) AS total_extras
FROM deliveries
GROUP BY bowler
ORDER BY total_extras DESC
LIMIT 10;
"""
run_query("10. Most Extras Conceded", q10)

# ==========================================
# CATEGORY C: TEAM ANALYSIS
# ==========================================

# 11. Highest Team Totals
q11 = """
SELECT match_id, batting_team, SUM(total_runs) AS team_score
FROM deliveries
GROUP BY match_id, batting_team
ORDER BY team_score DESC
LIMIT 10;
"""
run_query("11. Highest Team Totals", q11)

# 12. Most Wins by a Team
q12 = """
SELECT winner, COUNT(*) AS wins
FROM matches
WHERE winner != 'No Result'
GROUP BY winner
ORDER BY wins DESC
LIMIT 10;
"""
run_query("12. Most Wins", q12)

# 13. Toss Winner vs Match Winner Correlation
q13 = """
SELECT 
    (SUM(CASE WHEN toss_winner = winner THEN 1 ELSE 0 END) / COUNT(*)) * 100 AS toss_win_match_win_pct
FROM matches
WHERE winner != 'No Result';
"""
run_query("13. Win % if Toss Won", q13)

# 14. Wins by Batting First vs Chasing
q14 = """
SELECT 
    SUM(CASE WHEN win_by_runs > 0 THEN 1 ELSE 0 END) AS batting_first_wins,
    SUM(CASE WHEN win_by_wickets > 0 THEN 1 ELSE 0 END) AS chasing_wins
FROM matches;
"""
run_query("14. Bat First vs Chase Wins", q14)

# 15. Average Score in 1st Innings vs 2nd Innings
q15 = """
SELECT innings, AVG(total_score) as avg_score
FROM (
    SELECT match_id, innings, SUM(total_runs) as total_score
    FROM deliveries
    WHERE innings <= 2
    GROUP BY match_id, innings
) as scores
GROUP BY innings;
"""
run_query("15. Avg Score per Innings", q15)

# ==========================================
# CATEGORY D: MATCH & VENUE ANALYSIS
# ==========================================

# 16. Largest Margin of Victory (Runs)
q16 = """
SELECT date, team1, team2, winner, win_by_runs
FROM matches
WHERE win_by_runs > 0
ORDER BY win_by_runs DESC
LIMIT 5;
"""
run_query("16. Largest Win Margin (Runs)", q16)

# 17. Most Matches Played at a Venue
q17 = """
SELECT city, COUNT(*) AS matches_hosted
FROM matches
GROUP BY city
ORDER BY matches_hosted DESC
LIMIT 10;
"""
run_query("17. Top Venues", q17)

# 18. Most "Player of the Match" Awards
q18 = """
SELECT player_of_match, COUNT(*) AS awards
FROM matches
WHERE player_of_match IS NOT NULL
GROUP BY player_of_match
ORDER BY awards DESC
LIMIT 10;
"""
run_query("18. Most Player of Match Awards", q18)

# 19. Dismissal Types Distribution
q19 = """
SELECT wicket_type, COUNT(*) AS frequency
FROM deliveries
WHERE is_wicket = 1
GROUP BY wicket_type
ORDER BY frequency DESC;
"""
run_query("19. Dismissal Types", q19)

# 20. Total Boundary Runs vs Running Runs
q20 = """
SELECT 
    SUM(CASE WHEN batter_runs IN (4, 6) THEN batter_runs ELSE 0 END) AS boundary_runs,
    SUM(CASE WHEN batter_runs NOT IN (4, 6) THEN batter_runs ELSE 0 END) AS running_runs
FROM deliveries;
"""
run_query("20. Boundaries vs Running", q20)