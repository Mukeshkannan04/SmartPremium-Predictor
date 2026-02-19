import os
import json
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import certifi

# ==========================================
# CONFIGURATION
# ==========================================

# 1. Database Credentials (from your input)
DB_CONFIG = {
    "host": "gateway01.ap-southeast-1.prod.aws.tidbcloud.com",
    "port": 4000,
    "user": "n4Chu4sX3kRNKGe.root",
    "password": "wnhFzYTE7JytAfAl",
    "database": "MK"
}

# 2. Path to your downloaded Cricsheet JSON files
# IMPORTANT: Change this to the actual folder where you unzipped the files
# Example: If you have a folder named 'odi_json' inside 'data'
DATA_DIR = "D:\Project_2_folder\Data"
MATCH_TYPES = {
    'odi': 'odi_json',   # Folder name for ODIs
    't20': 't20_json',   # Folder name for T20s
    'test': 'test_json', # Folder name for Tests
    'ipl': 'ipl_json'    # Folder name for IPL
}

# ==========================================
# ETL FUNCTIONS
# ==========================================

def process_match_file(filepath, match_type):
    """Parses a single Cricsheet JSON file into flat dictionaries."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None, []

    # 1. Extract Match Metadata
    info = data.get('info', {})
    outcome = info.get('outcome', {})
    toss = info.get('toss', {})
    
    match_meta = {
        'match_id': os.path.basename(filepath).split('.')[0],
        'match_type': match_type,
        'date': info.get('dates', [None])[0],
        'venue': info.get('venue', 'Unknown'),
        'city': info.get('city', 'Unknown'),
        'team1': info.get('teams', ['T1', 'T2'])[0],
        'team2': info.get('teams', ['T1', 'T2'])[1],
        'toss_winner': toss.get('winner', None),
        'toss_decision': toss.get('decision', None),
        'winner': outcome.get('winner', 'No Result'),
        'win_by_runs': outcome.get('by', {}).get('runs', 0),
        'win_by_wickets': outcome.get('by', {}).get('wickets', 0),
        'player_of_match': info.get('player_of_match', [None])[0]
    }

    # 2. Extract Deliveries (Ball-by-Ball)
    deliveries_list = []
    if 'innings' in data:
        for inning_idx, inning in enumerate(data['innings']):
            team_batting = inning.get('team')
            
            # Handle old Cricsheet format vs new format
            overs_data = inning.get('overs', [])
            
            for over_data in overs_data:
                over_num = over_data.get('over')
                
                for ball_idx, ball_data in enumerate(over_data.get('deliveries', [])):
                    
                    # Wicket details
                    wicket_type = None
                    player_out = None
                    if 'wickets' in ball_data:
                        wicket_type = ball_data['wickets'][0]['kind']
                        player_out = ball_data['wickets'][0]['player_out']

                    deliveries_list.append({
                        'match_id': match_meta['match_id'],
                        'innings': inning_idx + 1,
                        'batting_team': team_batting,
                        'over': over_num,
                        'ball': ball_idx + 1,
                        'batter': ball_data['batter'],
                        'bowler': ball_data['bowler'],
                        'non_striker': ball_data['non_striker'],
                        'batter_runs': ball_data['runs']['batter'],
                        'extra_runs': ball_data['runs']['extras'],
                        'total_runs': ball_data['runs']['total'],
                        'is_wicket': 1 if 'wickets' in ball_data else 0,
                        'wicket_type': wicket_type,
                        'player_out': player_out
                    })
    
    return match_meta, deliveries_list

def load_data_to_db():
    all_matches = []
    all_deliveries = []

    print("🚀 Starting ETL Process...")

    # Iterate through defined match types
    for m_type, folder_name in MATCH_TYPES.items():
        folder_path = os.path.join(DATA_DIR, folder_name)
        
        if not os.path.exists(folder_path):
            print(f"⚠️ Warning: Folder not found: {folder_path}. Skipping.")
            continue
            
        files = [f for f in os.listdir(folder_path) if f.endswith('.json')]
        print(f"📂 Found {len(files)} files in {folder_name}. Processing...")

        # LIMITING TO 50 FILES FOR DEMO (Remove [:50] to process all)
        for file in files[:50]: 
            meta, balls = process_match_file(os.path.join(folder_path, file), m_type)
            if meta:
                all_matches.append(meta)
                all_deliveries.extend(balls)

    if not all_matches:
        print("❌ No data found. Check your DATA_DIR path.")
        return

    # Convert to DataFrames
    print("📊 Converting to DataFrames...")
    df_matches = pd.DataFrame(all_matches)
    df_deliveries = pd.DataFrame(all_deliveries)

    # Database Connection String
    # mysql+pymysql://user:password@host:port/database
    connection_string = (
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    print("🔌 Connecting to TiDB Cloud...")
    
    try:
        # Create Engine with SSL
        engine = create_engine(
            connection_string,
            connect_args={
                "ssl": {
                    "ca": certifi.where(),
                    "check_hostname": False
                }
            }
        )

        # Upload Matches Table
        print(f"💾 Uploading {len(df_matches)} matches to table 'matches'...")
        df_matches.to_sql(
            'matches', 
            engine, 
            if_exists='replace', # Use 'append' if you don't want to overwrite
            index=False,
            chunksize=1000 # Send data in chunks to avoid timeouts
        )

        # Upload Deliveries Table
        print(f"💾 Uploading {len(df_deliveries)} balls to table 'deliveries'...")
        df_deliveries.to_sql(
            'deliveries', 
            engine, 
            if_exists='replace', 
            index=False,
            chunksize=5000 # Larger chunksize for deliveries
        )

        print("✅ ETL Complete! Data successfully loaded into TiDB.")

    except Exception as e:
        print(f"❌ Database Error: {e}")

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    # Ensure your data folders exist before running!
    load_data_to_db()