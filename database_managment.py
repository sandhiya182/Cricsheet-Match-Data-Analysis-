import sqlite3
import pandas as pd
import os

class CricketDatabaseManager:
    def __init__(self, db_path, datasets_dir):
        self.db_path = db_path
        self.datasets_dir = datasets_dir
        self.conn = None

    def connect(self):
        """Establish a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(self.db_path)
            print("Connected to SQLite database successfully.")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def close_connection(self):
        """Close the SQLite database connection."""
        if self.conn:
            self.conn.close()
            print("Database connection closed.")

    def create_tables(self):
        """Create tables for Test, ODI, and T20 matches."""
        if not self.conn:
            print("No database connection.")
            return
        
        cursor = self.conn.cursor()
        tables = {
            "test_matches": """
            CREATE TABLE IF NOT EXISTS test_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                innings TEXT,
                data_version TEXT,
                created TEXT,
                revision TEXT,
                balls_per_over INTEGER,
                city TEXT,
                dates TEXT,
                event_match_number TEXT,
                event_name TEXT,
                gender TEXT,
                match_type TEXT,
                match_type_number INTEGER,
                officials_match_referees TEXT,
                officials_reserve_umpires TEXT,
                officials_tv_umpires TEXT,
                officials_umpires TEXT,
                outcome_by_runs INTEGER,
                outcome_winner TEXT,
                player_of_match TEXT,
                season TEXT,
                team_type TEXT,
            );
            """,
            "odi_matches": """
            CREATE TABLE IF NOT EXISTS odi_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                innings TEXT,
                data_version TEXT,
                created TEXT,
                revision TEXT,
                balls_per_over INTEGER,
                city TEXT,
                dates TEXT,
                event_match_number TEXT,
                event_name TEXT,
                gender TEXT,
                match_type TEXT,
                match_type_number INTEGER,
                officials_match_referees TEXT,
                officials_reserve_umpires TEXT,
                officials_tv_umpires TEXT,
                officials_umpires TEXT,
                outcome_by_runs INTEGER,
                outcome_winner TEXT,
                overs REAL,
                player_of_match TEXT,
                season TEXT
            );
            """,
            "t20_matches": """
            CREATE TABLE IF NOT EXISTS t20_matches (
                match_id INTEGER PRIMARY KEY AUTOINCREMENT,
                innings TEXT,
                data_version TEXT,
                created TEXT,
                revision TEXT,
                balls_per_over INTEGER,
                dates TEXT,
                event_match_number TEXT,
                event_name TEXT,
                gender TEXT,
                match_type TEXT,
                match_type_number INTEGER,
                officials_match_referees TEXT,
                officials_reserve_umpires TEXT,
                officials_tv_umpires TEXT,
                officials_umpires TEXT,
                outcome_by_wickets INTEGER,
                outcome_winner TEXT,
                overs REAL,
                player_of_match TEXT,
                season TEXT,
                team_type TEXT
            );
            """
        }

        try:
            for table_name, create_table_sql in tables.items():
                cursor.execute(create_table_sql)
            self.conn.commit()
            print("Tables created successfully.")
        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")

    def insert_data(self, match_type, csv_path, row_limit=500):
        """Insert data into the appropriate table based on match type."""
        if not self.conn:
            print("No database connection.")
            return

        columns_map = {
            "test": ['innings', 'data_version', 'created', 'revision', 'balls_per_over', 
                     'city', 'dates', 'event_match_number', 'event_name', 'gender', 'match_type',
                     'match_type_number', 'officials_match_referees', 'officials_reserve_umpires',
                     'officials_tv_umpires', 'officials_umpires', 'outcome_by_runs', 'outcome_winner',
                     'player_of_match', 'season', 'team_type'],
            "odi": ['innings', 'data_version', 'created', 'revision', 'balls_per_over', 
                    'city', 'dates', 'event_match_number', 'event_name', 'gender', 'match_type',
                    'match_type_number', 'officials_match_referees', 'officials_reserve_umpires',
                    'officials_tv_umpires', 'officials_umpires', 'outcome_by_runs', 'outcome_winner',
                    'overs', 'player_of_match', 'season'],
            "t20": ['innings', 'data_version', 'created', 'revision', 'balls_per_over', 
                    'dates', 'event_match_number', 'event_name', 'gender', 'match_type',
                    'match_type_number', 'officials_match_referees', 'officials_reserve_umpires',
                    'officials_tv_umpires', 'officials_umpires', 'outcome_by_wickets', 'outcome_winner',
                    'overs', 'player_of_match', 'season', 'team_type']
        }

        try:
            df = pd.read_csv(csv_path, low_memory=False)
            valid_columns = columns_map.get(match_type, [])
            df = df[valid_columns].iloc[:row_limit]
            df.to_sql(f"{match_type}_matches", self.conn, if_exists='append', index=False)
            print(f"Inserted {len(df)} rows into {match_type}_matches table.")
        except Exception as e:
            print(f"Error inserting data for {match_type}: {e}")

def main():
    db_manager = CricketDatabaseManager("cricket_data.sqlite", "datasets")
    db_manager.connect()

    try:
        # Create tables
        db_manager.create_tables()

        # Insert data for all match types
        match_types = {
            "test": os.path.join("datasets", "tests_matches.csv"),
            "odi": os.path.join("datasets", "odis_matches.csv"),
            "t20": os.path.join("datasets", "t20s_matches.csv")
        }

        for match_type, csv_path in match_types.items():
            if os.path.exists(csv_path):
                db_manager.insert_data(match_type, csv_path, row_limit=500)
            else:
                print(f"File not found: {csv_path}")
    finally:
        db_manager.close_connection()

if __name__ == "__main__":
    main()