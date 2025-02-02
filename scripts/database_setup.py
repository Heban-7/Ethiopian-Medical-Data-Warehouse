import pandas as pd
import os, sys
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Get working directory
sys.path.append(os.path.abspath('..'))

# Ensure logs folder exist
os.makedirs('../logs', exist_ok=True)

# Cinfigure logging
logging.basicConfig(
    level=logging.INFO,
    format= "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('../logs/database_setup.log'),
        logging.StreamHandler()
    ]
)

# Load environment variables
load_dotenv("../.env")

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

def get_database_connection():
    """Connect with postgreSQL DataBase"""
    try:
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            connection.execute(text('SELECT 1'))
        logging.info("Successfully connected to the PostgreSQL.")
        return engine
    except Exception as e:
        logging.error(f"Database connection failed")
        raise

def create_table(engine):
    """Create table for storing telegram data"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS telegram_medical_message (
        id SERIAL PRIMARY KEY,
        channel_title TEXT,
        channel_username TEXT,
        message_id BIGINT UNIQUE,
        message TEXT,
        message_date TIMESTAMP,
        emoji_used TEXT,     
        youtube_links TEXT
    );
    """
    try:
        with engine.connect().execution_options(isolation_level = "AUTOCOMMIT") as connection:
            connection.execute(text(create_table_query))
            logging.info("Table 'Telegram_Medical_Message' created succussfully")
    except Exception as e:
        logging.error(f"Error Creating Table")
        raise

def insert_data(engine, cleaned_df):
    """ Inserts cleaned Telegram data into PostgreSQL database. """
    try:
        # Convert NaT timestamps to None (NULL in SQL)
        cleaned_df["message_date"] = cleaned_df["message_date"].apply(lambda x: None if pd.isna(x) else str(x))

        insert_query = """
        INSERT INTO telegram_medical_message 
        (channel_title,channel_username,message_id,message,message_date,emoji_used,youtube_links) 
        VALUES (:channel_title, :channel_username, :message_id, :message, :message_date, :emoji_used, :youtube_links)
        ON CONFLICT (message_id) DO NOTHING;
        """

        with engine.begin() as connection:  # Auto-commit enabled
            for _, row in cleaned_df.iterrows():
                connection.execute(
                    text(insert_query),
                    {
                        "channel_title": row["channel_title"],
                        "channel_username": row["channel_username"],
                        "message_id": row["message_id"],
                        "message": row["message"],
                        "message_date": row["message_date"],
                        "emoji_used": row["emoji_used"],
                        "youtube_links": row["youtube_links"]
                    }
                )
                

        logging.info(f"{len(cleaned_df)} records inserted into PostgreSQL database.")
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        raise



