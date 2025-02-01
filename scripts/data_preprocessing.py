import pandas as pd 
import logging
import os
import sys
import re 
import emoji 

# Get working direcotry
sys.path.append(os.path.abspath('..'))

# Configure logging to write to file and display in jupyter notebook
logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("../logs/data_preprocessing.log"),
        logging.StreamHandler()
    ]
)

# Data importing
def load_data(file_path):
    """Load csv file into pandas dataframe. """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"CSV file; {file_path} loaded successefully.")
        return df
    except Exception as e:
        logging.error(f"Error Loading CSV file {e}")
        raise

# Merge dataframe
def merge_all_files(files):
    """Merge all file that scraped from deffirent telegram channel"""
    try:
        merged_df = pd.concat(files, ignore_index=True)
        logging.info("All Dataframes Marged Successefully")
        return merged_df
    except Exception as e:
        logging.error(f"Error Merging file: {e}")
        raise



# Emoji Extraction fro text 
def extract_emojis(text):
    """Extrat emoji frim the message, return 'No Emoji' if none id found"""
    emojis = ''.join(c for c in text if c in emoji.EMOJI_DATA)
    return emojis if emojis else "No Emoji"

# Remove emojis from data
def remove_emojis(text):
    """Remove emojis from the message text."""
    return ''.join(c for c in text if c not in emoji.EMOJI_DATA)

# Extract youtube link
def extract_youtube_links(text):
    """Etract youtube link from text and return 'NO YouTube Link' if none is found"""
    youtube_pattern = r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
    links = re.findall(youtube_pattern, text)
    return ', '.join(links) if links else 'No YouTube Link'

# Remove YouTube links 
def remove_youtube_links(text):
    """Remove youtube links from the text message"""
    youtube_pattern = r"(https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s]+)"
    return re.sub(youtube_pattern, '', text).strip()

# Remove links
def remove_links(text):
    """Remove all URLs including  Telegram, TikTok, and any other website links."""
    link_pattern = r"https?://[^\s]+|www\.[^\s]+" 
    return re.sub(link_pattern, '', text).strip()

# clean text 
def clean_text(text):
    """Standerdize text by removing newline and uneccessarly spaces"""
    if pd.isna(text):
        return "No Message"
    return re.sub('\n+', ' ', text).strip()

# clean dataframe 
def clean_dataframe(df):
    """Perform all cleaning and preprocessing"""
    try:
        # Remove a data duplication and ensure a new copy
        df = df.drop_duplicates(subset = ['Message ID']).copy()
        logging.info("Duplications Removed from the DataSet.")

        # Convart data into datetime format 
        df.loc[:, 'Message Date'] = pd.to_datetime(df['Message Date'], errors = 'coerce')
        df.loc[:, 'Message Date'] = df['Message Date'].where(df['Message Date'].notna(), None)
        logging.info("Date column formatted to datetime.")

        # Convert Message ID into integer
        df.loc[:, "Message ID"] = pd.to_numeric(df['Message ID'], errors='coerce').fillna(0).astype(int)
        logging.info('Convert Message Id into numerical columns')

        # Fill missing value 
        df.loc[:, 'Message Content'] = df['Message Content'].fillna('No Message')
        logging.info('Missing value fillid')

        # Standardize text columns
        df.loc[:, 'Channel Title'] = df['Channel Title'].str.strip()
        df.loc[:, 'Channel Username'] = df['Channel Username'].str.strip()
        df.loc[:, 'Message Content'] = df['Message Content'].apply(clean_text)
        logging.info("Text columns standardized.")

         # Extract emojis and store them in a new column
        df.loc[:, 'emoji_used'] = df['Message Content'].apply(extract_emojis)
        logging.info("Emojis extracted and stored in 'emoji_used' column.")
        
        # Remove emojis from message text
        df.loc[:, 'Message Content'] = df['Message Content'].apply(remove_emojis)

        # Extract YouTube links into a separate column
        df.loc[:, 'youtube_links'] = df['Message Content'].apply(extract_youtube_links)
        logging.info("ouTube links extracted and stored in 'youtube_links' column.")

        # Remove YouTube links from message text
        df.loc[:, 'Message Content'] = df['Message Content'].apply(remove_youtube_links)

        # Remove links frorm message text
        df.loc[:, 'Message Content'] = df['Message Content'].apply(remove_links)

        # Rename columns to match PostgreSQL schema
        df = df.rename(columns={
            "Channel Title": "channel_title",
            "Channel Username": "channel_username",
            "Message ID": "message_id",
            "Message Content": "message",
            "Message Date": "message_date",
            "emoji_used": "emoji_used",
            "youtube_links": "youtube_links"
        })

        logging.info("Data cleaning completed successfully.")
        return df
    except Exception as e:
        logging.error(f"Data cleaning error: {e}")
        raise

def save_cleaned_data(df, output_path):
    """ Save cleaned data to a new CSV file. """
    try:
        df.to_csv(output_path, index=False)
        logging.info(f"Cleaned data saved successfully to '{output_path}'.")
        print(f"Cleaned data saved successfully to '{output_path}'.")
    except Exception as e:
        logging.error(f"Error saving cleaned data: {e}")
        raise
       



