import logging
import os
import csv
import json
import sys
from telethon import TelegramClient
from dotenv import load_dotenv

# Setting up logging
logging.basicConfig(
    filename='../logs/text_scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv('../.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Function to read channels from a JSON file
def load_channels_from_json(file_path):
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        return []
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('channels', [])
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON format: {e}")
        return []

# Function to scrape messages from a single Telegram channel
async def scrape_channel(client, channel_username, writer, num_messages):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title

        message_count = 0
        async for message in client.iter_messages(entity):
            if message_count >= num_messages:
                break

            # Skip empty messages
            if message.message:
                writer.writerow([channel_title, channel_username, message.id, message.message, message.date])
                logging.info(f"Processed message ID {message.id} from {channel_username}.")
                message_count += 1

        if message_count == 0:
            logging.info(f"No text messages found for {channel_username}.")
    except Exception as e:
        logging.error(f"Error while scraping {channel_username}: {e}")

# Initialize the client
client = TelegramClient('text_scraping_session', api_id, api_hash)

async def main():
    try:
        await client.start(phone)
        logging.info("Client started successfully.")

        # Load channels from JSON file
        channels = load_channels_from_json('../data/channels.json')

        num_messages_to_scrape = 500  # Set limit for the number of messages per channel

        for channel in channels:
            # Create a CSV file named after the channel
            csv_filename = f"../data/{channel[1:]}_text_data.csv"  # Remove '@' from channel name
            with open(csv_filename, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                # CSV header
                writer.writerow(['Channel Title', 'Channel Username', 'Message ID', 'Message Content', 'Message Date'])
                
                await scrape_channel(client, channel, writer, num_messages_to_scrape)
                logging.info(f"Scraped text data from {channel}.")
    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
