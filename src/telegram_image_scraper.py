import logging
import os
import csv
import sys
from telethon import TelegramClient
from dotenv import load_dotenv

# get working directory
sys.path.append(os.path.abspath('..'))

# Set up logging
logging.basicConfig(
    filename='../logs/scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Load environment variables
load_dotenv('../.env')
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')
phone = os.getenv('phone')

# Function to scrape photos from specific channels
async def scrape_photos_only(client, channel_username, media_dir, num_messages):
    try:
        entity = await client.get_entity(channel_username)
        channel_title = entity.title

        message_count = 0
        async for message in client.iter_messages(entity):
            if message_count >= num_messages:
                break

            if message.media and hasattr(message.media, 'photo'):
                filename = f"{channel_username}_{message.id}.jpg"
                media_path = os.path.join(media_dir, filename)
                await client.download_media(message.media, media_path)
                logging.info(f"Downloaded image for message ID {message.id} from {channel_username}.")

                message_count += 1

        if message_count == 0:
            logging.info(f"No photos found for {channel_username}.")

    except Exception as e:
        logging.error(f"Error while scraping photos from {channel_username}: {e}")

# Initialize the client
client = TelegramClient('scraping_session', api_id, api_hash)

async def main():
    try:
        await client.start(phone)
        logging.info("Client started successfully.")

        media_dir = '../data/photos'
        os.makedirs(media_dir, exist_ok=True)

        # Channels for photo extraction only
        photo_channels = ["@CheMed123", "@lobelia4cosmetics"]

        num_messages_to_scrape = 10000  # Number of messages to scrape

        for channel in photo_channels:
            await scrape_photos_only(client, channel, media_dir, num_messages_to_scrape)
            logging.info(f"Scraped photos from {channel}.")

    except Exception as e:
        logging.error(f"Error in main function: {e}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
