import time
import os
import pandas as pd
import logging
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto
from telethon.sync import TelegramClient
import asyncio

api_id = '22908602'
api_hash = '20bdd2238cc3dbb7014870281cce444e'
client = TelegramClient('session_name', api_id, api_hash)
async def main():
    await client.start()
    await scrape_channel_messages('@yetenaweg', max_messages=10000, batch_size=100, sleep_time=0.2)

# Correct way to run the async main function
if __name__ == "__main__":
    asyncio.run(main())  # Use this to run the main async function

async def scrape_channel_messages(channel_username, max_messages=10000, batch_size=100, sleep_time=0.2):
    # Scraping logic here...
    pass

async def main():
    await client.start()
    await scrape_channel_messages('@yetenaweg', max_messages=10000, batch_size=100, sleep_time=0.2)

# Run the main function
asyncio.run(main())  # This should be used in a script to run the async main function.


# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(levelname)s - %(message)s',  # Specify the format of log messages
    filename='scraping_images.log',  # Log messages will be saved to this file
    filemode='w'  # 'w' overwrites the file on each run, 'a' will append to the file
)

# Directory to save images
output_directory = r'C:\Users\Yibabe\Desktop\10academykifiyaAIMweek-7\data\images1'
os.makedirs(output_directory, exist_ok=True)  # Create the directory if it doesn't exist

async def scrape_image_messages(channel_username, max_images=10000, batch_size=100, sleep_time=2):
    logging.info(f"Started scraping images from channel: {channel_username}")

    # Get the channel entity
    channel = await client.get_entity(channel_username)
    offset_id = 0
    all_images = []

    while len(all_images) < max_images:
        logging.info(f"Scraping images... Collected {len(all_images)} so far.")
        try:
            # Fetch the message history
            history = await client(GetHistoryRequest(
                peer=channel,
                offset_id=offset_id,
                offset_date=None,
                add_offset=0,
                limit=batch_size,
                max_id=0,
                min_id=0,
                hash=0
            ))

            if not history.messages:
                logging.info("No more images to scrape.")
                break

            messages = history.messages
            for msg in messages:
                if isinstance(msg.media, MessageMediaPhoto):
                    try:
                        image_file = await client.download_media(msg, output_directory)
                        if image_file:
                            all_images.append({
                                'ID': msg.id,
                                'Date': msg.date,
                                'Image Path': image_file
                            })
                    except Exception as e:
                        logging.error(f"Error downloading image for message ID {msg.id}: {e}")

            offset_id = messages[-1].id
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"Error while fetching image messages: {e}")
            break

    all_images = all_images[:max_images]
