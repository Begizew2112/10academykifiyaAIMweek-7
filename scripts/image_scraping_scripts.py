import time
import os
import pandas as pd
import logging
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.sync import TelegramClient

import asyncio

api_id = '22908602'
api_hash = '20bdd2238cc3dbb7014870281cce444e'
client = TelegramClient('session_name', api_id, api_hash)
# This structure is correct for running your async functions
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
    filename='scraping_messages.log',  # Log messages will be saved to this file
    filemode='w'  # 'w' overwrites the file on each run, 'a' will append to the file
)

async def scrape_channel_messages(channel_username, max_messages=10000, batch_size=100, sleep_time=2):
    logging.info(f"Started scraping messages from channel: {channel_username}")

    # Get the channel entity
    channel = await client.get_entity(channel_username)
    offset_id = 0
    all_messages = []

    # Extract channel details
    channel_title = channel.title if hasattr(channel, 'title') else 'N/A'
    
    while len(all_messages) < max_messages:
        logging.info(f"Scraping messages... Collected {len(all_messages)} so far.")
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
                logging.info("No more messages to scrape.")
                break

            messages = history.messages
            all_messages.extend(messages)
            offset_id = messages[-1].id
            time.sleep(sleep_time)
        except Exception as e:
            logging.error(f"Error while fetching messages: {e}")
            break

    all_messages = all_messages[:max_messages]
    
    # Prepare the data for CSV export
    data = []
    for msg in all_messages:
        try:
            if msg.message:
                media_path = ''
                if msg.media:
                    try:
                        if hasattr(msg.media, 'document'):
                            media_path = msg.media.document.attributes[0].file_name
                        elif hasattr(msg.media, 'photo'):
                            media_path = 'Photo media'
                        elif hasattr(msg.media, 'video'):
                            media_path = 'Video media'
                        else:
                            media_path = 'Other media'
                    except Exception as media_error:
                        logging.error(f"Error processing media: {media_error}")
                        media_path = 'Error processing media'

                data.append({
                    'Channel Title': channel_title,
                    'Channel Username': channel_username,
                    'ID': msg.id,
                    'Message': msg.message,
                    'Date': msg.date,
                    'Media Path': media_path
                })
        except Exception as msg_error:
            logging.error(f"Error processing message ID {msg.id}: {msg_error}")

