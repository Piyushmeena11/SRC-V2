# ---------------------------------------------------
# File Name: db.py
# Description: A Pyrogram bot for downloading files from Telegram channels or groups 
#              and uploading them back to Telegram.
# Author: Gagan
# GitHub: https://github.com/devgaganin/
# Telegram: https://t.me/team_spy_pro
# YouTube: https://youtube.com/@dev_gagan
# Created: 2025-01-11
# Last Modified: 2025-01-11
# Version: 2.0.5
# License: MIT License
# ---------------------------------------------------

from config import MONGO_DB
from motor.motor_asyncio import AsyncIOMotorClient as MongoCli
mongo = MongoCli(MONGO_DB)
db = mongo.user_data
db = db.users_data_db
async def get_data(user_id):
    x = await db.find_one({"_id": user_id})
    return x
async def set_thumbnail(user_id, thumb):
    data = await get_data(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"thumb": thumb}})
    else:
        await db.insert_one({"_id": user_id, "thumb": thumb})
async def set_caption(user_id, caption):
    data = await get_data(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"caption": caption}})
    else:
        await db.insert_one({"_id": user_id, "caption": caption})
async def replace_caption(user_id, replace_txt, to_replace):
    data = await get_data(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"replace_txt": replace_txt, "to_replace": to_replace}})
    else:
        await db.insert_one({"_id": user_id, "replace_txt": replace_txt, "to_replace": to_replace})
async def set_session(user_id, session):
    data = await get_data(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"session": session}})
    else:
        await db.insert_one({"_id": user_id, "session": session})
async def clean_words(user_id, new_clean_words):
    data = await get_data(user_id)
    if data and data.get("_id"):
        existing_words = data.get("clean_words", [])
         
        if existing_words is None:
            existing_words = []
        updated_words = list(set(existing_words + new_clean_words))
        await db.update_one({"_id": user_id}, {"$set": {"clean_words": updated_words}})
    else:
        await db.insert_one({"_id": user_id, "clean_words": new_clean_words})
async def remove_clean_words(user_id, words_to_remove):
    data = await get_data(user_id)
    if data and data.get("_id"):
        existing_words = data.get("clean_words", [])
        updated_words = [word for word in existing_words if word not in words_to_remove]
        await db.update_one({"_id": user_id}, {"$set": {"clean_words": updated_words}})
    else:
        await db.insert_one({"_id": user_id, "clean_words": []})
async def set_channel(user_id, chat_id):
    data = await get_data(user_id)
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"chat_id": chat_id}})
    else:
        await db.insert_one({"_id": user_id, "chat_id": chat_id})
async def all_words_remove(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"clean_words": None}})
async def remove_thumbnail(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"thumb": None}})
async def remove_caption(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"caption": None}})
async def remove_replace(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"replace_txt": None, "to_replace": None}})
 
async def remove_session(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"session": None}})
async def remove_channel(user_id):
    await db.update_one({"_id": user_id}, {"$set": {"chat_id": None}})
async def delete_session(user_id):
    """Delete the session associated with the given user_id from the database."""
    await db.update_one({"_id": user_id}, {"$unset": {"session": ""}})

# Topic batch ID storage helpers
async def set_topic_msg_ids(user_id, chat_id, topic_id, msg_ids):
    import time
    data = await get_data(user_id)
    cache_item = {
        "chat_id": chat_id,
        "topic_id": topic_id,
        "msg_ids": msg_ids,
        "timestamp": time.time()
    }
    if data and data.get("_id"):
        await db.update_one({"_id": user_id}, {"$set": {"topic_msg_cache": cache_item}})
    else:
        await db.insert_one({"_id": user_id, "topic_msg_cache": cache_item})

async def get_topic_msg_ids(user_id, chat_id, topic_id):
    import time
    data = await get_data(user_id)
    cache = data.get("topic_msg_cache", None) if data else None
    if cache:
        if cache.get("chat_id") == chat_id and cache.get("topic_id") == topic_id:
            ts = cache.get("timestamp", 0)
            if (time.time() - ts) < 72 * 3600:
                return cache.get("msg_ids", [])
    return []

async def clear_topic_msg_ids(user_id):
    await db.update_one({"_id": user_id}, {"$unset": {"topic_msg_cache": "", "topic_msg_ids": ""}})

# Batch auto-resume helpers
async def save_batch_state(user_id, state_dict):
    import time
    state_dict["timestamp"] = time.time()
    data = await get_data(user_id)
    if data and data.get("_id"):
         await db.update_one({"_id": user_id}, {"$set": {"batch_state": state_dict}})
    else:
         await db.insert_one({"_id": user_id, "batch_state": state_dict})

async def get_batch_state(user_id):
    data = await get_data(user_id)
    return data.get("batch_state", None) if data else None

async def delete_batch_state(user_id):
    await db.update_one({"_id": user_id}, {"$unset": {"batch_state": ""}})

async def get_all_active_batches():
    cursor = db.find({"batch_state": {"$exists": True}})
    return [doc async for doc in cursor]

async def update_bot_status(status):
    import time
    await mongo.user_data.bot_status.update_one({"_id": "status"}, {"$set": {"state": status, "timestamp": time.time()}}, upsert=True)

async def get_bot_status():
    data = await mongo.user_data.bot_status.find_one({"_id": "status"})
    return data if data else {"state": "crashed", "timestamp": 0}

async def cleanup_stale_data():
    """Cleans up batch_state and topic_msg_cache older than 72 hours."""
    import time
    cutoff_time = time.time() - (72 * 3600)
    
    # Topic msg cache cleanup
    await db.update_many(
        {"topic_msg_cache.timestamp": {"$lt": cutoff_time}},
        {"$unset": {"topic_msg_cache": ""}}
    )
    # Batch state cleanup
    await db.update_many(
        {"batch_state.timestamp": {"$lt": cutoff_time}},
        {"$unset": {"batch_state": ""}}
    )
    # Clean previous legacy objects as well
    await db.update_many(
        {"topic_msg_ids": {"$exists": True}},
        {"$unset": {"topic_msg_ids": ""}}
    )
