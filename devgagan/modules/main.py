# ---------------------------------------------------
# File Name: main.py
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
# More readable 
# ---------------------------------------------------

import time
import random
import string
import asyncio
from pyrogram import filters, Client
from devgagan import app, userrbot
from config import API_ID, API_HASH, FREEMIUM_LIMIT, PREMIUM_LIMIT, OWNER_ID, DEFAULT_SESSION
from devgagan.core.get_func import get_msg, telegram_bot
from devgagan.core.func import *
from devgagan.core.mongo import db
from devgagan.core.mongo.db import save_batch_state, get_batch_state, delete_batch_state, get_all_active_batches
from pyrogram.errors import FloodWait
from datetime import datetime, timedelta
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
import subprocess
from devgagan.modules.shrink import is_user_verified
async def generate_random_name(length=8):
    return ''.join(random.choices(string.ascii_lowercase, k=length))



users_loop = {}
interval_set = {}
batch_mode = {}

async def process_and_upload_link(userbot, user_id, msg_id, link, retry_count, message):
    try:
        res = await get_msg(userbot, user_id, msg_id, link, retry_count, message)
        if res is False:
            return False
        return True
    except Exception:
        return False
    finally:
        try:
            await app.delete_messages(user_id, msg_id)
        except Exception:
            pass

# Function to check if the user can proceed
async def check_interval(user_id, freecheck):
    if freecheck != 1 or await is_user_verified(user_id):  # Premium or owner users can always proceed
        return True, None

    now = datetime.now()

    # Check if the user is on cooldown
    if user_id in interval_set:
        cooldown_end = interval_set[user_id]
        if now < cooldown_end:
            remaining_time = (cooldown_end - now).seconds
            return False, f"Please wait {remaining_time} seconds(s) before sending another link. Alternatively, purchase premium for instant access.\n\n> Hey 👋 You can use /token to use the bot free for 3 hours without any time limit."
        else:
            del interval_set[user_id]  # Cooldown expired, remove user from interval set

    return True, None

async def set_interval(user_id, interval_minutes=45):
    now = datetime.now()
    # Set the cooldown interval for the user
    interval_set[user_id] = now + timedelta(seconds=interval_minutes)
    

@app.on_message(
    filters.regex(r'https?://(?:www\.)?t\.me/[^\s]+|tg://openmessage\?user_id=\w+&message_id=\d+')
    & filters.private
)
async def single_link(_, message):
    user_id = message.chat.id

    # Check subscription and batch mode
    if await subscribe(_, message) == 1 or user_id in batch_mode:
        return

    # Check if user is already in a loop
    if users_loop.get(user_id, False):
        await message.reply(
            "You already have an ongoing process. Please wait for it to finish or cancel it with /cancel."
        )
        return

    # Check freemium limits
    if await chk_user(message, user_id) == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    # Check cooldown
    can_proceed, response_message = await check_interval(user_id, await chk_user(message, user_id))
    if not can_proceed:
        await message.reply(response_message)
        return

    # Add user to the loop
    users_loop[user_id] = True

    link = message.text if "tg://openmessage" in message.text else get_link(message.text)
    msg = await message.reply("Processing...")
    userbot = await initialize_userbot(user_id)
    try:
        if await is_normal_tg_link(link):
            await process_and_upload_link(userbot, user_id, msg.id, link, 0, message)
            await set_interval(user_id, interval_minutes=45)
        else:
            await process_special_links(userbot, user_id, msg, link)
            
    except FloodWait as fw:
        await msg.edit_text(f'Try again after {fw.x} seconds due to floodwait from Telegram.')
    except Exception as e:
        await msg.edit_text(f"Link: `{link}`\n\n**Error:** {str(e)}")
    finally:
        users_loop[user_id] = False
        try:
            await msg.delete()
        except Exception:
            pass


USERBOT_CACHE = {}

async def initialize_userbot(user_id):
    data = await db.get_data(user_id)
    if data and data.get("session"):
        session_string = data.get("session")
        if user_id in USERBOT_CACHE:
            client = USERBOT_CACHE[user_id]
            if getattr(client, "sc_session_string", "") == session_string and client.is_connected:
                return client
            else:
                try:
                    await client.stop()
                except Exception:
                    pass
                USERBOT_CACHE.pop(user_id, None)

        try:
            device = 'iPhone 16 Pro'
            userbot = Client(
                "userbot",
                api_id=API_ID,
                api_hash=API_HASH,
                device_model=device,
                session_string=session_string
            )
            await userbot.start()
            userbot.sc_session_string = session_string
            USERBOT_CACHE[user_id] = userbot
            return userbot
        except Exception:
            try:
                await app.send_message(user_id, "Login Expired re do login")
            except:
                pass
            return None
    else:
        if DEFAULT_SESSION:
            return userrbot
        else:
            return None


async def is_normal_tg_link(link: str) -> bool:
    """Check if the link is a standard Telegram link."""
    special_identifiers = ['t.me/+', 't.me/c/', 't.me/b/', 'tg://openmessage']
    return 't.me/' in link and not any(x in link for x in special_identifiers)
    
async def process_special_links(userbot, user_id, msg, link):
    if userbot is None:
        return await msg.edit_text("Try logging in to the bot and try again.")
    if 't.me/+' in link:
        result = await userbot_join(userbot, link)
        await msg.edit_text(result)
        return
    special_patterns = ['t.me/c/', 't.me/b/', '/s/', 'tg://openmessage']
    if any(sub in link for sub in special_patterns):
        await process_and_upload_link(userbot, user_id, msg.id, link, 0, msg)
        await set_interval(user_id, interval_minutes=45)
        return
    await msg.edit_text("Invalid link...")


@app.on_message(filters.command("batch") & filters.private)
async def batch_link(_, message):
    join = await subscribe(_, message)
    if join == 1:
        return
    user_id = message.chat.id
    from devgagan.core.func import force_stop_flags, failed_messages_cache
    force_stop_flags[user_id] = False
    failed_messages_cache[user_id] = []
    
    # Check if a batch process is already running
    if users_loop.get(user_id, False):
        await app.send_message(
            message.chat.id,
            "You already have a process running. Please wait for it to complete or /cancel it."
        )
        return

    freecheck = await chk_user(message, user_id)
    if freecheck == 1 and FREEMIUM_LIMIT == 0 and user_id not in OWNER_ID and not await is_user_verified(user_id):
        await message.reply("Freemium service is currently not available. Upgrade to premium for access.")
        return

    max_batch_size = (FREEMIUM_LIMIT + 20) if await is_user_verified(user_id) else (FREEMIUM_LIMIT if freecheck == 1 else PREMIUM_LIMIT)

    # Validate and interval check
    can_proceed, response_message = await check_interval(user_id, freecheck)
    if not can_proceed:
        await message.reply(response_message)
        return

    # Start link input
    try:
        start_link_msg = await app.ask(message.chat.id, "Please send the Start Message Link.\n\n> Maximum tries 3", timeout=60)
        start_link = start_link_msg.text.strip()
    except asyncio.TimeoutError:
        await message.reply("⏰ Timed out. Please try again.")
        return

    # Determine connection type and chat info
    chat_id = None
    start_msg_id = None
    topic_id = None
    is_private = False

    if "t.me/c/" in start_link:
        is_private = True
        parts = start_link.split("/")
        try:
            chat_id_str = parts[parts.index('c') + 1]
            chat_id = int(f"-100{chat_id_str}")
            start_msg_id = int(parts[-1])
        except Exception as e:
            await message.reply(f"❌ Error parsing start link: {e}")
            return
    elif "t.me/b/" in start_link:
        is_private = True
        parts = start_link.split("/")
        try:
            start_msg_id = int(parts[-1])
        except Exception as e:
            await message.reply(f"❌ Error parsing start link: {e}")
            return
    else:
        # Normal public link
        parts = start_link.split("/")
        try:
            start_msg_id = int(parts[-1])
        except Exception as e:
            await message.reply(f"❌ Error parsing start link: {e}")
            return

    userbot = await initialize_userbot(user_id)
    if is_private and not userbot:
        await message.reply("❌ Userbot not initialized for private channel. Please /login first.")
        return

    if is_private and chat_id:
        # Check if it's a topic
        try:
            start_message_obj = await userbot.get_messages(chat_id, start_msg_id)
            if start_message_obj and getattr(start_message_obj, 'message_thread_id', None):
                topic_id = start_message_obj.message_thread_id
        except Exception as e:
            pass # Maybe not a topic or couldn't fetch

    # End link input
    try:
        end_link_msg = await app.ask(message.chat.id, "Please send the End Message Link, or type 'no' to download up to the latest message.", timeout=60)
        end_text = end_link_msg.text.strip()
        is_full_topic_download = False
        end_msg_id = None

        if end_text.lower() == 'no':
            is_full_topic_download = True
            if is_private and chat_id and userbot:
                last_message_list = [msg async for msg in userbot.get_chat_history(chat_id, limit=1)]
                end_msg_id = last_message_list[0].id if last_message_list else start_msg_id
            else:
                if not is_private:
                    try:
                        # Find the channel username correctly depending on link format
                        # Example: https://t.me/channel_name/123
                        t_me_index = parts.index('t.me')
                        username = parts[t_me_index + 1]
                        last_message_list = [msg async for msg in app.get_chat_history(username, limit=1)]
                        end_msg_id = last_message_list[0].id if last_message_list else start_msg_id
                    except Exception as e:
                        await message.reply("Could not fetch the last message of the public channel. Please provide an exact end link.")
                        return
                elif is_private and not chat_id:
                     await message.reply("'no' is not supported for bot links without the complete chat_id. Please provide an exact end link.")
                     return
        else:
            end_link = end_text
            if is_private and chat_id and (str(chat_id_str) not in end_link):
                await message.reply("❌ Invalid Link. End link must be from the same chat.")
                return
            end_parts = end_link.split("/")
            try:
                end_msg_id = int(end_parts[-1])
            except Exception as e:
                await message.reply(f"❌ Error parsing end link: {e}")
                return
    except asyncio.TimeoutError:
        await message.reply("⏰ Timed out. Please try again.")
        return

    if end_msg_id is None or end_msg_id < start_msg_id:
        await message.reply("End message must be after start message.")
        return

    total_to_check = end_msg_id - start_msg_id + 1
    if not is_full_topic_download and total_to_check > max_batch_size:
        await message.reply(f"Range exceeds limit of {max_batch_size}. Please try a smaller range.")
        return

    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/team_spy_pro")
    keyboard = InlineKeyboardMarkup([[join_button]])
    users_loop[user_id] = True
    processed_count = 0

    batch_state = {
        "start_link": start_link,
        "start_msg_id": start_msg_id,
        "end_msg_id": end_msg_id,
        "chat_id": chat_id,
        "topic_id": topic_id,
        "is_private": is_private,
        "is_topic": bool(topic_id),
        "total_messages": total_to_check,
        "current_msg_id": start_msg_id,
        "processed_count": 0,
        "current_index": 0
    }
    await save_batch_state(user_id, batch_state)

    if is_private and chat_id:
        # Topic batch logic (original proven approach)
        try:
            pin_msg = await app.send_message(user_id, f"Topic batch process started ⚡\nTotal messages to check: {total_to_check}\n\n**Powered by Team SPY**", reply_markup=keyboard)
            
            for i in range(start_msg_id, end_msg_id + 1):
                if not users_loop.get(user_id):
                    await pin_msg.edit("🛑 Batch process cancelled.")
                    break
                    
                batch_state["current_index"] = i - start_msg_id
                if (i - start_msg_id) % 5 == 0:
                    await save_batch_state(user_id, batch_state)
                    
                try:
                    current_msg = await userbot.get_messages(chat_id, i)
                    if current_msg and not current_msg.empty and not current_msg.service:
                        if current_msg.media or current_msg.text:
                            edit_msg = await app.send_message(user_id, f"Processing message {current_msg.id}...")
                            await telegram_bot._process_message(userbot, current_msg, user_id, edit_msg)
                            processed_count += 1
                            batch_state["processed_count"] = processed_count
                            await pin_msg.edit(f"Topic batch process running ⚡\nProcessed: {processed_count}\nChecked: {i - start_msg_id + 1}/{total_to_check}\n\n**Powered by Team SPY**", reply_markup=keyboard)
                            await asyncio.sleep(15)
                        
                except FloodWait as fw:
                    await pin_msg.edit(f"Floodwait of {fw.value} seconds. Sleeping...")
                    await asyncio.sleep(fw.value + 5)
                except Exception as e:
                    print(f"[Batch] Error processing msg {i}: {e}")
                    from devgagan.core.func import failed_messages_cache
                    if user_id in failed_messages_cache:
                        failed_messages_cache[user_id].append(i)
                    await asyncio.sleep(2)
                    
            await set_interval(user_id, interval_minutes=300)
            await pin_msg.edit(f"✅ Topic batch completed!\nProcessed {processed_count} messages.", reply_markup=keyboard)
            await delete_batch_state(user_id)
            
            from devgagan.core.func import failed_messages_cache
            failed_msgs = failed_messages_cache.get(user_id, [])
            if failed_msgs:
                import io
                fail_msg = "The following message IDs failed to process:\n" + "\n".join(str(m) for m in failed_msgs)
                file = io.BytesIO(fail_msg.encode('utf-8'))
                file.name = "failed_report.txt"
                await app.send_document(user_id, file, caption="📑 **Failed Messages Report**")
                failed_messages_cache.pop(user_id, None)

        except Exception as e:
            await app.send_message(message.chat.id, f"An error occurred during topic batch processing: {e}")
        finally:
            users_loop.pop(user_id, None)


    else:
        # Normal batch logic
        pin_msg = await app.send_message(user_id, f"Batch process started ⚡\nProcessing: 0/{total_to_check}\n\n**Powered by Team SPY**", reply_markup=keyboard)
        await pin_msg.pin(both_sides=True)
        try:
            for i in range(start_msg_id, end_msg_id + 1):
                if user_id in users_loop and users_loop[user_id]:
                    batch_state["current_msg_id"] = i
                    if processed_count % 5 == 0:
                        await save_batch_state(user_id, batch_state)

                    try:
                        url = f"{'/'.join(start_link.split('/')[:-1])}/{i}"
                        link = get_link(url)
                        if link:
                            msg = await app.send_message(message.chat.id, f"Processing...")
                            if await process_and_upload_link(userbot, user_id, msg.id, link, 0, message):
                                processed_count += 1
                                batch_state["processed_count"] = processed_count
                                sleep_time = 2
                                if processed_count % 50 == 0:
                                    sleep_time = 30
                                elif processed_count % 20 == 0:
                                    sleep_time = 15
                                elif processed_count % 5 == 0:
                                    sleep_time = 5
                                await pin_msg.edit_text(
                                    f"Batch process started ⚡\nProcessing: {processed_count}/{total_to_check}\nDelay: {sleep_time}s\n\n**__Powered by Team SPY__**",
                                    reply_markup=keyboard
                                )
                                await asyncio.sleep(sleep_time)
                            else:
                                failed_messages_cache[user_id].append(link)
                                try:
                                    await msg.delete()
                                except:
                                    pass
                    except Exception:
                        pass
                else:
                    break

            from devgagan.modules.main import telegram_bot
            from devgagan.core.func import failed_messages_cache
            
            while hasattr(telegram_bot, 'active_uploads') and telegram_bot.active_uploads.get(user_id):
                if not users_loop.get(user_id):
                    break
                await asyncio.sleep(2)

            await set_interval(user_id, interval_minutes=300)
            await pin_msg.edit_text(
                f"Batch completed successfully for {processed_count} messages 🎉\n\n**__Powered by Team SPY__**",
                reply_markup=keyboard
            )
            await app.send_message(message.chat.id, "Batch completed successfully! 🎉")
            await delete_batch_state(user_id)
            
            failed_msgs = failed_messages_cache.get(user_id, [])
            if failed_msgs:
                import io
                fail_msg = "The following links failed to process:\n" + "\n".join(str(m) for m in failed_msgs)
                file = io.BytesIO(fail_msg.encode('utf-8'))
                file.name = "failed_report.txt"
                await app.send_document(user_id, file, caption="📑 **Failed Links Report**")
                failed_messages_cache.pop(user_id, None)

        except Exception as e:
            await app.send_message(message.chat.id, f"Error: {e}")
        finally:
            users_loop.pop(user_id, None)

@app.on_message(filters.command("cancel"))
async def stop_batch(_, message):
    user_id = message.chat.id

    # Check if there is an active batch process for the user
    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  # Set the loop status to False
        await delete_batch_state(user_id)
        await app.send_message(
            message.chat.id, 
            "Graceful Cancel initiated. The current file will finish uploading/downloading, and then the batch process will halt safely."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to cancel."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to cancel."
        )

@app.on_message(filters.command("stop"))
async def force_stop_batch(_, message):
    user_id = message.chat.id

    from devgagan.core.func import force_stop_flags
    from devgagan.modules.main import telegram_bot

    if user_id in users_loop and users_loop[user_id]:
        users_loop[user_id] = False  # Stop the loop
        force_stop_flags[user_id] = True  # Abort Pyrogram tasks
        
        # Abort Telethon/Pro tasks
        if hasattr(telegram_bot, 'active_uploads') and user_id in telegram_bot.active_uploads:
            for task in telegram_bot.active_uploads[user_id]:
                if not task.done():
                    task.cancel()
            telegram_bot.active_uploads[user_id].clear()
            
        await delete_batch_state(user_id)
        await app.send_message(
            message.chat.id, 
            "🛑 Force stopped all active downloads and uploads immediately."
        )
    elif user_id in users_loop and not users_loop[user_id]:
        await app.send_message(
            message.chat.id, 
            "The batch process was already stopped. No active batch to stop."
        )
    else:
        await app.send_message(
            message.chat.id, 
            "No active batch processing is running to stop."
        )

async def resume_all_batches(status):
    active_batches = await get_all_active_batches()
    if not active_batches:
        return

    import time
    state = status.get("state", "crashed")
    last_time = status.get("timestamp", 0)
    current_time = time.time()

    is_valid_resume = False
    if state == "crashed":
        is_valid_resume = True # Always resume on crash
    elif state == "normal" and (current_time - last_time) < 1800: # 30 mins
        is_valid_resume = True
    
    if not is_valid_resume:
        for batch in active_batches:
            user_id = batch["_id"]
            await delete_batch_state(user_id)
            try:
                await app.send_message(user_id, "Your pending batch process was cancelled as the bot was intentionally offline for more than 30 minutes.")
            except:
                pass
        return

    # Trigger resumes
    for batch in active_batches:
        user_id = batch["_id"]
        batch_state = batch["batch_state"]
        asyncio.create_task(resume_individual_batch(user_id, batch_state))

async def resume_individual_batch(user_id, batch_state):
    start_link = batch_state["start_link"]
    start_msg_id = batch_state["start_msg_id"]
    end_msg_id = batch_state["end_msg_id"]
    chat_id = batch_state["chat_id"]
    topic_id = batch_state["topic_id"]
    is_private = batch_state["is_private"]
    total_to_check = batch_state["total_messages"]
    processed_count = batch_state.get("processed_count", 0)
    
    from devgagan.core.func import force_stop_flags
    force_stop_flags[user_id] = False

    userbot = await initialize_userbot(user_id)
    if is_private and not userbot:
        try:
            await app.send_message(user_id, "❌ Batch resumed but Userbot not initialized for private channel. Please /login first.")
        except:
            pass
        await delete_batch_state(user_id)
        return
        
    join_button = InlineKeyboardButton("Join Channel", url="https://t.me/team_spy_pro")
    keyboard = InlineKeyboardMarkup([[join_button]])
    users_loop[user_id] = True

    try:
        if topic_id:
            pin_msg = await app.send_message(user_id, f"Resuming topic batch ⚡\nProcessed: {processed_count}/{total_to_check}\n\n**Powered by Team SPY**", reply_markup=keyboard)
            saved_msg_ids = await db.get_topic_msg_ids(user_id, chat_id, topic_id)
            start_index = batch_state.get("current_index", 0)
            
            for idx in range(start_index, len(saved_msg_ids)):
                msg_id = saved_msg_ids[idx]
                if not users_loop.get(user_id):
                    await pin_msg.edit("🛑 Batch process cancelled.")
                    break
                
                batch_state["current_index"] = idx
                if idx % 5 == 0:
                    await save_batch_state(user_id, batch_state)

                try:
                    current_msg = await userbot.get_messages(chat_id, msg_id)
                    if current_msg:
                        edit_msg = await app.send_message(user_id, f"Processing message {current_msg.id}...")
                        await telegram_bot._process_message(userbot, current_msg, user_id, edit_msg)
                        processed_count += 1
                        batch_state["processed_count"] = processed_count
                        sleep_time = 2
                        if processed_count % 50 == 0:
                            sleep_time = 30
                        elif processed_count % 20 == 0:
                            sleep_time = 15
                        elif processed_count % 5 == 0:
                            sleep_time = 5
                        await pin_msg.edit(f"Topic batch process running ⚡\nProcessed: {processed_count}/{len(saved_msg_ids)}\nDelay: {sleep_time}s\n\n**Powered by Team SPY**", reply_markup=keyboard)
                        await asyncio.sleep(sleep_time)
                except FloodWait as fw:
                    await pin_msg.edit(f"Floodwait of {fw.value} seconds. Sleeping...")
                    await asyncio.sleep(fw.value + 5)
                except Exception:
                    pass
            await set_interval(user_id, interval_minutes=300)
            await pin_msg.edit(f"✅ Topic batch completed!\nProcessed {processed_count} messages.", reply_markup=keyboard)
            await db.clear_topic_msg_ids(user_id)
            await delete_batch_state(user_id)
        else:
            pin_msg = await app.send_message(user_id, f"Resuming normal batch ⚡\nProcessed: {processed_count}/{total_to_check}\n\n**Powered by Team SPY**", reply_markup=keyboard)
            await pin_msg.pin(both_sides=True)
            current_msg_id = batch_state.get("current_msg_id", start_msg_id)
            
            for i in range(current_msg_id, end_msg_id + 1):
                if user_id in users_loop and users_loop[user_id]:
                    batch_state["current_msg_id"] = i
                    if processed_count % 5 == 0:
                        await save_batch_state(user_id, batch_state)

                    try:
                        url = f"{'/'.join(start_link.split('/')[:-1])}/{i}"
                        link = get_link(url)
                        if link:
                            msg = await app.send_message(user_id, f"Processing...")
                            if await process_and_upload_link(userbot, user_id, msg.id, link, 0, None):
                                processed_count += 1
                                batch_state["processed_count"] = processed_count
                                sleep_time = 2
                                if processed_count % 50 == 0:
                                    sleep_time = 30
                                elif processed_count % 20 == 0:
                                    sleep_time = 15
                                elif processed_count % 5 == 0:
                                    sleep_time = 5
                                await pin_msg.edit_text(f"Batch process running ⚡\nProcessed: {processed_count}/{total_to_check}\nDelay: {sleep_time}s\n\n**__Powered by Team SPY__**", reply_markup=keyboard)
                                await asyncio.sleep(sleep_time)
                    except Exception:
                        pass
                else:
                    break
            
            await set_interval(user_id, interval_minutes=300)
            await pin_msg.edit_text(f"Batch completed successfully for {processed_count} messages 🎉\n\n**__Powered by Team SPY__**", reply_markup=keyboard)
            await app.send_message(user_id, "Batch completed successfully! 🎉")
            await delete_batch_state(user_id)
    except Exception as e:
        await app.send_message(user_id, f"Error during batch resume: {e}")
    finally:
        users_loop.pop(user_id, None)
