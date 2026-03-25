# ---------------------------------------------------
# File Name: __main__.py
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

import sys
if sys.platform != "win32":
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass

import asyncio
import importlib
import gc
from pyrogram import idle
from devgagan.modules import ALL_MODULES
from devgagan.core.mongo.plans_db import check_and_remove_expired_users
from aiojobs import create_scheduler

# ----------------------------Bot-Start---------------------------- #

loop = asyncio.get_event_loop()

# Function to schedule expiry checks
async def schedule_expiry_check():
    scheduler = await create_scheduler()
    while True:
        await scheduler.spawn(check_and_remove_expired_users())
        await asyncio.sleep(60)  # Check every hour
        gc.collect()

async def devggn_boot():
    for all_module in ALL_MODULES:
        importlib.import_module("devgagan.modules." + all_module)
    
    from devgagan.core.mongo.db import get_bot_status, update_bot_status, cleanup_stale_data
    from devgagan.modules.main import resume_all_batches
    
    # Run passive 72h cleanup of stale caches and aborted batches
    asyncio.create_task(cleanup_stale_data())
    
    # Fetch bot status and Auto-Resume batches
    status = await get_bot_status()
    asyncio.create_task(resume_all_batches(status))
    
    # Update status immediately to "crashed" as default fallback for OOMs
    await update_bot_status("crashed")

    print("""
---------------------------------------------------
📂 Bot Deployed successfully ...
📝 Description: A Pyrogram bot for downloading files from Telegram channels or groups 
                and uploading them back to Telegram.
👨‍💻 Author: Gagan
🌐 GitHub: https://github.com/devgaganin/
📬 Telegram: https://t.me/team_spy_pro
▶️ YouTube: https://youtube.com/@dev_gagan
🗓️ Created: 2025-01-11
🔄 Last Modified: 2025-01-11
🛠️ Version: 2.0.5
📜 License: MIT License
---------------------------------------------------
""")

    asyncio.create_task(schedule_expiry_check())
    print("Auto removal started ...")
    await idle()
    print("Bot stopped...")
    
    # This block executes fully ONLY on graceful shutdown (like Heroku restart/SIGTERM)
    await update_bot_status("normal")
    
    try:
        from devgagan import app, userrbot, pro, sex, telethon_client
        from devgagan.modules.main import USERBOT_CACHE
        
        if app and getattr(app, 'is_connected', False): await app.stop()
        if userrbot and getattr(userrbot, 'is_connected', False): await userrbot.stop()
        if pro and getattr(pro, 'is_connected', False): await pro.stop()
        if sex and getattr(sex, 'is_connected', lambda: False)(): await sex.disconnect()
        if telethon_client and getattr(telethon_client, 'is_connected', lambda: False)(): await telethon_client.disconnect()
        
        for user_id, client in USERBOT_CACHE.items():
            if getattr(client, 'is_connected', False):
                try: await client.stop()
                except: pass
                
    except Exception as e:
        print(f"Error during shutdown: {e}")


if __name__ == "__main__":
    try:
        loop.run_until_complete(devggn_boot())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        print(f"Bot error: {err}")
    finally:
        try:
            pending = [task for task in asyncio.all_tasks(loop) if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception as e:
            print(f"Task cleanup error: {e}")
        finally:
            loop.close()
            sys.exit(0)

# ------------------------------------------------------------------ #
