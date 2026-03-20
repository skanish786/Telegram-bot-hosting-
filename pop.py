import os
import time
import json
import threading
import shutil
import subprocess
import zipfile
import tempfile
import shlex
import signal
import atexit
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime, timedelta

from flask import Flask
import telebot
from telebot import types

# ---------------------------
# Configuration
# ---------------------------
BOT_TOKEN = "8643557168:AAGmmoLDsZth53GPXJYqTbxS6Puo0zQ-dZQ"
if not BOT_TOKEN:
    raise RuntimeError("Please set BOT_TOKEN (env or in code).")

OWNER_ID = int(os.getenv("OWNER_ID", "8336467661"))
OWNER_USERNAME = "@Contact1432"
OWNER_TG_URL = f"https://t.me/Contact1432"

# Channels for force-subscribe
CHANNEL_IDS = [
    int(os.getenv("CHANNEL_ID_1", "2027547140"))
]
CHANNEL_LINKS = [
    os.getenv("CHANNEL_LINK_1", "https://t.me/All_Time_Earning143")
]

PORT = int(os.getenv("PORT", "8000"))

DATA_DIR = "data"
PY_DIR = os.path.join(DATA_DIR, "projects")
DB_FILE = os.path.join(DATA_DIR, "users.json")
RUNNING_PROCS_FILE = os.path.join(DATA_DIR, "running_procs.json")
PLANS_FILE = os.path.join(DATA_DIR, "plans.json")

os.makedirs(PY_DIR, exist_ok=True)

# Subscription Plans
PLANS = {
    "free": {
        "name": "🆓 Free Plan",
        "price": "0₹",
        "duration_days": 0,  # Forever
        "max_bots": 1,
        "upload_limit_mb": 10,
        "credits": 5,
        "priority": 0,
        "features": [
            "✅ 1 Bot Hosting",
            "✅ 10MB Upload Limit",
            "✅ 5 Free Credits",
            "✅ Basic Support"
        ]
    },
    "basic": {
        "name": "🥈 Basic Plan",
        "price": "99₹",
        "duration_days": 30,
        "max_bots": 2,
        "upload_limit_mb": 50,
        "credits": 30,
        "priority": 1,
        "features": [
            "✅ 2 Bots Hosting",
            "✅ 50MB Upload Limit",
            "✅ 30 Credits",
            "✅ Medium Priority",
            "✅ 30 Days Validity"
        ]
    },
    "pro": {
        "name": "🥇 Pro Plan",
        "price": "199₹",
        "duration_days": 30,
        "max_bots": 5,
        "upload_limit_mb": 100,
        "credits": 100,
        "priority": 2,
        "features": [
            "✅ 5 Bots Hosting",
            "✅ 100MB Upload Limit",
            "✅ 100 Credits",
            "✅ High Priority",
            "✅ 30 Days Validity",
            "✅ Priority Support"
        ]
    },
    "premium": {
        "name": "👑 Premium Plan",
        "price": "299₹",
        "duration_days": 30,
        "max_bots": 10,
        "upload_limit_mb": 200,
        "credits": 250,
        "priority": 3,
        "features": [
            "✅ 10 Bots Hosting",
            "✅ 200MB Upload Limit",
            "✅ 250 Credits",
            "✅ Highest Priority",
            "✅ 30 Days Validity",
            "✅ 24/7 Priority Support",
            "✅ Auto Backup"
        ]
    }
}

DEFAULT_USER = {
    "credits": 5,
    "upload_mb": 10,
    "banned": False,
    "admin": False,
    "joined_at": datetime.now().isoformat(),
    "plan": "free",
    "plan_expiry": None,
    "max_bots": 1,
    "total_spent": 0,
    "purchase_history": []
}

db: Dict[str, Dict[str, Any]] = {}
running_procs: Dict[str, subprocess.Popen] = {}
admin_state: Dict[int, Dict[str, Any]] = {}

# Buttons
BTN_UPLOAD = "📥 Upload File / ZIP"
BTN_MYFILES = "🐍 My Files"
BTN_PLAN = "⚙️ Plan / Limits"
BTN_CONTACT = "📞 Contact Owner"
BTN_PLANS = "💰 Buy Plans"
BTN_STATS = "📊 User Stats"

BTN_MANAGE = "🔎 Manage User"
BTN_BROADCAST = "📢 Broadcast"
BTN_USERLIST = "👥 User List"
BTN_GIVE_PLAN = "🎁 Give Plan"
BTN_BACK_MAIN = "⬅️ Back to Main"
BTN_BACK_ADMIN = "⬅️ Back to Admin Panel"

BTN_BAN = "🚫 Ban / Unban"
BTN_ADDCRED = "💰 Add Credits"
BTN_SET_UPLOAD = "☁️ Set Upload Limit"
BTN_SET_PLAN = "👑 Set User Plan"

ALL_BUTTONS = {
    BTN_UPLOAD, BTN_MYFILES, BTN_PLAN, BTN_CONTACT, BTN_PLANS, BTN_STATS,
    BTN_MANAGE, BTN_BROADCAST, BTN_USERLIST, BTN_GIVE_PLAN, BTN_BACK_MAIN, BTN_BACK_ADMIN,
    BTN_BAN, BTN_ADDCRED, BTN_SET_UPLOAD, BTN_SET_PLAN
}

# ---------------------------
# DB helpers
# ---------------------------
def load_db():
    global db
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r", encoding="utf-8") as f:
                db = json.load(f)
        except Exception as e:
            print(f"Error loading DB: {e}")
            db = {}
    else:
        db = {}

def save_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

def get_user(uid: int) -> Dict[str, Any]:
    uid_s = str(uid)
    if uid_s not in db:
        db[uid_s] = DEFAULT_USER.copy()
        if uid == OWNER_ID:
            db[uid_s]["admin"] = True
            db[uid_s]["upload_mb"] = 500
            db[uid_s]["credits"] = 9999
            db[uid_s]["plan"] = "premium"
            db[uid_s]["max_bots"] = 999
        db[uid_s]["joined_at"] = datetime.now().isoformat()
        save_db()
    return db[uid_s]

def update_user(uid: int, key: str, value):
    _ = get_user(uid)
    db[str(uid)][key] = value
    save_db()

load_db()

# ---------------------------
# Plan Management
# ---------------------------
def apply_plan_to_user(user_id: int, plan_type: str, duration_days: Optional[int] = None):
    """Apply a plan to a user"""
    user = get_user(user_id)
    plan = PLANS[plan_type]
    
    # Calculate expiry
    if plan_type == "free":
        expiry = None
    else:
        if duration_days is None:
            duration_days = plan["duration_days"]
        expiry = datetime.now() + timedelta(days=duration_days)
    
    # Update user data
    user["plan"] = plan_type
    user["upload_mb"] = plan["upload_limit_mb"]
    user["max_bots"] = plan["max_bots"]
    user["plan_expiry"] = expiry.isoformat() if expiry else None
    
    # Add credits for paid plans
    if plan_type != "free":
        user["credits"] += plan["credits"]
    
    save_db()
    
    # Send notification to user
    try:
        expiry_msg = ""
        if expiry:
            expiry_date = expiry.strftime("%d %B %Y")
            expiry_msg = f"\n⏰ Plan Valid Till: {expiry_date}"
        
        message = (
            f"🎉 *Congratulations!*\n\n"
            f"✅ You have been upgraded to {plan['name']}\n\n"
            f"📋 *Plan Features:*\n"
            f"• Max Bots: {plan['max_bots']}\n"
            f"• Upload Limit: {plan['upload_limit_mb']}MB\n"
            f"• Added Credits: {plan['credits']}\n"
            f"{expiry_msg}\n\n"
            f"Thank you for choosing our service! 💝"
        )
        
        bot.send_message(user_id, message, parse_mode="Markdown")
    except Exception as e:
        print(f"Failed to send plan notification: {e}")
    
    return expiry

def check_plan_expiry():
    """Check and handle expired plans"""
    now = datetime.now()
    expired_users = []
    
    for uid_str, user_data in db.items():
        if user_data.get("plan", "free") != "free" and user_data.get("plan_expiry"):
            try:
                expiry_date = datetime.fromisoformat(user_data["plan_expiry"])
                uid = int(uid_str)
                
                # Check if expired
                if now > expiry_date:
                    # Downgrade to free plan
                    user_data["plan"] = "free"
                    user_data["upload_mb"] = PLANS["free"]["upload_limit_mb"]
                    user_data["max_bots"] = PLANS["free"]["max_bots"]
                    user_data["plan_expiry"] = None
                    expired_users.append(uid)
                    
                    # Notify user
                    try:
                        bot.send_message(
                            uid,
                            "⚠️ *Your Plan Has Expired*\n\n"
                            "Your premium plan has expired. You have been downgraded to the Free Plan.\n"
                            "To continue enjoying premium features, please renew your plan.",
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                
                # Check if 3 days before expiry
                elif (expiry_date - now).days <= 3:
                    days_left = (expiry_date - now).days
                    if days_left > 0:
                        try:
                            bot.send_message(
                                uid,
                                f"🔔 *Plan Expiry Reminder*\n\n"
                                f"Your {PLANS[user_data['plan']]['name']} will expire in {days_left} day(s).\n"
                                f"Expiry Date: {expiry_date.strftime('%d %B %Y')}\n\n"
                                f"Renew now to avoid interruption in service.",
                                parse_mode="Markdown"
                            )
                        except Exception:
                            pass
            except Exception as e:
                print(f"Error checking plan expiry for {uid_str}: {e}")
    
    # Save changes
    if expired_users:
        save_db()
        
        # Notify owner
        try:
            expired_list = "\n".join([f"• `{uid}`" for uid in expired_users[:10]])
            if len(expired_users) > 10:
                expired_list += f"\n• ... and {len(expired_users) - 10} more"
            
            bot.send_message(
                OWNER_ID,
                f"⚠️ *Plan Expiry Report*\n\n"
                f"Total Expired Plans: {len(expired_users)}\n\n"
                f"Users downgraded to Free Plan:\n"
                f"{expired_list}",
                parse_mode="Markdown"
            )
        except Exception:
            pass
    
    return expired_users

def get_user_stats():
    """Get statistics about users and plans"""
    total_users = len(db)
    free_users = sum(1 for u in db.values() if u.get("plan", "free") == "free")
    premium_users = total_users - free_users
    
    plan_counts = {"free": 0, "basic": 0, "pro": 0, "premium": 0}
    for user_data in db.values():
        plan = user_data.get("plan", "free")
        if plan in plan_counts:
            plan_counts[plan] += 1
    
    active_premium = 0
    for user_data in db.values():
        if user_data.get("plan", "free") != "free" and user_data.get("plan_expiry"):
            try:
                expiry_date = datetime.fromisoformat(user_data["plan_expiry"])
                if datetime.now() < expiry_date:
                    active_premium += 1
            except Exception:
                pass
    
    total_revenue = sum(u.get("total_spent", 0) for u in db.values())
    
    return {
        "total_users": total_users,
        "free_users": free_users,
        "premium_users": premium_users,
        "active_premium": active_premium,
        "plan_counts": plan_counts,
        "total_revenue": total_revenue
    }

# Start plan expiry checker thread
def plan_expiry_checker():
    """Background thread to check plan expiry"""
    while True:
        try:
            check_plan_expiry()
        except Exception as e:
            print(f"Plan expiry checker error: {e}")
        time.sleep(3600)  # Check every hour

# ---------------------------
# Special function: Owner unban
# ---------------------------
def owner_unban():
    """Ensure owner is never banned"""
    owner_str = str(OWNER_ID)
    if owner_str in db and db[owner_str].get("banned", False):
        print("⚠️ Owner was banned! Auto-unbanning...")
        db[owner_str]["banned"] = False
        db[owner_str]["admin"] = True
        save_db()

owner_unban()

# ---------------------------
# Running Processes Save/Load for Auto-Restart
# ---------------------------
def save_running_procs():
    """Save running processes info to file for auto-restart"""
    proc_info = {}
    for proc_key, proc in running_procs.items():
        if proc and proc.poll() is None:
            try:
                uid, bot_id = proc_key.split("_")
                proc_info[proc_key] = {
                    "uid": int(uid),
                    "bot_id": bot_id,
                    "pid": proc.pid,
                    "timestamp": datetime.now().isoformat()
                }
            except Exception as e:
                print(f"Error saving process {proc_key}: {e}")
    
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(RUNNING_PROCS_FILE, "w", encoding="utf-8") as f:
        json.dump(proc_info, f, indent=2)

def load_running_procs():
    """Load and restart previously running processes"""
    if not os.path.exists(RUNNING_PROCS_FILE):
        return
    
    try:
        with open(RUNNING_PROCS_FILE, "r", encoding="utf-8") as f:
            proc_info = json.load(f)
        
        print(f"Found {len(proc_info)} previously running processes to restart")
        
        for proc_key, info in proc_info.items():
            try:
                uid = info["uid"]
                bot_id = info["bot_id"]
                
                folder = bot_folder(uid, bot_id)
                if not os.path.exists(folder):
                    print(f"Folder not found for {proc_key}, skipping...")
                    continue
                
                filename = get_bot_filename(uid, bot_id)
                
                print(f"Auto-restarting {filename} for user {uid}")
                
                threading.Thread(
                    target=auto_restart_project,
                    args=(uid, bot_id, folder, filename),
                    daemon=True
                ).start()
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Failed to restart {proc_key}: {e}")
    
    except Exception as e:
        print(f"Error loading running processes: {e}")

def auto_restart_project(uid: int, bot_id: str, folder: str, filename: str):
    """Auto-restart a project in background"""
    try:
        key = f"{uid}_{bot_id}"
        
        if key in running_procs:
            proc = running_procs[key]
            if proc and proc.poll() is None:
                print(f"Project {filename} is already running, skipping restart")
                return
        
        run_project(uid, bot_id, folder, filename, None, auto_restart=True)
        
    except Exception as e:
        print(f"Auto-restart failed for {uid}_{bot_id}: {e}")

def save_procs_periodically():
    """Periodically save running processes"""
    while True:
        time.sleep(30)
        save_running_procs()

atexit.register(save_running_procs)

# ---------------------------
# Flask for uptime
# ---------------------------
app = Flask(__name__)

@app.route("/")
def home():
    return "<h3>Python/Node Hosting Bot</h3><p>Status: Running</p>"

def run_flask():
    app.run(host="0.0.0.0", port=PORT)

# ---------------------------
# Telebot init
# ---------------------------
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="Markdown")

# ---------------------------
# Force-subscribe
# ---------------------------
def is_subscribed(user_id: int) -> bool:
    if all(cid == 0 for cid in CHANNEL_IDS):
        return True
    if user_id == OWNER_ID:
        return True
    for cid in CHANNEL_IDS:
        if cid == 0:
            continue
        try:
            member = bot.get_chat_member(cid, user_id)
            if member.status in ("left", "kicked"):
                return False
        except Exception:
            continue
    return True

def send_subscribe_prompt(chat_id: int):
    text = (
        "📢 To use this bot, please join our official channel(s) first.\n"
        "After joining, click *Joined, Check Again*."
    )
    ikb = types.InlineKeyboardMarkup()
    for link in CHANNEL_LINKS:
        if link:
            ikb.add(types.InlineKeyboardButton("🔔 Join Channel", url=link))
    ikb.add(types.InlineKeyboardButton("✅ Joined, Check Again", callback_data="check_sub"))
    bot.send_message(chat_id, text, reply_markup=ikb)

@bot.callback_query_handler(func=lambda c: c.data == "check_sub")
def check_sub_callback(call):
    uid = call.from_user.id
    if is_subscribed(uid):
        bot.answer_callback_query(call.id, "✅ Subscription verified.")
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        fake_msg = types.Message(
            message_id=call.message.message_id,
            from_user=call.from_user,
            date=call.message.date,
            chat=call.message.chat,
            content_type="text",
            options={},
            json_string=""
        )
        fake_msg.text = "/start"
        start_handler(fake_msg)
    else:
        bot.answer_callback_query(call.id, "❌ Please join the channel(s) first.", show_alert=True)

# ---------------------------
# Keyboards
# ---------------------------
def main_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_UPLOAD, BTN_MYFILES)
    kb.row(BTN_PLAN, BTN_CONTACT)
    kb.row(BTN_PLANS, BTN_STATS)
    return kb

def admin_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_MANAGE, BTN_BROADCAST)
    kb.row(BTN_USERLIST, BTN_GIVE_PLAN)
    kb.row(BTN_STATS)
    kb.row(BTN_BACK_MAIN)
    return kb

def admin_user_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_BAN)
    kb.row(BTN_ADDCRED, BTN_SET_UPLOAD)
    kb.row(BTN_SET_PLAN)
    kb.row(BTN_BACK_ADMIN)
    return kb

def banned_user_keyboard():
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_CONTACT)
    return kb

# ---------------------------
# File helpers
# ---------------------------
def user_root(uid: int) -> str:
    return os.path.join(PY_DIR, str(uid))

def bot_folder(uid: int, bot_id: str) -> str:
    return os.path.join(user_root(uid), bot_id)

def bot_meta_path(uid: int, bot_id: str) -> str:
    return os.path.join(bot_folder(uid, bot_id), "meta.json")

def save_bot_meta(uid: int, bot_id: str, filename: str):
    os.makedirs(bot_folder(uid, bot_id), exist_ok=True)
    with open(bot_meta_path(uid, bot_id), "w", encoding="utf-8") as f:
        json.dump({"filename": filename, "uploaded_at": datetime.now().isoformat()}, f)

def get_bot_filename(uid: int, bot_id: str) -> str:
    try:
        with open(bot_meta_path(uid, bot_id), "r", encoding="utf-8") as f:
            return json.load(f).get("filename", f"{bot_id}.zip")
    except Exception:
        return f"{bot_id}.zip"

def list_user_bots(uid: int) -> List[Dict[str, Any]]:
    root = user_root(uid)
    if not os.path.exists(root):
        return []
    files = []
    for bot_id in os.listdir(root):
        folder = bot_folder(uid, bot_id)
        if os.path.isdir(folder):
            files.append({
                "id": bot_id,
                "name": get_bot_filename(uid, bot_id),
                "path": folder
            })
    files.sort(key=lambda x: x["id"], reverse=True)
    return files

def can_upload_more_bots(uid: int) -> bool:
    """Check if user can upload more bots based on their plan"""
    user = get_user(uid)
    current_bots = len(list_user_bots(uid))
    max_allowed = user.get("max_bots", 1)
    return current_bots < max_allowed

def safe_extract_zip(zfile: zipfile.ZipFile, dest: str):
    for member in zfile.namelist():
        member_path = os.path.normpath(os.path.join(dest, member))
        if not member_path.startswith(os.path.abspath(dest)):
            raise Exception("Zip contains unsafe paths")
    zfile.extractall(dest)

# ---------------------------
# Common checks
# ---------------------------
def banned_check(message) -> bool:
    uid = message.from_user.id
    
    if uid == OWNER_ID:
        return True
    
    user = get_user(uid)
    if user["banned"]:
        ban_message = "🚫 *You are banned from using this bot.*\n\n"
        ban_message += "If you think this is a mistake, please contact the owner."
        
        ikb = types.InlineKeyboardMarkup()
        ikb.add(types.InlineKeyboardButton("📞 Contact Owner", url=OWNER_TG_URL))
        
        bot.send_message(
            message.chat.id, 
            ban_message,
            reply_markup=banned_user_keyboard(),
            parse_mode="Markdown"
        )
        return False
    return True

# ---------------------------
# Start handler
# ---------------------------
@bot.message_handler(commands=["start", "help"])
def start_handler(message):
    uid = message.from_user.id

    if not is_subscribed(uid):
        send_subscribe_prompt(message.chat.id)
        return

    if uid == OWNER_ID:
        welcome_text = (
            f"👑 *Welcome back, Owner!*\n\n"
            "🤖 *Python/Node Hosting Bot*\n"
            f"🆔 Your ID: `{uid}`\n\n"
            "Use the buttons below to manage your bot."
        )
        
        owner_unban()
        
        try:
            photos = bot.get_user_profile_photos(uid, limit=1)
            if photos and photos.total_count > 0:
                file_id = photos.photos[0][0].file_id
                bot.send_photo(message.chat.id, file_id, caption=welcome_text, reply_markup=main_keyboard())
                return
        except Exception:
            pass
        
        bot.send_message(message.chat.id, welcome_text, reply_markup=main_keyboard())
        return

    if not banned_check(message):
        return

    user = get_user(uid)
    
    if user["banned"]:
        ban_message = "🚫 *You are banned from using this bot.*\n\n"
        ban_message += "If you think this is a mistake, please contact the owner."
        
        ikb = types.InlineKeyboardMarkup()
        ikb.add(types.InlineKeyboardButton("📞 Contact Owner", url=OWNER_TG_URL))
        
        bot.send_message(
            message.chat.id, 
            ban_message,
            reply_markup=banned_user_keyboard(),
            parse_mode="Markdown"
        )
        return

    welcome_text = (
        f"🌟 *Welcome, {message.from_user.first_name}*\n\n"
        "🤖 *Python/Node Hosting Bot*\n"
        "Host your Python (.py / requirements.txt) and Node (.js / package.json) projects.\n\n"
        f"🆔 Your ID: `{uid}`\n"
        f"📊 Credits: `{user['credits']}`\n"
        f"📦 Plan: {PLANS[user.get('plan', 'free')]['name']}\n\n"
        "Use the buttons below to upload and manage your projects."
    )
    
    try:
        photos = bot.get_user_profile_photos(uid, limit=1)
        if photos and photos.total_count > 0:
            file_id = photos.photos[0][0].file_id
            bot.send_photo(message.chat.id, file_id, caption=welcome_text, reply_markup=main_keyboard())
            return
    except Exception:
        pass
    
    bot.send_message(message.chat.id, welcome_text, reply_markup=main_keyboard())

@bot.message_handler(commands=["admin", "owner"])
def admin_command(message):
    uid = message.from_user.id
    
    if uid == OWNER_ID:
        owner_unban()
        admin_state[uid] = {"mode": "idle", "target": None}
        bot.send_message(message.chat.id, "👑 Owner Panel\nUse the buttons below.", reply_markup=admin_keyboard())
        return
    
    user = get_user(uid)
    if not user.get("admin"):
        bot.reply_to(message, "❌ You are not an admin.")
        return
    admin_state[uid] = {"mode": "idle", "target": None}
    bot.send_message(message.chat.id, "👑 Admin Panel\nUse the buttons below.", reply_markup=admin_keyboard())

@bot.message_handler(commands=["unbanme", "iamowner"])
def unban_owner_command(message):
    uid = message.from_user.id
    if uid == OWNER_ID:
        owner_unban()
        bot.reply_to(message, "✅ Owner status restored! You are now unbanned and admin.")
        
        admin_state[uid] = {"mode": "idle", "target": None}
        bot.send_message(message.chat.id, "👑 Owner Panel", reply_markup=admin_keyboard())
    else:
        bot.reply_to(message, "❌ This command is only for the bot owner.")

# ---------------------------
# Auto-delete helper
# ---------------------------
def auto_delete_message(chat_id: int, message_id: int, delay: int = 5):
    time.sleep(delay)
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass

# ---------------------------
# Runner utilities
# ---------------------------
def detect_project_type(folder: str) -> Dict[str, Any]:
    files = os.listdir(folder)
    info = {"type": "unknown", "entry": None, "preinstall": []}

    if "package.json" in files:
        info["type"] = "node"
        pkg_path = os.path.join(folder, "package.json")
        try:
            with open(pkg_path, "r", encoding="utf-8") as f:
                pkg = json.load(f)
            scripts = pkg.get("scripts", {})
            if "start" in scripts:
                info["entry"] = ["npm", "start"]
            else:
                mainf = pkg.get("main")
                if mainf and os.path.exists(os.path.join(folder, mainf)):
                    info["entry"] = ["node", mainf]
                elif os.path.exists(os.path.join(folder, "index.js")):
                    info["entry"] = ["node", "index.js"]
                else:
                    js_files = [f for f in files if f.endswith(".js")]
                    if js_files:
                        info["entry"] = ["node", js_files[0]]
            info["preinstall"] = [["npm", "install"]]
        except Exception:
            if os.path.exists(os.path.join(folder, "index.js")):
                info["type"] = "node"
                info["entry"] = ["node", "index.js"]
                info["preinstall"] = [["npm", "install"]]
    
    py_files = [f for f in files if f.endswith(".py")]
    if info["type"] == "unknown" and py_files:
        info["type"] = "python"
        if "main.py" in py_files:
            info["entry"] = ["python3", "main.py"]
        elif len(py_files) == 1:
            info["entry"] = ["python3", py_files[0]]
        else:
            info["entry"] = ["python3", "main.py"]
    
    js_files = [f for f in files if f.endswith(".js")]
    if info["type"] == "unknown" and js_files:
        info["type"] = "single_js"
        info["entry"] = ["node", js_files[0]]
    
    return info

WRAPPER_MAIN = """import subprocess, os, time, signal
procs = []
for fname in os.listdir('.'):
    if fname.endswith('.py') and fname != 'main.py':
        p = subprocess.Popen(['python3', fname])
        procs.append(p)
try:
    while True:
        time.sleep(1)
        procs = [p for p in procs if p.poll() is None]
        if not procs:
            break
except KeyboardInterrupt:
    for p in procs:
        try:
            p.terminate()
        except:
            pass
"""

def prepare_python_env(folder: str):
    venv_dir = os.path.join(folder, ".venv")
    if not os.path.exists(venv_dir):
        subprocess.run(["python3", "-m", "venv", ".venv"], cwd=folder, capture_output=True)
    req = os.path.join(folder, "requirements.txt")
    if os.path.exists(req):
        pip_exe = os.path.join(venv_dir, "bin", "pip")
        if not os.path.exists(pip_exe):
            pip_exe = "pip"
        subprocess.run([pip_exe, "install", "-r", "requirements.txt"], cwd=folder, capture_output=True)

def prepare_node(folder: str):
    if os.path.exists(os.path.join(folder, "package.json")):
        subprocess.run(["npm", "install"], cwd=folder, capture_output=True)

def stream_proc_output(proc: subprocess.Popen, log_path: str, uid: int, bot_id: str):
    try:
        with open(log_path, "ab") as logf:
            if proc.stdout:
                for line in proc.stdout:
                    try:
                        logf.write(line)
                        logf.flush()
                    except Exception:
                        pass
            if proc.stderr:
                for line in proc.stderr:
                    try:
                        logf.write(line)
                        logf.flush()
                    except Exception:
                        pass
        
        if proc.poll() is not None:
            key = f"{uid}_{bot_id}"
            if key in running_procs:
                del running_procs[key]
                save_running_procs()
                
    except Exception:
        pass

def run_project(uid: int, bot_id: str, folder: str, filename: str, chat_id: Optional[int] = None, auto_restart: bool = False):
    key = f"{uid}_{bot_id}"
    if key in running_procs:
        proc = running_procs[key]
        if proc and proc.poll() is None:
            if chat_id:
                bot.send_message(chat_id, f"ℹ️ `{filename}` is already running.")
            return

    info = detect_project_type(folder)
    if info["type"] == "unknown" or not info["entry"]:
        py_files = [f for f in os.listdir(folder) if f.endswith(".py")]
        if py_files:
            main_py = os.path.join(folder, "main.py")
            if not os.path.exists(main_py):
                with open(main_py, "w", encoding="utf-8") as f:
                    f.write(WRAPPER_MAIN)
            info = detect_project_type(folder)
        else:
            if chat_id:
                bot.send_message(chat_id, "❌ No runnable files (.py or .js) found.")
            return

    try:
        if info.get("preinstall"):
            if chat_id and not auto_restart:
                bot.send_message(chat_id, f"🔧 Running preinstall: `{' '.join(info['preinstall'][0])}` ...")
            for cmd in info["preinstall"]:
                subprocess.run(cmd, cwd=folder, timeout=600, capture_output=True)
        if info["type"] == "python":
            try:
                if chat_id and not auto_restart:
                    bot.send_message(chat_id, "🔧 Setting up Python virtualenv...")
                prepare_python_env(folder)
            except Exception as e:
                if chat_id and not auto_restart:
                    bot.send_message(chat_id, f"⚠️ venv/install error: `{e}` (continuing)")
        elif info["type"] == "node":
            try:
                if chat_id and not auto_restart:
                    bot.send_message(chat_id, "🔧 Installing Node dependencies...")
                prepare_node(folder)
            except Exception as e:
                if chat_id and not auto_restart:
                    bot.send_message(chat_id, f"⚠️ npm error: `{e}` (continuing)")
    except Exception as e:
        if chat_id and not auto_restart:
            bot.send_message(chat_id, f"❌ Preinstall error: `{e}`")
        return

    entry = info["entry"]
    log_path = os.path.join(folder, "run.log")
    try:
        proc = subprocess.Popen(
            entry,
            cwd=folder,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE,
            text=False
        )
        running_procs[key] = proc
        
        t = threading.Thread(
            target=stream_proc_output, 
            args=(proc, log_path, uid, bot_id), 
            daemon=True
        )
        t.start()
        
        save_running_procs()
        
        if chat_id:
            ikb = types.InlineKeyboardMarkup()
            ikb.row(
                types.InlineKeyboardButton("⏹ Stop", callback_data=f"file_stop_{uid}_{bot_id}"),
                types.InlineKeyboardButton("🔄 Restart", callback_data=f"file_restart_{uid}_{bot_id}")
            )
            ikb.row(
                types.InlineKeyboardButton("🗑 Delete", callback_data=f"file_del_{uid}_{bot_id}"),
                types.InlineKeyboardButton("📄 Show Log", callback_data=f"file_log_{uid}_{bot_id}")
            )
            
            status_msg = "🔄 Auto-restarted" if auto_restart else "▶️ Started"
            bot.send_message(chat_id, f"{status_msg} `{filename}` (PID: {proc.pid})", reply_markup=ikb)
        elif auto_restart:
            print(f"✅ Auto-restarted {filename} for user {uid}")
            
    except Exception as e:
        if chat_id:
            bot.send_message(chat_id, f"❌ Run error: `{e}`")

def stop_project(uid: int, bot_id: str, filename: str, chat_id: int):
    key = f"{uid}_{bot_id}"
    proc = running_procs.pop(key, None)
    if proc:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                if proc.poll() is None:
                    proc.kill()
            
            save_running_procs()
            
            bot.send_message(chat_id, f"⏹ `{filename}` stopped.")
        except Exception as e:
            bot.send_message(chat_id, f"❌ Stop error: `{e}`")
    else:
        bot.send_message(chat_id, f"ℹ️ `{filename}` is not running.")

def restart_project(uid: int, bot_id: str, filename: str, chat_id: int):
    key = f"{uid}_{bot_id}"
    
    proc = running_procs.pop(key, None)
    if proc:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                if proc.poll() is None:
                    proc.kill()
        except Exception:
            pass
    
    files = list_user_bots(uid)
    file = next((f for f in files if f["id"] == bot_id), None)
    if not file:
        bot.send_message(chat_id, "❌ File not found.")
        return
    
    folder = file["path"]
    bot.send_message(chat_id, f"🔄 Restarting `{filename}`...")
    run_project(uid, bot_id, folder, filename, chat_id)

def delete_project(uid: int, bot_id: str, folder: str, filename: str, chat_id: int):
    key = f"{uid}_{bot_id}"
    proc = running_procs.pop(key, None)
    if proc:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except Exception:
                if proc.poll() is None:
                    proc.kill()
        except Exception:
            pass
    
    save_running_procs()
    
    try:
        shutil.rmtree(folder, ignore_errors=True)
        bot.send_message(chat_id, f"🗑 `{filename}` deleted.")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Delete error: `{e}`")

def list_folder_files(chat_id: int, folder: str):
    lines = []
    for root, dirs, files in os.walk(folder):
        for f in files:
            rel = os.path.relpath(os.path.join(root, f), folder)
            lines.append(rel)
    if not lines:
        bot.send_message(chat_id, "📁 Folder empty.")
    else:
        msg = "📁 Files:\n" + "\n".join(f"- `{l}`" for l in lines[:200])
        bot.send_message(chat_id, msg[:4000])

def show_log(chat_id: int, folder: str, tail: int = 2000):
    log_file = os.path.join(folder, "run.log")
    if not os.path.exists(log_file):
        bot.send_message(chat_id, "ℹ️ No log found.")
        return
    try:
        with open(log_file, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            to_read = min(size, tail * 2)
            f.seek(max(0, size - to_read))
            content = f.read().decode(errors="replace")
        
        if len(content) < 4000:
            bot.send_message(chat_id, f"```\n{content}\n```")
        else:
            for i in range(0, len(content), 3800):
                bot.send_message(chat_id, f"```\n{content[i:i+3800]}\n```")
    except Exception as e:
        bot.send_message(chat_id, f"❌ Log read error: `{e}`")

# ---------------------------
# Document handler with plan limits
# ---------------------------
@bot.message_handler(content_types=["document"])
def handle_document(message):
    uid = message.from_user.id

    if not is_subscribed(uid):
        send_subscribe_prompt(message.chat.id)
        return
    
    if uid != OWNER_ID and not banned_check(message):
        return

    user = get_user(uid)
    
    # Check bot limit
    if not can_upload_more_bots(uid):
        current_bots = len(list_user_bots(uid))
        max_bots = user.get("max_bots", 1)
        bot.reply_to(
            message, 
            f"❌ You have reached your bot limit!\n\n"
            f"Current Bots: {current_bots}/{max_bots}\n"
            f"Plan: {PLANS[user.get('plan', 'free')]['name']}\n\n"
            "Upgrade your plan to host more bots."
        )
        return

    if user["credits"] <= 0:
        bot.reply_to(message, "❌ You have no credits. Ask the owner to add credits.")
        return

    doc = message.document
    fname = doc.file_name or ""
    ext = os.path.splitext(fname)[1].lower()

    size_mb = doc.file_size / (1024 * 1024) if doc.file_size else 0
    if size_mb > user["upload_mb"]:
        bot.reply_to(
            message, 
            f"❌ File too large. Limit: {user['upload_mb']} MB.\n\n"
            f"Your Plan: {PLANS[user.get('plan', 'free')]['name']}\n"
            f"Max Upload: {user['upload_mb']} MB\n\n"
            "Upgrade your plan for higher limits."
        )
        return

    status = bot.reply_to(message, "⏳ Uploading...")

    try:
        file_info = bot.get_file(doc.file_id)
        downloaded = bot.download_file(file_info.file_path)
    except Exception as e:
        bot.reply_to(message, f"❌ Download error: `{e}`")
        return

    bot_id = str(int(time.time()))
    folder = bot_folder(uid, bot_id)
    os.makedirs(folder, exist_ok=True)

    try:
        save_bot_meta(uid, bot_id, fname)

        if ext == ".zip":
            zip_path = os.path.join(folder, fname)
            with open(zip_path, "wb") as f:
                f.write(downloaded)
            try:
                with zipfile.ZipFile(zip_path, "r") as z:
                    safe_extract_zip(z, folder)
            except Exception as e:
                bot.reply_to(message, f"❌ Zip extract error: `{e}`")
                shutil.rmtree(folder, ignore_errors=True)
                return
        else:
            out_path = os.path.join(folder, fname)
            with open(out_path, "wb") as f:
                f.write(downloaded)

        new_credits = max(0, user["credits"] - 1)
        update_user(uid, "credits", new_credits)
        
        try:
            bot.delete_message(status.chat.id, status.message_id)
        except Exception:
            pass

    except Exception as e:
        bot.reply_to(message, f"❌ Save error: `{e}`")
        shutil.rmtree(folder, ignore_errors=True)
        return

    text = (
        f"✅ File received.\n\n"
        f"📄 Name: `{fname}`\n"
        f"🆔 ID: `{bot_id}`\n"
        f"💰 Credits left: `{new_credits}`\n"
        f"📦 Bots: {len(list_user_bots(uid))}/{user.get('max_bots', 1)}\n"
        "Choose an action:"
    )
    ikb = types.InlineKeyboardMarkup()
    ikb.row(
        types.InlineKeyboardButton("▶ Run", callback_data=f"file_run_{uid}_{bot_id}"),
        types.InlineKeyboardButton("⏹ Stop", callback_data=f"file_stop_{uid}_{bot_id}"),
        types.InlineKeyboardButton("🔄 Restart", callback_data=f"file_restart_{uid}_{bot_id}")
    )
    ikb.row(
        types.InlineKeyboardButton("🗑 Delete", callback_data=f"file_del_{uid}_{bot_id}"),
        types.InlineKeyboardButton("📄 Show Log", callback_data=f"file_log_{uid}_{bot_id}")
    )
    bot.send_message(message.chat.id, text, reply_markup=ikb)

# ---------------------------
# Callback handler for file actions
# ---------------------------
@bot.callback_query_handler(func=lambda c: c.data.startswith("file_"))
def file_callback(call):
    try:
        _, action, uid_str, bot_id = call.data.split("_", 3)
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    
    file_owner = int(uid_str)
    clicker = call.from_user.id

    if clicker != file_owner and clicker != OWNER_ID:
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return

    files = list_user_bots(file_owner)
    file = next((f for f in files if f["id"] == bot_id), None)
    if not file:
        bot.answer_callback_query(call.id, "File not found.")
        return

    filename = file["name"]
    folder = file["path"]
    chat_id = call.message.chat.id
    bot.answer_callback_query(call.id)

    if action == "run":
        run_project(file_owner, bot_id, folder, filename, chat_id)
    elif action == "stop":
        stop_project(file_owner, bot_id, filename, chat_id)
    elif action == "restart":
        restart_project(file_owner, bot_id, filename, chat_id)
    elif action == "del":
        delete_project(file_owner, bot_id, folder, filename, chat_id)
    elif action == "ls":
        list_folder_files(chat_id, folder)
    elif action == "log":
        show_log(chat_id, folder)

# ---------------------------
# Admin-state handler
# ---------------------------
@bot.message_handler(func=lambda m: m.from_user.id in admin_state and (not m.text or m.text not in ALL_BUTTONS))
def handle_admin_state(message):
    uid = message.from_user.id
    st = admin_state.get(uid)
    if not st:
        return
    mode = st.get("mode")
    text = (message.text or "").strip()

    if mode == "choose_user":
        if text.lower() == "me":
            target = uid
        else:
            try:
                target = int(text)
            except Exception:
                bot.reply_to(message, "❌ Send a valid user ID or `me`.")
                return
        if str(target) not in db:
            bot.reply_to(message, "⚠️ User not found in DB.")
            return
        st["target"] = target
        st["mode"] = "selected"
        show_admin_user(message.chat.id, target)
        return

    if mode in ("broadcast_target", "add_credits", "set_upload", "set_plan_user") and not st.get("target"):
        bot.reply_to(message, "❌ No target user selected.")
        admin_state.pop(uid, None)
        return

    if mode == "broadcast":
        send_broadcast_message(message, target=None)
        admin_state[uid]["mode"] = "idle"
        return

    if mode == "broadcast_user":
        target = st.get("target")
        if not target:
            bot.reply_to(message, "❌ No target selected.")
            return
        send_broadcast_message(message, target=target)
        admin_state[uid]["mode"] = "idle"
        return

    if mode == "set_plan_user":
        target = st.get("target")
        if text in PLANS:
            # Ask for confirmation
            plan = PLANS[text]
            ikb = types.InlineKeyboardMarkup()
            ikb.row(
                types.InlineKeyboardButton("✅ Confirm", callback_data=f"confirm_plan_{target}_{text}_{uid}"),
                types.InlineKeyboardButton("❌ Cancel", callback_data=f"cancel_plan_{uid}")
            )
            
            expiry_msg = ""
            if text != "free":
                expiry_date = datetime.now() + timedelta(days=plan["duration_days"])
                expiry_msg = f"\n⏰ Plan Valid Till: {expiry_date.strftime('%d %B %Y')}"
            
            bot.send_message(
                message.chat.id,
                f"📋 *Confirm Plan Assignment*\n\n"
                f"👤 User ID: `{target}`\n"
                f"📦 Plan: {plan['name']}\n"
                f"💰 Price: {plan['price']}\n"
                f"📊 Max Bots: {plan['max_bots']}\n"
                f"☁️ Upload Limit: {plan['upload_limit_mb']}MB\n"
                f"🎁 Credits: {plan['credits']}\n"
                f"{expiry_msg}\n\n"
                f"Are you sure you want to assign this plan?",
                reply_markup=ikb,
                parse_mode="Markdown"
            )
            admin_state[uid]["mode"] = "idle"
        else:
            bot.reply_to(message, "❌ Invalid plan. Choose from: free, basic, pro, premium")
        return

    try:
        val = int(text)
    except Exception:
        bot.reply_to(message, "❌ Send a number.")
        return

    target = st.get("target")
    if mode == "add_credits":
        curr = get_user(target)["credits"]
        update_user(target, "credits", curr + val)
        bot.reply_to(message, f"✅ Credits updated: `{curr}` → `{curr + val}`")
    elif mode == "set_upload":
        update_user(target, "upload_mb", val)
        bot.reply_to(message, f"✅ Upload limit set to `{val} MB`")
    admin_state[uid]["mode"] = "selected"
    show_admin_user(message.chat.id, target)

# ---------------------------
# Broadcast sender
# ---------------------------
def replace_placeholders(text: str, target_uid: int = None):
    if target_uid:
        try:
            profile = bot.get_chat(target_uid)
            uname = profile.username
        except Exception:
            uname = None
        if uname:
            mention = f"@{uname}"
        else:
            mention = f"[{target_uid}](tg://user?id={target_uid})"
        text = text.replace("[USERNAME]", uname if uname else str(target_uid))
        text = text.replace("[MENTION]", mention)
    return text

def send_broadcast_message(message, target: int = None):
    sender_id = message.from_user.id
    if target:
        if message.content_type == "text":
            text = replace_placeholders(message.text, target_uid=target)
            try:
                bot.send_message(target, text, parse_mode="Markdown")
                bot.reply_to(message, "✅ Broadcast sent to target user.")
            except Exception as e:
                bot.reply_to(message, f"❌ Send error: {e}")
        else:
            try:
                bot.copy_message(target, message.chat.id, message.message_id)
                bot.reply_to(message, "✅ Broadcast media sent.")
            except Exception as e:
                bot.reply_to(message, f"❌ Send error: {e}")
        return

    sent = 0
    failed = 0
    info = bot.reply_to(message, "📢 Broadcast starting...")
    for uid_s in list(db.keys()):
        try:
            uid = int(uid_s)
            if message.content_type == "text":
                text = replace_placeholders(message.text, target_uid=uid)
                bot.send_message(uid, text, parse_mode="Markdown")
            else:
                bot.copy_message(uid, message.chat.id, message.message_id)
            sent += 1
            time.sleep(0.05)
        except Exception:
            failed += 1
    try:
        bot.edit_message_text(f"✅ Broadcast complete.\nSent: {sent}\nFailed: {failed}", info.chat.id, info.message_id)
    except Exception:
        pass

# ---------------------------
# User List Feature
# ---------------------------
def get_user_list_page(page: int = 1, items_per_page: int = 10):
    users_list = []
    for user_id_str, user_data in db.items():
        user_id = int(user_id_str)
        try:
            chat = bot.get_chat(user_id)
            username = f"@{chat.username}" if chat.username else "No username"
            first_name = chat.first_name or ""
        except Exception:
            username = "Unknown"
            first_name = ""
        
        joined_at = user_data.get("joined_at", "Unknown")
        try:
            join_date = datetime.fromisoformat(joined_at).strftime("%Y-%m-%d") if joined_at != "Unknown" else "Unknown"
        except Exception:
            join_date = "Unknown"
        
        plan = user_data.get("plan", "free")
        plan_name = PLANS[plan]["name"]
        
        users_list.append({
            "id": user_id,
            "username": username,
            "name": first_name,
            "credits": user_data.get("credits", 0),
            "upload_mb": user_data.get("upload_mb", 10),
            "banned": user_data.get("banned", False),
            "admin": user_data.get("admin", False),
            "plan": plan_name,
            "joined": join_date
        })
    
    users_list.sort(key=lambda x: x["id"], reverse=True)
    
    total_users = len(users_list)
    total_pages = (total_users + items_per_page - 1) // items_per_page
    
    if page < 1 or page > total_pages:
        page = 1
    
    start_idx = (page - 1) * items_per_page
    end_idx = start_idx + items_per_page
    
    return {
        "users": users_list[start_idx:end_idx],
        "page": page,
        "total_pages": total_pages,
        "total_users": total_users
    }

def show_user_list(chat_id: int, page: int = 1):
    data = get_user_list_page(page)
    
    if not data["users"]:
        bot.send_message(chat_id, "📭 No users found in database.")
        return
    
    message_text = f"📋 *User List* (Page {data['page']}/{data['total_pages']})\n"
    message_text += f"👥 Total Users: `{data['total_users']}`\n\n"
    
    for idx, user_info in enumerate(data["users"], start=(data['page']-1)*10+1):
        status = "🔴" if user_info["banned"] else "🟢"
        admin = " 👑" if user_info["admin"] else ""
        message_text += f"{idx}. {status}{admin} `{user_info['id']}`\n"
        message_text += f"   👤 Name: {user_info['name']}\n"
        message_text += f"   📱 Username: {user_info['username']}\n"
        message_text += f"   💰 Credits: {user_info['credits']}\n"
        message_text += f"   📦 Plan: {user_info['plan']}\n\n"
    
    ikb = types.InlineKeyboardMarkup()
    
    if data["total_pages"] > 1:
        row_buttons = []
        if data["page"] > 1:
            row_buttons.append(types.InlineKeyboardButton("⬅️ Previous", callback_data=f"userlist_{data['page']-1}"))
        
        row_buttons.append(types.InlineKeyboardButton(f"{data['page']}/{data['total_pages']}", callback_data="noop"))
        
        if data["page"] < data["total_pages"]:
            row_buttons.append(types.InlineKeyboardButton("Next ➡️", callback_data=f"userlist_{data['page']+1}"))
        
        if len(row_buttons) > 1:
            ikb.row(*row_buttons)
    
    ikb.row(
        types.InlineKeyboardButton("📊 Export JSON", callback_data="export_users"),
        types.InlineKeyboardButton("📈 Stats", callback_data="user_stats")
    )
    ikb.row(types.InlineKeyboardButton("❌ Close", callback_data="close_list"))
    
    bot.send_message(chat_id, message_text[:4000], reply_markup=ikb)

# ---------------------------
# Show Plans
# ---------------------------
def show_plans(chat_id: int):
    """Show all available plans"""
    plans_text = "💰 *Available Plans*\n\n"
    
    for plan_id, plan in PLANS.items():
        plans_text += f"{plan['name']} - {plan['price']}\n"
        for feature in plan["features"]:
            plans_text += f"  {feature}\n"
        plans_text += "\n"
    
    plans_text += "\nTo purchase a plan, contact the owner."
    
    ikb = types.InlineKeyboardMarkup()
    ikb.row(
        types.InlineKeyboardButton("📞 Contact Owner", url=OWNER_TG_URL),
        types.InlineKeyboardButton("👤 Owner ID", callback_data="show_owner_info")
    )
    
    bot.send_message(chat_id, plans_text, reply_markup=ikb, parse_mode="Markdown")

# ---------------------------
# Show Stats
# ---------------------------
def show_stats(chat_id: int, is_admin: bool = False):
    """Show user/plan statistics"""
    stats = get_user_stats()
    
    if is_admin:
        stats_text = (
            "📊 *Admin Statistics*\n\n"
            f"👥 Total Users: `{stats['total_users']}`\n"
            f"🆓 Free Users: `{stats['free_users']}`\n"
            f"💰 Premium Users: `{stats['premium_users']}`\n"
            f"✅ Active Premium: `{stats['active_premium']}`\n"
            f"💸 Total Revenue: `₹{stats['total_revenue']}`\n\n"
            f"📦 *Plan Distribution:*\n"
            f"• 🆓 Free: `{stats['plan_counts']['free']}`\n"
            f"• 🥈 Basic: `{stats['plan_counts']['basic']}`\n"
            f"• 🥇 Pro: `{stats['plan_counts']['pro']}`\n"
            f"• 👑 Premium: `{stats['plan_counts']['premium']}`\n\n"
            f"📅 Last Updated: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
        )
    else:
        user = get_user(chat_id)
        plan = user.get("plan", "free")
        plan_info = PLANS[plan]
        
        expiry_msg = ""
        if user.get("plan_expiry"):
            try:
                expiry_date = datetime.fromisoformat(user["plan_expiry"])
                days_left = (expiry_date - datetime.now()).days
                expiry_msg = f"\n⏰ Days Left: `{days_left}`"
            except Exception:
                pass
        
        stats_text = (
            "📊 *Your Statistics*\n\n"
            f"📦 Current Plan: {plan_info['name']}\n"
            f"💰 Credits: `{user['credits']}`\n"
            f"☁️ Upload Limit: `{user['upload_mb']} MB`\n"
            f"🤖 Bots: `{len(list_user_bots(chat_id))}/{user.get('max_bots', 1)}`{expiry_msg}\n\n"
            f"📅 Joined: `{user.get('joined_at', 'Unknown')[:10] if 'joined_at' in user else 'Unknown'}`"
        )
    
    bot.send_message(chat_id, stats_text, parse_mode="Markdown")

# ---------------------------
# Main menu handler
# ---------------------------
@bot.message_handler(func=lambda m: m.text in ALL_BUTTONS)
def main_menu_handler(message):
    uid = message.from_user.id

    if not is_subscribed(uid):
        send_subscribe_prompt(message.chat.id)
        return

    if uid == OWNER_ID:
        owner_unban()
        user = get_user(uid)
        text = message.text.strip()
        
        if text == BTN_UPLOAD:
            msg = bot.reply_to(message, "📤 Send your file (.py/.js) or .zip archive.")
            threading.Thread(target=auto_delete_message, args=(msg.chat.id, msg.message_id, 6), daemon=True).start()
        
        elif text == BTN_MYFILES:
            files = list_user_bots(uid)
            if not files:
                bot.reply_to(message, "📭 You have no uploaded files.")
                return
            
            for i, f in enumerate(files, start=1):
                key = f"{uid}_{f['id']}"
                is_running = key in running_procs
                status = "🟢 Running" if is_running else "🔴 Stopped"
                
                try:
                    total_size = 0
                    for dirpath, dirnames, filenames in os.walk(f['path']):
                        for filename in filenames:
                            fp = os.path.join(dirpath, filename)
                            total_size += os.path.getsize(fp)
                    size_mb = total_size / (1024 * 1024)
                    size_str = f"{size_mb:.2f} MB"
                except:
                    size_str = "Unknown size"
                
                msg = f"📁 *File {i}*\n"
                msg += f"📄 Name: `{f['name']}`\n"
                msg += f"🆔 ID: `{f['id']}`\n"
                msg += f"📊 Status: {status}\n"
                msg += f"💾 Size: {size_str}\n"
                
                ikb = types.InlineKeyboardMarkup()
                
                if is_running:
                    ikb.row(
                        types.InlineKeyboardButton("⏹ Stop", callback_data=f"file_stop_{uid}_{f['id']}"),
                        types.InlineKeyboardButton("🔄 Restart", callback_data=f"file_restart_{uid}_{f['id']}")
                    )
                else:
                    ikb.row(
                        types.InlineKeyboardButton("▶ Run", callback_data=f"file_run_{uid}_{f['id']}"),
                        types.InlineKeyboardButton("🗑 Delete", callback_data=f"file_del_{uid}_{f['id']}")
                    )
                
                ikb.row(
                    types.InlineKeyboardButton("📁 View Files", callback_data=f"file_ls_{uid}_{f['id']}"),
                    types.InlineKeyboardButton("📄 Show Log", callback_data=f"file_log_{uid}_{f['id']}")
                )
                
                bot.send_message(message.chat.id, msg, reply_markup=ikb)
            
            summary = f"📊 *Summary*\nTotal Files: {len(files)}\n"
            running_count = sum(1 for f in files if f"{uid}_{f['id']}" in running_procs)
            summary += f"Running: {running_count}\nStopped: {len(files) - running_count}"
            bot.send_message(message.chat.id, summary)
        
        elif text == BTN_PLAN:
            msg = (
                "👑 *Owner Plan:*\n\n"
                f"💰 Credits: `{user['credits']}` (Unlimited)\n"
                f"☁️ Max Upload: `{user['upload_mb']} MB`\n"
                f"🤖 Max Bots: `{user.get('max_bots', 999)}`\n"
                f"📦 Plan: {PLANS[user.get('plan', 'premium')]['name']}\n"
                f"📅 Joined: `{user.get('joined_at', 'Unknown')[:10] if 'joined_at' in user else 'Unknown'}`"
            )
            bot.reply_to(message, msg)
        
        elif text == BTN_CONTACT:
            contact_text = (
                "📞 *Owner Information*\n\n"
                "👤 Username: @Contact1432\n"
                "🆔 Owner ID: `8336467661`\n"
                "📧 Contact: @Contact1432"
            )
            bot.send_message(message.chat.id, contact_text, parse_mode="Markdown")
        
        elif text == BTN_PLANS:
            show_plans(message.chat.id)
        
        elif text == BTN_STATS:
            show_stats(message.chat.id, is_admin=True)
        
        elif text == BTN_BACK_MAIN:
            bot.send_message(message.chat.id, "🏠 Main menu", reply_markup=main_keyboard())
            admin_state.pop(uid, None)
        
        elif text == BTN_MANAGE:
            admin_state[uid] = {"mode": "choose_user", "target": None}
            bot.send_message(message.chat.id, "👤 Send user ID to manage (or `me` for yourself):")
        
        elif text == BTN_BROADCAST:
            ikb = types.InlineKeyboardMarkup()
            ikb.row(types.InlineKeyboardButton("📣 Broadcast to ALL", callback_data=f"broadcast_all_{uid}"))
            ikb.row(types.InlineKeyboardButton("👤 Broadcast to a USER", callback_data=f"broadcast_user_{uid}"))
            ikb.row(types.InlineKeyboardButton("⬅️ Cancel", callback_data=f"broadcast_cancel_{uid}"))
            bot.send_message(message.chat.id, "📢 Choose broadcast type. You can use [USERNAME] or [MENTION] placeholders.", reply_markup=ikb)
        
        elif text == BTN_USERLIST:
            show_user_list(message.chat.id)
        
        elif text == BTN_GIVE_PLAN:
            admin_state[uid] = {"mode": "choose_user", "target": None}
            bot.send_message(message.chat.id, "👤 Send user ID to give plan (or `me` for yourself):")
        
        elif text == BTN_BACK_ADMIN:
            st = admin_state.get(uid)
            if st and st.get("target"):
                admin_state[uid] = {"mode": "idle", "target": None}
            bot.send_message(message.chat.id, "👑 Owner Panel", reply_markup=admin_keyboard())
        
        elif text in (BTN_BAN, BTN_ADDCRED, BTN_SET_UPLOAD, BTN_SET_PLAN):
            st = admin_state.get(uid)
            if not st or not st.get("target"):
                bot.reply_to(message, "Select a user first using Manage User.")
                return
            target = st["target"]
            
            if text == BTN_BAN and target == OWNER_ID:
                bot.reply_to(message, "❌ You cannot ban the owner!")
                show_admin_user(message.chat.id, target)
                return
                
            if text == BTN_BAN:
                u = get_user(target)
                new_status = not u["banned"]
                update_user(target, "banned", new_status)
                status = "BANNED" if new_status else "ACTIVE"
                bot.reply_to(message, f"✅ User `{target}` is now {status}.")
                
                if new_status:
                    ban_notification = "🚫 *You have been banned from using this bot.*\n\n"
                    ban_notification += "If you think this is a mistake, please contact the owner."
                    
                    ikb = types.InlineKeyboardMarkup()
                    ikb.add(types.InlineKeyboardButton("📞 Contact @rdxking1000", url=OWNER_TG_URL))
                    
                    try:
                        bot.send_message(
                            target,
                            ban_notification,
                            reply_markup=banned_user_keyboard(),
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
                
                show_admin_user(message.chat.id, target)
                admin_state[uid]["mode"] = "selected"
            
            elif text == BTN_ADDCRED:
                admin_state[uid]["mode"] = "add_credits"
                bot.send_message(message.chat.id, "💳 Send amount to add credits (positive to add, negative to deduct):")
            
            elif text == BTN_SET_UPLOAD:
                admin_state[uid]["mode"] = "set_upload"
                bot.send_message(message.chat.id, "☁️ Send new upload limit in MB (number):")
            
            elif text == BTN_SET_PLAN:
                admin_state[uid]["mode"] = "set_plan_user"
                plan_options = "Available Plans:\n"
                for plan_id, plan in PLANS.items():
                    plan_options += f"• `{plan_id}` - {plan['name']} ({plan['price']})\n"
                bot.send_message(message.chat.id, f"Select plan type:\n\n{plan_options}")
        
        return

    # Normal user handling
    user = get_user(uid)
    
    if user["banned"] and message.text not in [BTN_CONTACT, BTN_BACK_MAIN]:
        ban_message = "🚫 *You are banned from using this bot.*\n\n"
        ban_message += "If you think this is a mistake, please contact the owner."
        
        ikb = types.InlineKeyboardMarkup()
        ikb.add(types.InlineKeyboardButton("📞 Contact", url=OWNER_TG_URL))
        
        bot.send_message(
            message.chat.id, 
            ban_message,
            reply_markup=banned_user_keyboard(),
            parse_mode="Markdown"
        )
        return

    if not banned_check(message):
        return

    text = message.text.strip()

    if text == BTN_UPLOAD:
        if not can_upload_more_bots(uid):
            current_bots = len(list_user_bots(uid))
            max_bots = user.get("max_bots", 1)
            bot.reply_to(
                message, 
                f"❌ You have reached your bot limit!\n\n"
                f"Current Bots: {current_bots}/{max_bots}\n"
                f"Plan: {PLANS[user.get('plan', 'free')]['name']}\n\n"
                "Upgrade your plan to host more bots."
            )
            return
        
        msg = bot.reply_to(message, "📤 Send your file (.py/.js) or .zip archive.\nEach upload costs 1 credit.")
        threading.Thread(target=auto_delete_message, args=(msg.chat.id, msg.message_id, 6), daemon=True).start()
    
    elif text == BTN_MYFILES:
        files = list_user_bots(uid)
        if not files:
            bot.reply_to(message, "📭 You have no uploaded files.")
            return
        
        for i, f in enumerate(files, start=1):
            key = f"{uid}_{f['id']}"
            is_running = key in running_procs
            status = "🟢 Running" if is_running else "🔴 Stopped"
            
            try:
                total_size = 0
                for dirpath, dirnames, filenames in os.walk(f['path']):
                    for filename in filenames:
                        fp = os.path.join(dirpath, filename)
                        total_size += os.path.getsize(fp)
                size_mb = total_size / (1024 * 1024)
                size_str = f"{size_mb:.2f} MB"
            except:
                size_str = "Unknown size"
            
            msg = f"📁 *File {i}*\n"
            msg += f"📄 Name: `{f['name']}`\n"
            msg += f"🆔 ID: `{f['id']}`\n"
            msg += f"📊 Status: {status}\n"
            msg += f"💾 Size: {size_str}\n"
            
            ikb = types.InlineKeyboardMarkup()
            
            if is_running:
                ikb.row(
                    types.InlineKeyboardButton("⏹ Stop", callback_data=f"file_stop_{uid}_{f['id']}"),
                    types.InlineKeyboardButton("🔄 Restart", callback_data=f"file_restart_{uid}_{f['id']}")
                )
            else:
                ikb.row(
                    types.InlineKeyboardButton("▶ Run", callback_data=f"file_run_{uid}_{f['id']}"),
                    types.InlineKeyboardButton("🗑 Delete", callback_data=f"file_del_{uid}_{f['id']}")
                )
            
            ikb.row(
                types.InlineKeyboardButton("📁 View Files", callback_data=f"file_ls_{uid}_{f['id']}"),
                types.InlineKeyboardButton("📄 Show Log", callback_data=f"file_log_{uid}_{f['id']}")
            )
            
            bot.send_message(message.chat.id, msg, reply_markup=ikb)
        
        summary = f"📊 *Summary*\nTotal Files: {len(files)}\n"
        running_count = sum(1 for f in files if f"{uid}_{f['id']}" in running_procs)
        summary += f"Running: {running_count}\nStopped: {len(files) - running_count}"
        bot.send_message(message.chat.id, summary)
    
    elif text == BTN_PLAN:
        current_plan = user.get("plan", "free")
        plan_info = PLANS[current_plan]
        
        expiry_msg = ""
        if user.get("plan_expiry"):
            try:
                expiry_date = datetime.fromisoformat(user["plan_expiry"])
                expiry_msg = f"\n⏰ Plan Valid Till: `{expiry_date.strftime('%d %B %Y')}`"
            except Exception:
                pass
        
        msg = (
            "⚙️ *Your Plan / Limits:*\n\n"
            f"📦 Current Plan: {plan_info['name']}\n"
            f"💰 Credits: `{user['credits']}` (1 upload = 1 credit)\n"
            f"☁️ Max Upload: `{user['upload_mb']} MB`\n"
            f"🤖 Max Bots: `{user.get('max_bots', 1)}`\n"
            f"📅 Joined: `{user.get('joined_at', 'Unknown')[:10] if 'joined_at' in user else 'Unknown'}`"
            f"{expiry_msg}\n\n"
            f"Tap *Buy Plans* to upgrade your plan!"
        )
        bot.reply_to(message, msg)
    
    elif text == BTN_CONTACT:
        contact_text = (
            "📞 *Contact Owner*\n\n"
            "If you have any questions or need help, contact:\n"
            f"👤 Username: @SkAnish143\n"
            f"🆔 Owner ID: `8336467661`"
        )
        
        ikb = types.InlineKeyboardMarkup()
        ikb.add(types.InlineKeyboardButton("📞 Contact @rdxking1000", url=OWNER_TG_URL))
        
        bot.send_message(message.chat.id, contact_text, reply_markup=ikb, parse_mode="Markdown")
    
    elif text == BTN_PLANS:
        show_plans(message.chat.id)
    
    elif text == BTN_STATS:
        show_stats(message.chat.id, is_admin=False)
    
    elif text == BTN_BACK_MAIN:
        bot.send_message(message.chat.id, "🏠 Main menu", reply_markup=main_keyboard())
        admin_state.pop(uid, None)
    
    elif text == BTN_MANAGE:
        if not (user.get("admin") or uid == OWNER_ID):
            return
        admin_state[uid] = {"mode": "choose_user", "target": None}
        bot.send_message(message.chat.id, "👤 Send user ID to manage (or `me` for yourself):")
    
    elif text == BTN_BROADCAST:
        if not (user.get("admin") or uid == OWNER_ID):
            return
        ikb = types.InlineKeyboardMarkup()
        ikb.row(types.InlineKeyboardButton("📣 Broadcast to ALL", callback_data=f"broadcast_all_{uid}"))
        ikb.row(types.InlineKeyboardButton("👤 Broadcast to a USER", callback_data=f"broadcast_user_{uid}"))
        ikb.row(types.InlineKeyboardButton("⬅️ Cancel", callback_data=f"broadcast_cancel_{uid}"))
        bot.send_message(message.chat.id, "📢 Choose broadcast type. You can use [USERNAME] or [MENTION] placeholders.", reply_markup=ikb)
    
    elif text == BTN_USERLIST:
        if not (user.get("admin") or uid == OWNER_ID):
            return
        show_user_list(message.chat.id)
    
    elif text == BTN_GIVE_PLAN:
        if not (user.get("admin") or uid == OWNER_ID):
            return
        admin_state[uid] = {"mode": "choose_user", "target": None}
        bot.send_message(message.chat.id, "👤 Send user ID to give plan (or `me` for yourself):")
    
    elif text == BTN_BACK_ADMIN:
        st = admin_state.get(uid)
        if st and st.get("target"):
            admin_state[uid] = {"mode": "idle", "target": None}
        bot.send_message(message.chat.id, "👑 Admin Panel", reply_markup=admin_keyboard())
    
    elif text in (BTN_BAN, BTN_ADDCRED, BTN_SET_UPLOAD, BTN_SET_PLAN):
        st = admin_state.get(uid)
        if not st or not st.get("target"):
            bot.reply_to(message, "Select a user first using Manage User.")
            return
        target = st["target"]
        
        if text == BTN_BAN and target == OWNER_ID:
            bot.reply_to(message, "❌ You cannot ban the owner!")
            show_admin_user(message.chat.id, target)
            return
            
        if text == BTN_BAN:
            u = get_user(target)
            new_status = not u["banned"]
            update_user(target, "banned", new_status)
            status = "BANNED" if new_status else "ACTIVE"
            bot.reply_to(message, f"✅ User `{target}` is now {status}.")
            
            if new_status:
                ban_notification = "🚫 *You have been banned from using this bot.*\n\n"
                ban_notification += "If you think this is a mistake, please contact the owner."
                
                ikb = types.InlineKeyboardMarkup()
                ikb.add(types.InlineKeyboardButton("📞 Contact @rdxking1000", url=OWNER_TG_URL))
                
                try:
                    bot.send_message(
                        target,
                        ban_notification,
                        reply_markup=banned_user_keyboard(),
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
            
            show_admin_user(message.chat.id, target)
            admin_state[uid]["mode"] = "selected"
        
        elif text == BTN_ADDCRED:
            admin_state[uid]["mode"] = "add_credits"
            bot.send_message(message.chat.id, "💳 Send amount to add credits (positive to add, negative to deduct):")
        
        elif text == BTN_SET_UPLOAD:
            admin_state[uid]["mode"] = "set_upload"
            bot.send_message(message.chat.id, "☁️ Send new upload limit in MB (number):")
        
        elif text == BTN_SET_PLAN:
            admin_state[uid]["mode"] = "set_plan_user"
            plan_options = "Available Plans:\n"
            for plan_id, plan in PLANS.items():
                plan_options += f"• `{plan_id}` - {plan['name']} ({plan['price']})\n"
            bot.send_message(message.chat.id, f"Select plan type:\n\n{plan_options}")

# ---------------------------
# Callback handlers
# ---------------------------
@bot.callback_query_handler(func=lambda c: c.data.startswith("broadcast_"))
def broadcast_callback(call):
    try:
        _, action, uid_str = call.data.split("_", 2)
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    
    admin_id = int(uid_str)
    if call.from_user.id != admin_id:
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    bot.answer_callback_query(call.id)

    if action == "all":
        admin_state[admin_id] = {"mode": "broadcast", "target": None}
        bot.send_message(call.message.chat.id, "📢 Send the message (text or media) to broadcast to ALL users.")
    elif action == "user":
        admin_state[admin_id] = {"mode": "broadcast_target", "target": None}
        bot.send_message(call.message.chat.id, "👤 Send the target user ID to broadcast to (or `me`):")
    elif action == "cancel":
        admin_state.pop(admin_id, None)
        bot.send_message(call.message.chat.id, "❌ Broadcast cancelled.")

@bot.callback_query_handler(func=lambda c: c.data.startswith("userlist_"))
def userlist_callback(call):
    try:
        _, page_str = call.data.split("_")
        page = int(page_str)
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    
    uid = call.from_user.id
    if not (get_user(uid).get("admin") or uid == OWNER_ID):
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    
    show_user_list(call.message.chat.id, page)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("confirm_plan_"))
def confirm_plan_callback(call):
    try:
        _, _, target_str, plan_type, admin_str = call.data.split("_", 4)
        target = int(target_str)
        admin_id = int(admin_str)
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    
    if call.from_user.id != admin_id:
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id, "✅ Plan assigned successfully!")
    
    # Apply the plan
    expiry = apply_plan_to_user(target, plan_type)
    
    # Update total spent for paid plans
    if plan_type != "free":
        try:
            price = int(''.join(filter(str.isdigit, PLANS[plan_type]["price"])))
            user = get_user(target)
            user["total_spent"] = user.get("total_spent", 0) + price
            save_db()
        except Exception:
            pass
    
    # Notify admin
    expiry_msg = ""
    if expiry:
        expiry_msg = f"\n⏰ Expiry Date: {expiry.strftime('%d %B %Y')}"
    
    bot.send_message(
        admin_id,
        f"✅ *Plan Assigned Successfully!*\n\n"
        f"👤 User ID: `{target}`\n"
        f"📦 Plan: {PLANS[plan_type]['name']}\n"
        f"💰 Price: {PLANS[plan_type]['price']}{expiry_msg}",
        parse_mode="Markdown"
    )
    
    # Delete the confirmation message
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data.startswith("cancel_plan_"))
def cancel_plan_callback(call):
    try:
        _, _, admin_str = call.data.split("_", 2)
        admin_id = int(admin_str)
    except ValueError:
        bot.answer_callback_query(call.id)
        return
    
    if call.from_user.id != admin_id:
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    
    bot.answer_callback_query(call.id, "❌ Plan assignment cancelled.")
    
    bot.send_message(admin_id, "❌ Plan assignment cancelled.")
    
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda c: c.data == "export_users")
def export_users_callback(call):
    uid = call.from_user.id
    if not (get_user(uid).get("admin") or uid == OWNER_ID):
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    
    export_data = []
    for user_id_str, user_data in db.items():
        user_id = int(user_id_str)
        try:
            chat = bot.get_chat(user_id)
            username = chat.username or ""
            first_name = chat.first_name or ""
        except Exception:
            username = ""
            first_name = ""
        
        export_data.append({
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "credits": user_data.get("credits", 0),
            "upload_mb": user_data.get("upload_mb", 10),
            "banned": user_data.get("banned", False),
            "admin": user_data.get("admin", False),
            "plan": user_data.get("plan", "free"),
            "total_spent": user_data.get("total_spent", 0),
            "joined_at": user_data.get("joined_at", "")
        })
    
    try:
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8')
        json.dump(export_data, temp_file, indent=2, ensure_ascii=False)
        temp_file.close()
        
        with open(temp_file.name, 'rb') as f:
            bot.send_document(call.message.chat.id, f, caption="📊 Users data export")
        
        os.unlink(temp_file.name)
        bot.answer_callback_query(call.id, "✅ Exported successfully!")
    except Exception as e:
        bot.answer_callback_query(call.id, f"❌ Export failed: {e}")

@bot.callback_query_handler(func=lambda c: c.data == "user_stats")
def user_stats_callback(call):
    uid = call.from_user.id
    if not (get_user(uid).get("admin") or uid == OWNER_ID):
        bot.answer_callback_query(call.id, "Not allowed.", show_alert=True)
        return
    
    stats = get_user_stats()
    
    stats_text = (
        "📊 *Admin Statistics*\n\n"
        f"👥 Total Users: `{stats['total_users']}`\n"
        f"🆓 Free Users: `{stats['free_users']}`\n"
        f"💰 Premium Users: `{stats['premium_users']}`\n"
        f"✅ Active Premium: `{stats['active_premium']}`\n"
        f"💸 Total Revenue: `₹{stats['total_revenue']}`\n\n"
        f"📦 *Plan Distribution:*\n"
        f"• 🆓 Free: `{stats['plan_counts']['free']}`\n"
        f"• 🥈 Basic: `{stats['plan_counts']['basic']}`\n"
        f"• 🥇 Pro: `{stats['plan_counts']['pro']}`\n"
        f"• 👑 Premium: `{stats['plan_counts']['premium']}`\n\n"
        f"📅 Last Updated: `{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}`"
    )
    
    bot.answer_callback_query(call.id)
    bot.send_message(call.message.chat.id, stats_text, parse_mode="Markdown")

@bot.callback_query_handler(func=lambda c: c.data == "close_list")
def close_list_callback(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "Closed")
    except Exception:
        bot.answer_callback_query(call.id, "Already closed")

@bot.callback_query_handler(func=lambda c: c.data == "noop")
def noop_callback(call):
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "show_owner_info")
def show_owner_info_callback(call):
    bot.answer_callback_query(call.id)
    bot.send_message(
        call.message.chat.id,
        "👤 *Owner Information*\n\n"
        "Username: @rdxking1000bot\n"
        "Owner ID: `8485798078`\n\n"
        "Contact the owner for plan purchases and support.",
        parse_mode="Markdown"
    )

# ---------------------------
# Admin helper: show user card
# ---------------------------
def show_admin_user(chat_id: int, target_id: int):
    u = get_user(target_id)
    try:
        chat = bot.get_chat(target_id)
        username = f"@{chat.username}" if chat.username else "No username"
        name = chat.first_name or ""
    except Exception:
        username = "Unknown"
        name = ""
    
    status = "🔴 BANNED" if u["banned"] else "🟢 Active"
    admin = "👑 Admin" if u.get("admin") else "👤 User"
    plan = PLANS[u.get("plan", "free")]["name"]
    
    expiry_msg = ""
    if u.get("plan_expiry"):
        try:
            expiry_date = datetime.fromisoformat(u["plan_expiry"])
            days_left = (expiry_date - datetime.now()).days
            expiry_msg = f"\n⏰ Plan Expiry: {expiry_date.strftime('%d %B %Y')} ({days_left} days left)"
        except Exception:
            pass
    
    msg = (
        f"👤 *User Information*\n\n"
        f"🆔 ID: `{target_id}`\n"
        f"👤 Name: {name}\n"
        f"📱 Username: {username}\n"
        f"📊 Status: {status}\n"
        f"👥 Role: {admin}\n"
        f"📦 Plan: {plan}{expiry_msg}\n"
        f"💰 Credits: `{u['credits']}`\n"
        f"☁️ Upload Limit: `{u['upload_mb']} MB`\n"
        f"🤖 Max Bots: `{u.get('max_bots', 1)}`\n"
        f"💸 Total Spent: `₹{u.get('total_spent', 0)}`\n"
        f"📅 Joined: `{u.get('joined_at', 'Unknown')[:10] if 'joined_at' in u else 'Unknown'}`"
    )
    
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_BAN)
    kb.row(BTN_ADDCRED, BTN_SET_UPLOAD)
    kb.row(BTN_SET_PLAN)
    kb.row(BTN_BACK_ADMIN)
    
    bot.send_message(chat_id, msg, reply_markup=kb, parse_mode="Markdown")

# ---------------------------
# Start polling with auto-restart
# ---------------------------
if __name__ == "__main__":
    print("🚀 Starting Flask server...")
    t = threading.Thread(target=run_flask)
    t.daemon = True
    t.start()

    print("🤖 Starting Telegram bot...")
    
    # Ensure owner is not banned on startup
    owner_unban()
    
    # Start background thread to save running processes periodically
    proc_saver = threading.Thread(target=save_procs_periodically, daemon=True)
    proc_saver.start()
    
    # Start plan expiry checker
    expiry_checker = threading.Thread(target=plan_expiry_checker, daemon=True)
    expiry_checker.start()
    
    # Load and restart previously running processes
    print("🔄 Loading and restarting previously running processes...")
    load_running_procs()
    
    try:
        bot.send_message(OWNER_ID, 
            "✅ Bot started successfully!\n\n"
            "🔄 Auto-restarting previously running projects...\n"
            "⏰ Plan expiry checker started...\n\n"
            "👤 Owner: RDXBIO87\n"
            "🆔 Owner ID: 8336467661"
        )
    except Exception:
        pass
    
    while True:
        try:
            bot.infinity_polling(timeout=30, long_polling_timeout=30)
        except Exception as e:
            print(f"⚠️ Polling error:{e}")
            time.sleep(5)