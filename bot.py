import asyncio
import logging
import sqlite3
import random
import string
import time
import os
from typing import Union

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton, 
    InlineKeyboardMarkup, 
    CallbackQuery, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    FSInputFile,
    BufferedInputFile
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest

# --- CONFIGURATION ---
API_TOKEN = "8675364583:AAHTEeOezGCTtPbZ5DGoIIXXiqsO_jwOFQ8"
SUPER_ADMIN_ID = 7582584348
SUPPORT_USER = "@Ronakguptaji"

# Default Channels
DEFAULT_CHANNELS = [
    {"name": "Main Channel 🎟", "id": "@Shein_X_Deals", "link": "https://t.me/Shein_X_Deals"},
    {"name": "Freinds Chats Group", "id": "@Ronakgupta2", "link": "https://t.me/ronakgupta2"},
    {"name": "op Loot Hub", "id": "@oplooters10", "link": "https://t.me/oplooters10"}
]

# --- DATABASE SETUP ---
def init_db():
    conn = sqlite3.connect("haruki_referral.db")
    cur = conn.cursor()
    
    # Users
    cur.execute("""CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY, 
        username TEXT,
        ref_by INTEGER,
        points INTEGER DEFAULT 0,
        join_date TEXT,
        last_msg_id INTEGER
    )""")
    
    # Stock (Updated with 'type')
    cur.execute("""CREATE TABLE IF NOT EXISTS stock (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        type TEXT,
        code TEXT UNIQUE
    )""")
    
    # Channels
    cur.execute("""CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_name TEXT,
        channel_id TEXT,
        invite_link TEXT
    )""")
    
    # Services (The Shein Tiers)
    cur.execute("""CREATE TABLE IF NOT EXISTS services (
        id TEXT PRIMARY KEY,
        name TEXT,
        price INTEGER
    )""")

    # Config
    cur.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
    
    # Admins
    cur.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)")
    cur.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (SUPER_ADMIN_ID,))
    
    # --- AUTO-MIGRATIONS ---
    try: cur.execute("ALTER TABLE stock ADD COLUMN type TEXT"); 
    except: pass
    try: cur.execute("ALTER TABLE users ADD COLUMN last_msg_id INTEGER"); 
    except: pass
    # -----------------------

    # Initialize Services (Shein Tiers)
    default_services = [
        ("S500", "Shein 500 pe 500", 5),
        ("S1000", "Shein 1000 pe 1000", 10),
        ("S2000", "Shein 2000 pe 2000", 20),
        ("S4000", "Shein 4000 pe 4000", 40)
    ]
    for sid, name, price in default_services:
        cur.execute("INSERT OR IGNORE INTO services (id, name, price) VALUES (?, ?, ?)", (sid, name, price))

    # Initialize Config
    cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("referral_reward", "1"))
        
    # Insert Default Channels if empty
    check = cur.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    if check == 0:
        for c in DEFAULT_CHANNELS:
            cur.execute("INSERT INTO channels (channel_name, channel_id, invite_link) VALUES (?, ?, ?)", 
                        (c["name"], str(c["id"]), c["link"]))

    conn.commit()
    conn.close()

# --- DB HELPERS ---
def db_query(query, params=(), fetchone=False, fetchall=False):
    conn = sqlite3.connect("haruki_referral.db")
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        if fetchone: res = cur.fetchone()
        elif fetchall: res = cur.fetchall()
        else: res = None
        conn.commit()
        return res
    except Exception as e:
        logging.error(f"DB Error: {e}")
        return None
    finally:
        conn.close()

def get_config(key):
    res = db_query("SELECT value FROM config WHERE key=?", (key,), fetchone=True)
    return int(res[0]) if res else 0

def set_config(key, value):
    db_query("INSERT INTO config (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=?", (key, value, value))

def is_admin(user_id):
    return db_query("SELECT user_id FROM admins WHERE user_id = ?", (user_id,), fetchone=True) is not None

# --- STATES ---
class BotStates(StatesGroup):
    add_stock_select = State()
    add_stock_input = State()
    broadcast = State()
    add_channel_id = State()
    add_channel_link = State()
    add_channel_name = State()
    add_points_user = State()
    add_points_amount = State()
    change_price_select = State()
    change_price_input = State()
    change_reward = State()

# --- INITIALIZATION ---
logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# --- UI HELPERS ---
def get_divider():
    return "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"

async def delete_old_msg(user_id):
    res = db_query("SELECT last_msg_id FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    if res and res[0]:
        try: await bot.delete_message(chat_id=user_id, message_id=res[0])
        except: pass

async def send_haruki_msg(user_id, text, reply_markup=None):
    await delete_old_msg(user_id)
    try:
        msg = await bot.send_message(chat_id=user_id, text=text, reply_markup=reply_markup, parse_mode="HTML", disable_web_page_preview=True)
        db_query("UPDATE users SET last_msg_id = ? WHERE user_id = ?", (msg.message_id, user_id))
    except: pass

async def check_membership(user_id):
    channels = db_query("SELECT channel_id, invite_link, channel_name FROM channels", fetchall=True)
    missing = []
    for cid, link, name in channels:
        try:
            member = await bot.get_chat_member(chat_id=cid, user_id=user_id)
            if member.status in ['left', 'kicked', 'restricted']:
                missing.append({'name': name, 'link': link})
        except:
            # Assume missing if bot cannot verify (safest option)
            missing.append({'name': name, 'link': link})
    return missing

# --- KEYBOARDS ---
def join_channels_kb(missing):
    builder = InlineKeyboardBuilder()
    for ch in missing:
        builder.row(InlineKeyboardButton(text=f"👉 Join {ch['name']}", url=ch['link']))
    builder.row(InlineKeyboardButton(text="✅ I Have Joined", callback_data="check_sub"))
    return builder.as_markup()

def main_menu_kb():
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="🎁 Redeem Loot"), KeyboardButton(text="🤝 Refer & Earn"))
    builder.row(KeyboardButton(text="👤 Profile"), KeyboardButton(text="📞 Support"))
    return builder.as_markup(resize_keyboard=True)

def admin_kb():
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="➕ Add Stock", callback_data="adm_stock"), InlineKeyboardButton(text="🗑 Clear All Stock", callback_data="adm_clear"))
    builder.row(InlineKeyboardButton(text="🎯 Add Points", callback_data="adm_pts"), InlineKeyboardButton(text="💰 Set Price", callback_data="adm_price"))
    builder.row(InlineKeyboardButton(text="📢 Broadcast", callback_data="adm_bc"), InlineKeyboardButton(text="🔗 Channels", callback_data="adm_ch"))
    builder.row(InlineKeyboardButton(text="🔙 Close", callback_data="adm_close"))
    return builder.as_markup()

def back_kb(cb):
    return InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Back", callback_data=cb)).as_markup()

# --- HANDLERS ---

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject):
    user_id = message.from_user.id
    username = f"@{message.from_user.username}" if message.from_user.username else message.from_user.first_name
    
    # Register if new
    db_query("INSERT OR IGNORE INTO users (user_id, username, points, join_date) VALUES (?, ?, 0, ?)", (user_id, username, time.strftime("%Y-%m-%d")))
    
    # Membership Gate
    missing = await check_membership(user_id)
    if missing:
        await delete_old_msg(user_id)
        msg = await message.answer(
            f"🚫 <b>Access Restricted</b>\n{get_divider()}\nTo access the <b>Haruki Premium Loot</b>, you must join our channels.",
            reply_markup=join_channels_kb(missing),
            parse_mode="HTML"
        )
        db_query("UPDATE users SET last_msg_id = ? WHERE user_id = ?", (msg.message_id, user_id))
        return

    # Referral Logic (Only if verified)
    user_data = db_query("SELECT ref_by FROM users WHERE user_id = ?", (user_id,), fetchone=True)
    if user_data and user_data[0] is None: # Not referred yet
        args = command.args
        if args and args.isdigit() and int(args) != user_id:
            ref_id = int(args)
            if db_query("SELECT 1 FROM users WHERE user_id = ?", (ref_id,), fetchone=True):
                reward = get_config("referral_reward")
                db_query("UPDATE users SET ref_by = ? WHERE user_id = ?", (ref_id, user_id))
                db_query("UPDATE users SET points = points + ? WHERE user_id = ?", (reward, ref_id))
                try: await bot.send_message(ref_id, f"🎉 <b>New Referral!</b>\n+ {reward} Points added.", parse_mode="HTML")
                except: pass

    text = (
        f"👋 <b>Welcome, {message.from_user.first_name}.</b>\n"
        f"{get_divider()}\n"
        "💎 <b>HARUKI LOOT SYSTEM</b> 💎\n"
        "<b>Refer friends. Earn points. Redeem Shein Coupons for FREE.</b>\n\n"
        "<i>Select an option below to begin.</i>"
    )
    await send_haruki_msg(user_id, text, main_menu_kb())

@dp.callback_query(F.data == "check_sub")
async def check_sub_cb(callback: CallbackQuery):
    missing = await check_membership(callback.from_user.id)
    if not missing:
        await callback.message.delete()
        await cmd_start(callback.message, CommandObject(prefix="/", command="start", args=None)) # Reload start
    else:
        await callback.answer("❌ You are still missing channels!", show_alert=True)

@dp.message(F.text == "🤝 Refer & Earn")
async def refer_menu(message: types.Message):
    user_id = message.from_user.id
    bot_info = await bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={user_id}"
    reward = get_config("referral_reward")
    
    text = (
        "🤝 <b>REFERRAL PROGRAM</b>\n"
        f"{get_divider()}\n"
        f"<b>Invite friends and earn points to redeem premium loot.</b>\n\n"
        f"🎁 <b>Reward:</b> {reward} Points / User\n"
        f"🔗 <b>Your Link:</b>\n<code>{link}</code>\n\n"
        f"<i>Tap to copy.</i>"
    )
    kb = InlineKeyboardBuilder().button(text="🚀 Share Link", url=f"https://t.me/share/url?url={link}&text=Join%20Now!")
    await send_haruki_msg(user_id, text, kb.as_markup())

@dp.message(F.text == "👤 Profile")
async def profile_menu(message: types.Message):
    user = db_query("SELECT points FROM users WHERE user_id=?", (message.from_user.id,), fetchone=True)
    pts = user[0] if user else 0
    text = (
        "👤 <b>USER DASHBOARD</b>\n"
        f"{get_divider()}\n"
        f"🆔 <b>ID:</b> <code>{message.from_user.id}</code>\n"
        f"💎 <b>Balance:</b> {pts} Points\n"
        f"{get_divider()}"
    )
    await send_haruki_msg(message.from_user.id, text, main_menu_kb())

@dp.message(F.text == "🎁 Redeem Loot")
async def redeem_menu(message: types.Message):
    user_pts = db_query("SELECT points FROM users WHERE user_id=?", (message.from_user.id,), fetchone=True)[0]
    
    # Get all services (tiers)
    services = db_query("SELECT id, name, price FROM services", fetchall=True)
    
    kb = InlineKeyboardBuilder()
    items_shown = False
    
    text = (
        "🎁 <b>REDEEM SHOP</b>\n"
        f"{get_divider()}\n"
        f"💎 <b>Your Balance:</b> {user_pts} Points\n"
        f"👇 <b>Available Loot:</b>\n"
    )

    for sid, name, price in services:
        # Check stock for specific type
        count = db_query("SELECT COUNT(*) FROM stock WHERE type=?", (sid,), fetchone=True)[0]
        
        # Only show button if stock exists
        if count > 0:
            items_shown = True
            kb.row(InlineKeyboardButton(text=f"🎟 {name} ({price} Pts)", callback_data=f"redeem_{sid}"))
            
    if not items_shown:
        text += "\n🔴 <b>Everything is currently OUT OF STOCK.</b>\n<i>Please check back later!</i>"
    else:
        text += "\n<i>Click an item to redeem instantly.</i>"

    await send_haruki_msg(message.from_user.id, text, kb.as_markup())

@dp.callback_query(F.data.startswith("redeem_"))
async def process_redeem(callback: CallbackQuery):
    user_id = callback.from_user.id
    sid = callback.data.split("_")[1]
    
    service = db_query("SELECT name, price FROM services WHERE id=?", (sid,), fetchone=True)
    if not service: return await callback.answer("Service error.")
    
    name, price = service
    
    # Atomic Transaction
    conn = sqlite3.connect("haruki_referral.db")
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        pts = cur.execute("SELECT points FROM users WHERE user_id=?", (user_id,)).fetchone()[0]
        if pts < price:
            await callback.answer(f"❌ You need {price} points!", show_alert=True)
            return
            
        code_row = cur.execute("SELECT id, code FROM stock WHERE type=? LIMIT 1", (sid,)).fetchone()
        if not code_row:
            await callback.answer("❌ Just went out of stock!", show_alert=True)
            return
            
        # Execute Trade
        cur.execute("DELETE FROM stock WHERE id=?", (code_row[0],))
        cur.execute("UPDATE users SET points = points - ? WHERE user_id=?", (price, user_id))
        conn.commit()
        
        # Success
        code = code_row[1]
        text = (
            "✅ <b>SUCCESSFULLY REDEEMED!</b>\n"
            f"{get_divider()}\n"
            f"📦 <b>Item:</b> {name}\n"
            f"🎟 <b>Code:</b> <code>{code}</code>\n"
            f"{get_divider()}\n"
            "<i>Screenshot this immediately.</i>"
        )
        await send_haruki_msg(user_id, text, main_menu_kb())
        
    except Exception as e:
        conn.rollback()
        await callback.answer("Error processing request.", show_alert=True)
    finally:
        conn.close()

@dp.message(F.text == "📞 Support")
async def support_menu(message: types.Message):
    text = f"📞 <b>SUPPORT</b>\n{get_divider()}\nContact: {SUPPORT_USER}"
    await send_haruki_msg(message.from_user.id, text, main_menu_kb())

# --- ADMIN PANEL ---
@dp.message(Command("panel"))
async def admin_panel(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    await state.clear()
    await send_haruki_msg(message.from_user.id, "🛠 <b>ADMIN TERMINAL</b>", admin_kb())

@dp.callback_query(F.data == "adm_close")
async def admin_close(callback: CallbackQuery):
    await callback.message.delete()

# Admin: Add Stock (Select Tier First)
@dp.callback_query(F.data == "adm_stock")
async def adm_stock_start(callback: CallbackQuery, state: FSMContext):
    services = db_query("SELECT id, name FROM services", fetchall=True)
    kb = InlineKeyboardBuilder()
    for sid, name in services:
        kb.row(InlineKeyboardButton(text=f"➕ {name}", callback_data=f"add_stk_{sid}"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="adm_home"))
    await callback.message.edit_text("📥 <b>Select Category to Add Stock:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("add_stk_"))
async def adm_stock_input(callback: CallbackQuery, state: FSMContext):
    sid = callback.data.split("_")[2]
    await state.update_data(sid=sid)
    await callback.message.edit_text(f"📥 <b>Paste Codes for {sid}:</b>\n(One per line)", reply_markup=back_kb("adm_home"), parse_mode="HTML")
    await state.set_state(BotStates.add_stock)

@dp.message(BotStates.add_stock)
async def adm_stock_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    sid = data['sid']
    codes = [x.strip() for x in message.text.replace(',', '\n').split('\n') if x.strip()]
    
    for c in codes: 
        db_query("INSERT OR IGNORE INTO stock (type, code) VALUES (?, ?)", (sid, c))
        
    await send_haruki_msg(message.from_user.id, f"✅ Added {len(codes)} codes to {sid}.", admin_kb())
    await state.clear()

# Admin: Set Price (Select Tier First)
@dp.callback_query(F.data == "adm_price")
async def adm_price_start(callback: CallbackQuery, state: FSMContext):
    services = db_query("SELECT id, name, price FROM services", fetchall=True)
    kb = InlineKeyboardBuilder()
    for sid, name, price in services:
        kb.row(InlineKeyboardButton(text=f"💰 {name} ({price} Pts)", callback_data=f"set_pr_{sid}"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="adm_home"))
    await callback.message.edit_text("💰 <b>Select Category to Change Price:</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("set_pr_"))
async def adm_price_input(callback: CallbackQuery, state: FSMContext):
    sid = callback.data.split("_")[2]
    await state.update_data(sid=sid)
    await callback.message.edit_text(f"💰 <b>Enter New Point Cost for {sid}:</b>", reply_markup=back_kb("adm_home"), parse_mode="HTML")
    await state.set_state(BotStates.change_price)

@dp.message(BotStates.change_price)
async def adm_price_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        new_price = int(message.text)
        db_query("UPDATE services SET price=? WHERE id=?", (new_price, data['sid']))
        await send_haruki_msg(message.from_user.id, f"✅ Price updated.", admin_kb())
    except:
        await message.answer("Invalid number.")
    await state.clear()

# Admin: Clear Stock
@dp.callback_query(F.data == "adm_clear")
async def adm_clear_stock(callback: CallbackQuery):
    db_query("DELETE FROM stock")
    await callback.answer("🗑 All Stock Cleared", show_alert=True)

# Admin: Add Points
@dp.callback_query(F.data == "adm_pts")
async def adm_pts_start(callback: CallbackQuery, state: FSMContext):
    await callback.message.edit_text("👤 <b>Send User ID to add points:</b>", reply_markup=back_kb("adm_home"))
    await state.set_state(BotStates.add_points_user)

@dp.message(BotStates.add_points_user)
async def adm_pts_uid(message: types.Message, state: FSMContext):
    await state.update_data(target_uid=message.text)
    await message.answer("🔢 <b>Enter Amount to Add:</b>")
    await state.set_state(BotStates.add_points_amount)

@dp.message(BotStates.add_points_amount)
async def adm_pts_save(message: types.Message, state: FSMContext):
    data = await state.get_data()
    try:
        amt = int(message.text)
        db_query("UPDATE users SET points = points + ? WHERE user_id=?", (amt, data['target_uid']))
        await send_haruki_msg(message.from_user.id, f"✅ Added {amt} points to {data['target_uid']}", admin_kb())
        # Notify User
        try: await bot.send_message(data['target_uid'], f"🎁 <b>Admin added {amt} points to your balance!</b>", parse_mode="HTML")
        except: pass
    except: await message.answer("Invalid number.")
    await state.clear()

# Admin: Channels
@dp.callback_query(F.data == "adm_ch")
async def adm_ch_list(callback: CallbackQuery):
    chans = db_query("SELECT id, channel_name FROM channels", fetchall=True)
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="➕ Add Channel", callback_data="add_ch_start"))
    for cid, name in chans:
        kb.row(InlineKeyboardButton(text=f"🗑 Remove {name}", callback_data=f"del_ch_{cid}"))
    kb.row(InlineKeyboardButton(text="🔙 Back", callback_data="adm_home"))
    await callback.message.edit_text("📢 <b>Manage Channels</b>", reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("del_ch_"))
async def adm_del_ch(callback: CallbackQuery):
    cid = callback.data.split("_")[2]
    db_query("DELETE FROM channels WHERE id=?", (c
