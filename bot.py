import telebot
from telebot import types
import sqlite3
import time
import html

# ---------------- CONFIGURATION ---------------- #
API_TOKEN = "8675364583:AAFFkvZoWMPlxDEoJPJ0NNm1VNRU8Hp9K6U"
ADMIN_IDS = [6806787718, 7582584348]
SUPPORT_USER = "@Sheinsupport80"

DEFAULT_CHANNELS = [
    {"name": "join", "id": "@ronakgupta2", "link": "https://t.me/ronakgupta2"},
    {"name": "join", "id": "@devildeal18", "link": "https://t.me/devildeal18"}
]

bot = telebot.TeleBot(API_TOKEN)

# ---------------- DATABASE SETUP ---------------- #
def init_db():
    conn = sqlite3.connect("haruki_referral.db")
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, ref_by INTEGER, points INTEGER DEFAULT 0, join_date TEXT, last_msg_id INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS stock (id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT, code TEXT UNIQUE)")
    cur.execute("CREATE TABLE IF NOT EXISTS channels (id INTEGER PRIMARY KEY AUTOINCREMENT, channel_name TEXT, channel_id TEXT, invite_link TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS services (id TEXT PRIMARY KEY, name TEXT, price INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS admins (user_id INTEGER PRIMARY KEY)")
    
    for admin_id in ADMIN_IDS:
        cur.execute("INSERT OR IGNORE INTO admins (user_id) VALUES (?)", (admin_id,))

    cur.execute("CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)")
    cur.execute("INSERT OR IGNORE INTO config (key, value) VALUES (?, ?)", ("referral_reward", "1"))

    default_services = [
        ("S500", "Shein 500 pe 500", 5),
        ("S1000", "Shein 1000 pe 1000", 10),
        ("S2000", "Shein 2000 pe 2000", 20),
        ("S4000", "Shein 4000 pe 4000", 40),
        ("BB100", "Big Basket 100/100 Chocolate", 5)
    ]
    for sid, name, price in default_services:
        cur.execute("INSERT OR IGNORE INTO services (id,name,price) VALUES (?,?,?)", (sid,name,price))

    cur.execute("DELETE FROM channels")
    for c in DEFAULT_CHANNELS:
        cur.execute("INSERT INTO channels (channel_name, channel_id, invite_link) VALUES (?,?,?)", (c["name"], c["id"], c["link"]))

    conn.commit()
    conn.close()

# ---------------- DB HELPERS ---------------- #
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
        print(f"DB Error: {e}")
        return None
    finally:
        conn.close()

def get_config(key):
    res = db_query("SELECT value FROM config WHERE key=?", (key,), fetchone=True)
    return int(res[0]) if res else 0

def is_admin(user_id):
    return db_query("SELECT user_id FROM admins WHERE user_id=?", (user_id,), fetchone=True) is not None

def get_divider():
    return "<b>━━━━━━━━━━━━━━━━━━━━━━━━━</b>"

# ---------------- KEYBOARDS ---------------- #
def join_channels_kb(missing):
    kb = types.InlineKeyboardMarkup()
    for ch in missing:
        kb.add(types.InlineKeyboardButton(f"👉 Join {ch['name']}", url=ch['link']))
    kb.add(types.InlineKeyboardButton("✅ I Have Joined", callback_data="check_sub"))
    return kb

def main_menu_kb(user_id):
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2, is_persistent=True)
    kb.add(types.KeyboardButton("🎁 Redeem Loot"), types.KeyboardButton("🤝 Refer & Earn"), types.KeyboardButton("👤 Profile"), types.KeyboardButton("📞 Support"))
    if is_admin(user_id):
        kb.add(types.KeyboardButton("🛠 Admin Panel"))
    return kb

def admin_kb():
    kb = types.InlineKeyboardMarkup()
    kb.row(types.InlineKeyboardButton("➕ Add Stock", callback_data="adm_stock"), types.InlineKeyboardButton("🗑 Clear All", callback_data="adm_clear"))
    kb.row(types.InlineKeyboardButton("🎯 Add Points", callback_data="adm_pts"), types.InlineKeyboardButton("💰 Set Price", callback_data="adm_price"))
    kb.row(types.InlineKeyboardButton("📢 Broadcast", callback_data="adm_bc"), types.InlineKeyboardButton("🔗 Channels", callback_data="adm_ch"))
    kb.row(types.InlineKeyboardButton("🔙 Close", callback_data="adm_close"))
    return kb

def back_kb(cb):
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🔙 Back", callback_data=cb))
    return kb

# ---------------- MEMBERSHIP LOGIC ---------------- #
def check_membership(user_id):
    channels = db_query("SELECT channel_id, invite_link, channel_name FROM channels", fetchall=True)
    missing = []
    if channels:
        for cid, link, name in channels:
            try:
                member = bot.get_chat_member(cid, user_id)
                if member.status in ['left', 'kicked', 'restricted']: missing.append({'name': name, 'link': link, 'id': cid})
            except Exception:
                missing.append({'name': name, 'link': link, 'id': cid})
    return missing

def is_subscribed_or_restrict(chat_id, user_id):
    missing = check_membership(user_id)
    if missing:
        text = f"🚫 <b>Access Restricted</b>\n{get_divider()}\nTo access the bot, you must be a member of our channels."
        bot.send_message(chat_id, text, reply_markup=join_channels_kb(missing), parse_mode="HTML")
        return False
    return True

def send_welcome(chat_id, first_name, user_id):
    try:
        safe_name = html.escape(first_name) if first_name else "User"
        text = f"👋 <b>Welcome, {safe_name}.</b>\n{get_divider()}\n💎 <b>HARUKI LOOT SYSTEM</b> 💎\n<b>Refer friends. Earn points. Redeem Shein Coupons for FREE.</b>\n\n<i>Select an option below to begin.</i>"
        bot.send_message(chat_id, text, reply_markup=main_menu_kb(user_id), parse_mode="HTML")
    except Exception as e:
        print(f"Error in send_welcome: {e}")

# ---------------- HANDLERS ---------------- #
@bot.message_handler(commands=["start"])
def start(message):
    try:
        user_id = message.from_user.id
        username = f"@{message.from_user.username}" if message.from_user.username else "No_Username"
        db_query("INSERT OR IGNORE INTO users (user_id, username, points, join_date) VALUES (?,?,0,?)", (user_id, username, time.strftime("%Y-%m-%d")))
        if not is_subscribed_or_restrict(message.chat.id, user_id): return
        user_data = db_query("SELECT ref_by FROM users WHERE user_id = ?", (user_id,), fetchone=True)
        if user_data and user_data[0] is None:
            args = message.text.split()
            if len(args) > 1 and args[1].isdigit() and int(args[1]) != user_id:
                ref_id = int(args[1])
                if db_query("SELECT 1 FROM users WHERE user_id=?", (ref_id,), fetchone=True):
                    reward = get_config("referral_reward")
                    db_query("UPDATE users SET ref_by=? WHERE user_id=?", (ref_id, user_id))
                    db_query("UPDATE users SET points=points+? WHERE user_id=?", (reward, ref_id))
                    try: bot.send_message(ref_id, f"🎉 <b>New Referral!</b>\n+ {reward} Points added.", parse_mode="HTML")
                    except Exception: pass
        send_welcome(message.chat.id, message.from_user.first_name, user_id)
    except Exception as e:
        print(f"Error in START: {e}")

@bot.message_handler(commands=["panel"])
def admin_panel_cmd(message):
    if is_admin(message.from_user.id): bot.send_message(message.from_user.id, "🛠 <b>ADMIN TERMINAL</b>", reply_markup=admin_kb(), parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "🛠 Admin Panel")
def admin_panel_btn(message):
    if is_admin(message.from_user.id): bot.send_message(message.from_user.id, "🛠 <b>ADMIN TERMINAL</b>", reply_markup=admin_kb(), parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "🎁 Redeem Loot")
def redeem_menu(message):
    user_id = message.from_user.id
    if not is_subscribed_or_restrict(message.chat.id, user_id): return 
    user_pts = db_query("SELECT points FROM users WHERE user_id=?", (user_id,), fetchone=True)[0]
    services = db_query("SELECT id, name, price FROM services", fetchall=True)
    kb = types.InlineKeyboardMarkup()
    items_shown = False
    text = f"🎁 <b>REDEEM SHOP</b>\n{get_divider()}\n💎 <b>Your Balance:</b> {user_pts} Points\n👇 <b>Available Loot:</b>\n"
    if services:
        for sid, name, price in services:
            count = db_query("SELECT COUNT(*) FROM stock WHERE type=?", (sid,), fetchone=True)[0]
            if count > 0:
                items_shown = True
                kb.add(types.InlineKeyboardButton(f"🎟 {name} ({price} Pts)", callback_data=f"redeem_{sid}"))
    if not items_shown: text += "\n🔴 <b>Everything is currently OUT OF STOCK.</b>\n<i>Please check back later!</i>"
    else: text += "\n<i>Click an item to redeem instantly.</i>"
    bot.send_message(user_id, text, reply_markup=kb, parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "🤝 Refer & Earn")
def refer_menu(message):
    user_id = message.from_user.id
    if not is_subscribed_or_restrict(message.chat.id, user_id): return 
    bot_info = bot.get_me()
    link = f"https://t.me/{bot_info.username}?start={user_id}"
    reward = get_config("referral_reward")
    text = f"🤝 <b>REFERRAL PROGRAM</b>\n{get_divider()}\n<b>Invite friends and earn points to redeem premium loot.</b>\n\n🎁 <b>Reward:</b> {reward} Points / User\n🔗 <b>Your Link:</b>\n<code>{link}</code>\n\n<i>Tap to copy.</i>"
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("🚀 Share Link", url=f"https://t.me/share/url?url={link}&text=Join%20Now!"))
    bot.send_message(user_id, text, reply_markup=kb, parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "👤 Profile")
def profile_menu(message):
    user_id = message.from_user.id
    if not is_subscribed_or_restrict(message.chat.id, user_id): return 
    user = db_query("SELECT points FROM users WHERE user_id=?", (user_id,), fetchone=True)
    pts = user[0] if user else 0
    text = f"👤 <b>USER DASHBOARD</b>\n{get_divider()}\n🆔 <b>ID:</b> <code>{user_id}</code>\n💎 <b>Balance:</b> {pts} Points\n{get_divider()}"
    bot.send_message(user_id, text, parse_mode="HTML")

@bot.message_handler(func=lambda message: message.text == "📞 Support")
def support_menu(message):
    user_id = message.from_user.id
    if not is_subscribed_or_restrict(message.chat.id, user_id): return 
    text = f"📞 <b>SUPPORT</b>\n{get_divider()}\nContact: {SUPPORT_USER}"
    bot.send_message(message.from_user.id, text, parse_mode="HTML")

# ---------------- ADMIN MULTI-STEP LOGIC ---------------- #
def process_add_stock(message, sid):
    codes = [x.strip() for x in message.text.replace(',', '\n').split('\n') if x.strip()]
    for c in codes: db_query("INSERT OR IGNORE INTO stock (type, code) VALUES (?, ?)", (sid, c))
    bot.send_message(message.chat.id, f"✅ Added {len(codes)} codes to {sid}.", reply_markup=admin_kb())

def process_set_price(message, sid):
    try:
        new_price = int(message.text)
        db_query("UPDATE services SET price=? WHERE id=?", (new_price, sid))
        bot.send_message(message.chat.id, f"✅ Price updated.", reply_markup=admin_kb())
    except Exception: bot.send_message(message.chat.id, "❌ Invalid number. Try again from Panel.")

def process_add_pts_uid(message):
    uid = message.text
    msg = bot.send_message(message.chat.id, "🔢 <b>Enter Amount to Add:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_add_pts_amt, uid)

def process_add_pts_amt(message, uid):
    try:
        amt = int(message.text)
        db_query("UPDATE users SET points = points + ? WHERE user_id=?", (amt, uid))
        bot.send_message(message.chat.id, f"✅ Added {amt} points to {uid}", reply_markup=admin_kb())
        try: bot.send_message(uid, f"🎁 <b>Admin added {amt} points to your balance!</b>", parse_mode="HTML")
        except Exception: pass
    except Exception: bot.send_message(message.chat.id, "❌ Invalid number.")

def process_broadcast(message):
    users = db_query("SELECT user_id FROM users", fetchall=True)
    count = 0
    m = bot.send_message(message.chat.id, "🚀 Sending...")
    for u in users:
        try:
            bot.copy_message(chat_id=u[0], from_chat_id=message.chat.id, message_id=message.message_id)
            count += 1
            time.sleep(0.05)
        except Exception: pass
    bot.edit_message_text(f"✅ Sent to {count} users.", m.chat.id, m.message_id)
    bot.send_message(message.chat.id, "🛠 <b>ADMIN TERMINAL</b>", reply_markup=admin_kb(), parse_mode="HTML")

def process_add_ch_id(message):
    cid = message.text
    msg = bot.send_message(message.chat.id, "2️⃣ <b>Send Channel Name:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_add_ch_name, cid)

def process_add_ch_name(message, cid):
    cname = message.text
    msg = bot.send_message(message.chat.id, "3️⃣ <b>Send Invite Link:</b>", parse_mode="HTML")
    bot.register_next_step_handler(msg, process_add_ch_link, cid, cname)

def process_add_ch_link(message, cid, cname):
    link = message.text
    db_query("INSERT INTO channels (channel_name, channel_id, invite_link) VALUES (?,?,?)", (cname, cid, link))
    bot.send_message(message.chat.id, "✅ Channel Added.", reply_markup=admin_kb())

# ---------------- CALLBACK HANDLERS ---------------- #
@bot.callback_query_handler(func=lambda call: True)
def callback_handler(call):
    user_id = call.from_user.id
    data = call.data

    if data == "check_sub":
        missing = check_membership(user_id)
        if not missing:
            try: bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception: pass
            send_welcome(call.message.chat.id, call.from_user.first_name, user_id)
        else:
            bot.answer_callback_query(call.id, "❌ You are still missing channels!", show_alert=True)

    elif data.startswith("redeem_"):
        if not is_subscribed_or_restrict(call.message.chat.id, user_id): return 
        sid = data.split("_")[1]
        service = db_query("SELECT name, price FROM services WHERE id=?", (sid,), fetchone=True)
        if not service: return bot.answer_callback_query(call.id, "Service error.")
        name, price = service
        conn = sqlite3.connect("haruki_referral.db")
        cur = conn.cursor()
        try:
            cur.execute("BEGIN IMMEDIATE")
            pts = cur.execute("SELECT points FROM users WHERE user_id=?", (user_id,)).fetchone()[0]
            if pts < price:
                bot.answer_callback_query(call.id, f"❌ You need {price} points!", show_alert=True)
                return
            code_row = cur.execute("SELECT id, code FROM stock WHERE type=? LIMIT 1", (sid,)).fetchone()
            if not code_row:
                bot.answer_callback_query(call.id, "❌ Just went out of stock!", show_alert=True)
                return
            cur.execute("DELETE FROM stock WHERE id=?", (code_row[0],))
            cur.execute("UPDATE users SET points = points - ? WHERE user_id=?", (price, user_id))
            conn.commit()
            code = code_row[1]
            text = f"✅ <b>SUCCESSFULLY REDEEMED!</b>\n{get_divider()}\n📦 <b>Item:</b> {name}\n🎟 <b>Code:</b> <code>{code}</code>\n{get_divider()}\n<i>Screenshot this immediately.</i>"
            bot.send_message(user_id, text, parse_mode="HTML")
        except Exception as e:
            conn.rollback()
            bot.answer_callback_query(call.id, "Error processing request.", show_alert=True)
        finally:
            conn.close()

    elif data == "adm_close":
        try: bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception: pass

    elif data == "adm_home":
        bot.clear_step_handler_by_chat_id(call.message.chat.id)
        bot.edit_message_text("🛠 <b>ADMIN TERMINAL</b>", call.message.chat.id, call.message.message_id, reply_markup=admin_kb(), parse_mode="HTML")

    elif data == "adm_clear":
        db_query("DELETE FROM stock")
        bot.answer_callback_query(call.id, "🗑 All Stock Cleared", show_alert=True)
        
    elif data == "adm_stock":
        services = db_query("SELECT id, name FROM services", fetchall=True)
        kb = types.InlineKeyboardMarkup()
        for sid, name in services: kb.add(types.InlineKeyboardButton(f"➕ {name}", callback_data=f"add_stk_{sid}"))
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_home"))
        bot.edit_message_text("📥 <b>Select Category to Add Stock:</b>", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="HTML")

    elif data.startswith("add_stk_"):
        sid = data.split("_")[2]
        msg = bot.edit_message_text(f"📥 <b>Paste Codes for {sid}:</b>\n(One per line)", call.message.chat.id, call.message.message_id, reply_markup=back_kb("adm_home"), parse_mode="HTML")
        bot.register_next_step_handler(msg, process_add_stock, sid)

    elif data == "adm_price":
        services = db_query("SELECT id, name, price FROM services", fetchall=True)
        kb = types.InlineKeyboardMarkup()
        for sid, name, price in services: kb.add(types.InlineKeyboardButton(f"💰 {name} ({price} Pts)", callback_data=f"set_pr_{sid}"))
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_home"))
        bot.edit_message_text("💰 <b>Select Category to Change Price:</b>", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="HTML")

    elif data.startswith("set_pr_"):
        sid = data.split("_")[2]
        msg = bot.edit_message_text(f"💰 <b>Enter New Point Cost for {sid}:</b>", call.message.chat.id, call.message.message_id, reply_markup=back_kb("adm_home"), parse_mode="HTML")
        bot.register_next_step_handler(msg, process_set_price, sid)

    elif data == "adm_pts":
        msg = bot.edit_message_text("👤 <b>Send User ID to add points:</b>", call.message.chat.id, call.message.message_id, reply_markup=back_kb("adm_home"), parse_mode="HTML")
        bot.register_next_step_handler(msg, process_add_pts_uid)

    elif data == "adm_bc":
        msg = bot.edit_message_text("📢 <b>Send Broadcast Message:</b>", call.message.chat.id, call.message.message_id, reply_markup=back_kb("adm_home"), parse_mode="HTML")
        bot.register_next_step_handler(msg, process_broadcast)

    elif data == "adm_ch":
        chans = db_query("SELECT id, channel_name FROM channels", fetchall=True)
        kb = types.InlineKeyboardMarkup()
        kb.add(types.InlineKeyboardButton("➕ Add Channel", callback_data="add_ch_start"))
        if chans:
            for cid, name in chans: kb.add(types.InlineKeyboardButton(f"🗑 Remove {name}", callback_data=f"del_ch_{cid}"))
        kb.add(types.InlineKeyboardButton("🔙 Back", callback_data="adm_home"))
        bot.edit_message_text("📢 <b>Manage Channels</b>", call.message.chat.id, call.message.message_id, reply_markup=kb, parse_mode="HTML")

    elif data.startswith("del_ch_"):
        cid = data.split("_")[2]
        db_query("DELETE FROM channels WHERE id=?", (cid,))
        bot.answer_callback_query(call.id, "Channel Removed", show_alert=True)
        call.data = "adm_ch"
        callback_handler(call) 

    elif data == "add_ch_start":
        msg = bot.edit_message_text("1️⃣ <b>Send Channel ID (e.g. -100...):</b>", call.message.chat.id, call.message.message_id, parse_mode="HTML")
        bot.register_next_step_handler(msg, process_add_ch_id)

# ---------------- INIT ---------------- #
init_db()

# ---------------- RUN BOT ---------------- #
if __name__ == "__main__":
    print("Haruki Bot Termux Fixed Version is Running...")
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            print(f"Polling Error: {e}")
            time.sleep(5)
