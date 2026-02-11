import os
import json
import asyncio
import tempfile
import time
import re
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from pathlib import Path
import logging
import hashlib

import firebase_admin
from firebase_admin import credentials, db
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, 
    InlineKeyboardButton, CallbackQuery,
    InputMediaPhoto, InputMediaVideo,
    InlineQuery, InlineQueryResultArticle,
    InputTextMessageContent
)
from pyrogram.errors import FloodWait, UserNotParticipant, ChatWriteForbidden, MessageNotModified
import yt_dlp
import requests
import aiohttp
from aiohttp import web
from PIL import Image
import io

# ========== আপনার হার্ডকোডেড ক্রেডেনশিয়ালস ==========
BOT_TOKEN = "8258934199:AAGP9aCB7KdI74bZvh2mkpl6cTL7S2aYyfE"
FIREBASE_DB_URL = "https://fir-ca7b5-default-rtdb.firebaseio.com/"
PUBLIC_CHANNEL = "@youtube_SR_200K"
LOG_CHANNEL = "@youtube_SR_200K"
ADMIN_EMAIL = "msagorkhan953@gmail.com"
ADMIN_PIN = "sagor7171SR"
DEFAULT_AD_NETWORK = "gigapub"
DEFAULT_AD_SCRIPT_ID = "5585"

# আপনার টেলিগ্রাম ইউজার আইডি (8258934199)
ADMIN_USER_IDS = [8258934199]

# ========== লগিং সেটআপ ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# গ্লোবাল ভ্যারিয়েবল
broadcast_store = {}
downloader = None
app = None
channel_manager = None
earnings_tracker = None
ad_manager = None

# ========== ফায়ারবেস ইনিশিয়ালাইজেশন ==========
def initialize_firebase():
    """ফায়ারবেস ইনিশিয়ালাইজেশন"""
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate('config.json')
            firebase_admin.initialize_app(cred, {
                'databaseURL': FIREBASE_DB_URL
            })
            logger.info("✅ Firebase initialized successfully")
            return True
        return True
    except Exception as e:
        logger.error(f"❌ Firebase initialization failed: {e}")
        return False

# ========== ডাটাবেস হেল্পারস ==========
def get_db_ref(path: str = '/'):
    """ডাটাবেস রেফারেন্স"""
    try:
        return db.reference(path)
    except:
        return None

def get_user_ref(user_id: int):
    """ইউজার ডাটার রেফারেন্স"""
    return get_db_ref(f'/users/{user_id}')

def get_settings_ref():
    """সেটিংস রেফারেন্স"""
    return get_db_ref('/settings')

def get_videos_ref():
    """ভিডিও ডাটার রেফারেন্স"""
    return get_db_ref('/videos')

def get_earnings_ref():
    """আয়ের ডাটার রেফারেন্স"""
    return get_db_ref('/earnings')

def initialize_database():
    """ডাটাবেস ইনিশিয়ালাইজেশন"""
    try:
        settings_ref = get_settings_ref()
        if settings_ref:
            settings = settings_ref.get() or {}
            
            if not settings:
                default_settings = {
                    'ad_network': DEFAULT_AD_NETWORK,
                    'ad_script_id': DEFAULT_AD_SCRIPT_ID,
                    'ad_link': f'https://{DEFAULT_AD_NETWORK}.com/ad?id={DEFAULT_AD_SCRIPT_ID}',
                    'ad_timer': 10,
                    'ad_status': 'active',
                    
                    'notice_text': '🎬 YouTube Video Gallery - Browse and download videos!',
                    'channel_welcome': 'Welcome to YouTube SR 200K Video Gallery!',
                    'bot_name': 'YouTube Downloader Bot',
                    
                    'earnings_total': 0,
                    'earnings_today': 0,
                    'last_reset': datetime.now().isoformat(),
                    'downloads_total': 0,
                    'users_total': 0,
                    
                    'ad_rotation': 'enabled',
                    'ad_frequency': 1,
                    'ad_providers': {
                        'gigapub': {'active': True, 'rate': 0.01},
                        'adsense': {'active': False, 'rate': 0.02},
                        'propeller': {'active': False, 'rate': 0.015}
                    }
                }
                settings_ref.set(default_settings)
                logger.info("✅ Database initialized with default settings")
    except Exception as e:
        logger.error(f"Database init error: {e}")

# ========== ওয়েব সার্ভার (হেলথ চেক) ==========
async def health_check(request):
    """হেলথ চেক এন্ডপয়েন্ট"""
    return web.Response(text="YouTube Downloader Bot is running", status=200)

async def start_web_server():
    """হেলথ চেকের জন্য ওয়েব সার্ভার স্টার্ট"""
    try:
        app_web = web.Application()
        app_web.router.add_get('/', health_check)
        app_web.router.add_get('/health', health_check)
        app_web.router.add_get('/ping', health_check)
        
        runner = web.AppRunner(app_web)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8000)
        await site.start()
        logger.info("✅ Web server started on port 8000")
        return runner
    except Exception as e:
        logger.error(f"❌ Web server failed: {e}")
        return None

# ========== ইউটিউব ডাউনলোডার ক্লাস ==========
class YouTubeDownloader:
    def __init__(self):
        self.ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
        }
    
    def get_video_info(self, url: str) -> Dict:
        """ভিডিও ইনফো সংগ্রহ"""
        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info
        except Exception as e:
            logger.error(f"Video info error: {e}")
            return None
    
    def get_thumbnail(self, url: str) -> Optional[bytes]:
        """ভিডিও থাম্বনেইল সংগ্রহ"""
        try:
            info = self.get_video_info(url)
            if info and 'thumbnail' in info:
                thumbnail_url = info['thumbnail']
                response = requests.get(thumbnail_url, timeout=10)
                if response.status_code == 200:
                    return response.content
        except Exception as e:
            logger.error(f"Thumbnail error: {e}")
        return None
    
    def download_video(self, url: str, format_id: str, user_id: int) -> Optional[str]:
        """ভিডিও ডাউনলোড"""
        try:
            temp_dir = tempfile.mkdtemp(prefix=f"ytdl_{user_id}_")
            
            ydl_opts = {
                'format': format_id,
                'outtmpl': os.path.join(temp_dir, '%(title).100s.%(ext)s'),
                'quiet': True,
                'no_warnings': True,
                'max_filesize': 2000 * 1024 * 1024,
            }
            
            if 'bestaudio' in format_id:
                ydl_opts.update({
                    'format': 'bestaudio/best',
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '128',
                    }],
                })
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
                
                for file in os.listdir(temp_dir):
                    if file.endswith(('.mp4', '.mkv', '.webm', '.mp3', '.m4a')):
                        return os.path.join(temp_dir, file)
            
            return None
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None

# ========== এড ম্যানেজার ক্লাস ==========
class AdManager:
    def __init__(self):
        self.ad_providers = {
            'gigapub': {
                'name': 'GigaPub',
                'base_url': 'https://gigapub.com/ad?id=',
                'active': True,
                'rate_per_view': 0.01
            },
            'adsense': {
                'name': 'Google AdSense',
                'base_url': 'https://googleads.g.doubleclick.net/pagead/ads?client=ca-pub-',
                'active': False,
                'rate_per_view': 0.02
            },
            'propeller': {
                'name': 'Propeller Ads',
                'base_url': 'https://go.propellerads.com/',
                'active': False,
                'rate_per_view': 0.015
            }
        }
    
    def get_active_ad(self) -> Dict:
        """অ্যাক্টিভ এড প্রোভাইডার রিটার্ন করে"""
        try:
            settings_ref = get_settings_ref()
            if settings_ref:
                settings = settings_ref.get() or {}
                ad_network = settings.get('ad_network', DEFAULT_AD_NETWORK)
                script_id = settings.get('ad_script_id', DEFAULT_AD_SCRIPT_ID)
                
                provider = self.ad_providers.get(ad_network, self.ad_providers['gigapub'])
                
                ad_link = f"{provider['base_url']}{script_id}"
                if ad_network == 'adsense':
                    ad_link = f"{provider['base_url']}{script_id}"
                
                return {
                    'provider': provider['name'],
                    'network': ad_network,
                    'link': ad_link,
                    'script_id': script_id,
                    'rate': provider['rate_per_view'],
                    'active': provider['active']
                }
        except Exception as e:
            logger.error(f"Get active ad error: {e}")
        
        default_provider = self.ad_providers['gigapub']
        return {
            'provider': default_provider['name'],
            'network': 'gigapub',
            'link': f"{default_provider['base_url']}{DEFAULT_AD_SCRIPT_ID}",
            'script_id': DEFAULT_AD_SCRIPT_ID,
            'rate': default_provider['rate_per_view'],
            'active': True
        }
    
    def update_ad_settings(self, network: str, script_id: str, status: str = 'active'):
        """এড সেটিংস আপডেট"""
        try:
            settings_ref = get_settings_ref()
            if settings_ref:
                settings = settings_ref.get() or {}
                
                if network not in self.ad_providers:
                    return False, "Invalid ad network"
                
                settings['ad_network'] = network
                settings['ad_script_id'] = script_id
                settings['ad_status'] = status
                
                provider = self.ad_providers[network]
                if network == 'adsense':
                    settings['ad_link'] = f"{provider['base_url']}{script_id}"
                else:
                    settings['ad_link'] = f"{provider['base_url']}{script_id}"
                
                settings_ref.set(settings)
                return True, "Ad settings updated successfully"
        except Exception as e:
            logger.error(f"Update ad settings error: {e}")
            return False, f"Error: {str(e)}"

# ========== চ্যানেল ম্যানেজার ক্লাস ==========
class ChannelManager:
    def __init__(self, client: Client):
        self.client = client
        self.downloader = YouTubeDownloader()
    
    async def post_video_to_channel(self, url: str, title: str, description: str = ""):
        """চ্যানেলে ভিডিও পোস্ট করুন"""
        try:
            info = self.downloader.get_video_info(url)
            if not info:
                return None
            
            thumbnail = self.downloader.get_thumbnail(url)
            video_id = hashlib.md5(url.encode()).hexdigest()[:10]
            
            post_text = f"🎬 **{title}**\n\n"
            if description:
                post_text += f"{description}\n\n"
            
            duration = info.get('duration', 0)
            if duration:
                hours = duration // 3600
                minutes = (duration % 3600) // 60
                seconds = duration % 60
                if hours > 0:
                    duration_str = f"{hours}:{minutes:02d}:{seconds:02d}"
                else:
                    duration_str = f"{minutes}:{seconds:02d}"
                post_text += f"⏱️ Duration: {duration_str}\n"
            
            view_count = info.get('view_count', 0)
            if view_count:
                post_text += f"👁️ Views: {view_count:,}\n"
            
            post_text += f"\n⬇️ **Download this video:**"
            
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "📥 Download Now", 
                    callback_data=f"channel_dl_{video_id}"
                )
            ]])
            
            if thumbnail:
                message = await self.client.send_photo(
                    chat_id=PUBLIC_CHANNEL,
                    photo=thumbnail,
                    caption=post_text,
                    reply_markup=keyboard
                )
            else:
                message = await self.client.send_message(
                    chat_id=PUBLIC_CHANNEL,
                    text=post_text,
                    reply_markup=keyboard
                )
            
            self._save_video_to_db(video_id, url, title, info, message.message_id)
            
            return message
            
        except Exception as e:
            logger.error(f"Channel post error: {e}")
            return None
    
    def _save_video_to_db(self, video_id: str, url: str, title: str, info: Dict, message_id: int):
        """ভিডিও ডাটাবেসে সেভ"""
        try:
            videos_ref = get_db_ref(f'/videos/{video_id}')
            if videos_ref:
                video_data = {
                    'url': url,
                    'title': title,
                    'video_id': info.get('id', ''),
                    'channel': info.get('uploader', ''),
                    'duration': info.get('duration', 0),
                    'views': info.get('view_count', 0),
                    'message_id': message_id,
                    'posted_at': datetime.now().isoformat(),
                    'downloads': 0,
                    'earnings': 0
                }
                videos_ref.set(video_data)
        except Exception as e:
            logger.error(f"Save video DB error: {e}")
    
    async def update_channel_posts(self):
        """চ্যানেলের সকল পোস্ট আপডেট করুন"""
        try:
            settings_ref = get_settings_ref()
            if not settings_ref:
                return
            
            settings = settings_ref.get() or {}
            ad_link = settings.get('ad_link', f'https://{DEFAULT_AD_NETWORK}.com/ad?id={DEFAULT_AD_SCRIPT_ID}')
            
            async for message in self.client.get_chat_history(PUBLIC_CHANNEL, limit=50):
                if message.caption or message.text:
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("📥 Download Now", callback_data="download_from_channel")
                    ]])
                    
                    try:
                        await message.edit_reply_markup(reply_markup=keyboard)
                        await asyncio.sleep(1)
                    except MessageNotModified:
                        continue
                    except Exception as e:
                        logger.error(f"Update post error: {e}")
                        
        except Exception as e:
            logger.error(f"Update channel posts error: {e}")

# ========== আয় ট্র্যাকার ক্লাস ==========
class EarningsTracker:
    def __init__(self):
        pass
    
    def add_earning(self, amount: float, source: str, user_id: int = None):
        """আয় যোগ করুন"""
        try:
            earnings_ref = get_earnings_ref()
            if not earnings_ref:
                return
            
            daily_ref = get_db_ref(f'/earnings/daily/{datetime.now().strftime("%Y-%m-%d")}')
            daily_data = daily_ref.get() or {}
            daily_total = daily_data.get('total', 0)
            daily_ref.set({
                'total': daily_total + amount,
                'last_updated': datetime.now().isoformat()
            })
            
            total_ref = get_db_ref('/earnings/total')
            total_data = total_ref.get() or {}
            grand_total = total_data.get('total', 0)
            total_ref.set({
                'total': grand_total + amount,
                'last_updated': datetime.now().isoformat()
            })
            
            logger.info(f"Earning added: ${amount} from {source}")
            
        except Exception as e:
            logger.error(f"Add earning error: {e}")
    
    def get_today_earnings(self):
        """আজকের আয়"""
        try:
            daily_ref = get_db_ref(f'/earnings/daily/{datetime.now().strftime("%Y-%m-%d")}')
            daily_data = daily_ref.get() or {}
            return daily_data.get('total', 0)
        except:
            return 0
    
    def get_total_earnings(self):
        """মোট আয়"""
        try:
            total_ref = get_db_ref('/earnings/total')
            total_data = total_ref.get() or {}
            return total_data.get('total', 0)
        except:
            return 0

# ========== বট হ্যান্ডলারস ==========
async def check_subscription(user_id: int) -> bool:
    """সাবস্ক্রিপশন চেক"""
    try:
        member = await app.get_chat_member(PUBLIC_CHANNEL, user_id)
        return member.status in ['member', 'administrator', 'creator']
    except:
        return False

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """স্টার্ট কমান্ড"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    
    logger.info(f"User {user_id} ({user_name}) started bot")
    
    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Join Channel", url=PUBLIC_CHANNEL),
            InlineKeyboardButton("✅ Check Join", callback_data="check_join")
        ]])
        await message.reply_text(
            "⚠️ **Please join our channel first to use this bot!**\n\n"
            "Join our video gallery to access all videos!",
            reply_markup=keyboard
        )
        return
    
    try:
        user_ref = get_user_ref(user_id)
        if user_ref:
            user_data = user_ref.get() or {}
            if not user_data:
                user_ref.set({
                    'name': user_name,
                    'username': message.from_user.username,
                    'joined_date': datetime.now().isoformat(),
                    'last_active': datetime.now().isoformat(),
                    'total_downloads': 0
                })
    except Exception as e:
        logger.error(f"User save error: {e}")
    
    settings = get_settings_ref().get() or {} if get_settings_ref() else {}
    welcome_text = settings.get('channel_welcome', 'Welcome to YouTube Video Gallery!')
    
    text = f"👋 **Welcome {user_name}!**\n\n"
    text += f"{welcome_text}\n\n"
    text += "**How to use:**\n"
    text += "1. Browse videos in our channel\n"
    text += "2. Click 'Download' button\n"
    text += "3. Watch a short ad\n"
    text += "4. Get your video file\n\n"
    text += f"📺 **Channel:** {PUBLIC_CHANNEL}\n"
    text += "👇 **Browse videos now!**"
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📺 Browse Videos", url=PUBLIC_CHANNEL),
        InlineKeyboardButton("🔍 Search Videos", switch_inline_query_current_chat="")
    ]])
    
    await message.reply_text(text, reply_markup=keyboard)

@app.on_message(filters.command("channel") & filters.private)
async def channel_command(client: Client, message: Message):
    """চ্যানেল ম্যানেজমেন্ট কমান্ড"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) < 2:
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Update All Posts", callback_data="update_all_posts"),
            InlineKeyboardButton("📊 Channel Stats", callback_data="channel_stats")
        ], [
            InlineKeyboardButton("📤 Post New Video", callback_data="post_new_video")
        ]])
        
        await message.reply_text(
            "📺 **Channel Management Panel**\n\n"
            "Manage your video gallery channel:",
            reply_markup=keyboard
        )
        return
    
    cmd = message.command[1]
    
    if cmd == "update":
        await message.reply_text("🔄 Updating all channel posts...")
        await channel_manager.update_channel_posts()
        await message.reply_text("✅ All channel posts updated!")
    
    elif cmd == "stats":
        stats = await channel_manager.get_channel_stats()
        text = "📊 **Channel Statistics**\n\n"
        text += f"📺 **Total Videos:** {stats.get('total_videos', 0)}\n"
        text += f"📥 **Total Downloads:** {stats.get('total_downloads', 0)}\n"
        text += f"💰 **Total Earnings:** ${stats.get('total_earnings', 0):.2f}\n"
        await message.reply_text(text)
    
    elif cmd == "post":
        if len(message.command) < 4:
            await message.reply_text("❌ **Usage:** `/channel post <url> <title>`")
            return
        
        url = message.command[2]
        title = ' '.join(message.command[3:])
        
        await message.reply_text(f"📤 Posting video: {title}")
        result = await channel_manager.post_video_to_channel(url, title)
        
        if result:
            await message.reply_text(f"✅ Video posted successfully!\nPost ID: {result.message_id}")
        else:
            await message.reply_text("❌ Failed to post video")

@app.on_callback_query(filters.regex(r'^channel_dl_'))
async def channel_download_callback(client: Client, callback_query: CallbackQuery):
    """চ্যানেল থেকে ডাউনলোড"""
    user_id = callback_query.from_user.id
    video_id = callback_query.data.split('_')[2]
    
    await callback_query.answer("Redirecting to bot...")
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📥 Download Now", url=f"https://t.me/{app.me.username}?start=video_{video_id}")
    ]])
    
    await callback_query.message.reply_text(
        "📥 **Please use our bot to download this video:**",
        reply_markup=keyboard
    )

@app.on_message(filters.command("add") & filters.private)
async def add_video_command(client: Client, message: Message):
    """ভিডিও যোগ করুন"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) < 3:
        await message.reply_text(
            "❌ **Usage:** `/add <youtube_url> <title>`\n\n"
            "**Example:**\n"
            "`/add https://youtu.be/example Best Video 2024`"
        )
        return
    
    url = message.command[1]
    title = ' '.join(message.command[2:])
    
    if not re.search(r'(youtube\.com|youtu\.be)', url):
        await message.reply_text("❌ **Invalid YouTube URL**")
        return
    
    await message.reply_text(f"🔄 Adding video: {title}")
    result = await channel_manager.post_video_to_channel(url, title)
    
    if result:
        earnings_tracker.add_earning(0.10, "video_post", user_id)
        await message.reply_text(
            f"✅ **Video added successfully!**\n\n"
            f"**Title:** {title}\n"
            f"**Channel:** {PUBLIC_CHANNEL}\n"
            f"**Post ID:** {result.message_id}\n\n"
            f"💰 **Earning:** $0.10 added"
        )
    else:
        await message.reply_text("❌ **Failed to add video**")

@app.on_message(filters.command("earnings") & filters.private)
async def earnings_command(client: Client, message: Message):
    """আয়ের তথ্য"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    today_earnings = earnings_tracker.get_today_earnings()
    total_earnings = earnings_tracker.get_total_earnings()
    
    text = "💰 **Earnings Dashboard**\n\n"
    text += f"📅 **Today's Earnings:** ${today_earnings:.2f}\n"
    text += f"💰 **Total Earnings:** ${total_earnings:.2f}\n\n"
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("📊 Detailed Report", callback_data="earnings_detailed")
    ]])
    
    await message.reply_text(text, reply_markup=keyboard)

@app.on_message(filters.command("ads") & filters.private)
async def ads_management(client: Client, message: Message):
    """এড নেটওয়ার্ক ম্যানেজমেন্ট"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) == 1:
        current_ad = ad_manager.get_active_ad()
        
        text = "💰 **Ad Network Management**\n\n"
        text += f"📊 **Current Ad Network:**\n"
        text += f"• Provider: {current_ad['provider']}\n"
        text += f"• Script ID: `{current_ad['script_id']}`\n"
        text += f"• Rate per view: ${current_ad['rate']:.3f}\n"
        text += f"• Link: `{current_ad['link'][:50]}...`\n\n"
        
        text += "**Commands:**\n"
        text += "• `/ads set <network> <script_id>` - Change ad network\n"
        text += "• `/ads test` - Test current ad\n"
        
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔄 Change Ad", callback_data="change_ad"),
            InlineKeyboardButton("🧪 Test Ad", callback_data="test_ad")
        ]])
        
        await message.reply_text(text, reply_markup=keyboard)
    
    elif message.command[1] == "set":
        if len(message.command) < 4:
            await message.reply_text(
                "❌ **Usage:** `/ads set <network> <script_id>`\n\n"
                "**Example:** `/ads set gigapub 5585`"
            )
            return
        
        network = message.command[2].lower()
        script_id = message.command[3]
        
        success, msg = ad_manager.update_ad_settings(network, script_id)
        
        if success:
            current_ad = ad_manager.get_active_ad()
            await message.reply_text(
                f"✅ **Ad Network Updated!**\n\n"
                f"**New Settings:**\n"
                f"• Network: {current_ad['provider']}\n"
                f"• Script ID: `{current_ad['script_id']}`\n"
                f"• Ad Link: `{current_ad['link'][:60]}...`"
            )
            await channel_manager.update_channel_posts()
        else:
            await message.reply_text(f"❌ **Error:** {msg}")
    
    elif message.command[1] == "test":
        current_ad = ad_manager.get_active_ad()
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("🔗 Test Ad Link", url=current_ad['link'])
        ]])
        
        await message.reply_text(
            f"🧪 **Testing Ad Network**\n\n"
            f"**Network:** {current_ad['provider']}\n"
            f"**Script ID:** `{current_ad['script_id']}`\n"
            f"**Link:** `{current_ad['link'][:50]}...`\n\n"
            f"Click the button below to test the ad:",
            reply_markup=keyboard
        )

@app.on_message(filters.command("adconfig") & filters.private)
async def ad_config_command(client: Client, message: Message):
    """এড কনফিগারেশন"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) == 1:
        settings = get_settings_ref().get() or {} if get_settings_ref() else {}
        
        text = "⚙️ **Ad Configuration**\n\n"
        text += f"⏱️ **Ad Timer:** {settings.get('ad_timer', 10)} seconds\n"
        text += f"📈 **Ad Status:** {settings.get('ad_status', 'active')}\n\n"
        
        text += "**Commands:**\n"
        text += "• `/adconfig timer <seconds>` - Change ad timer\n"
        text += "• `/adconfig status <active/inactive>` - Enable/disable ads\n"
        
        await message.reply_text(text)
    
    elif message.command[1] == "timer":
        if len(message.command) < 3:
            await message.reply_text("❌ **Usage:** `/adconfig timer <seconds>`")
            return
        
        try:
            timer = int(message.command[2])
            if timer < 5 or timer > 60:
                await message.reply_text("❌ **Timer must be between 5 and 60 seconds**")
                return
            
            settings_ref = get_settings_ref()
            if settings_ref:
                settings = settings_ref.get() or {}
                settings['ad_timer'] = timer
                settings_ref.set(settings)
                await message.reply_text(f"✅ **Ad timer updated to {timer} seconds**")
                
        except ValueError:
            await message.reply_text("❌ **Invalid timer value**")
    
    elif message.command[1] == "status":
        if len(message.command) < 3:
            await message.reply_text("❌ **Usage:** `/adconfig status <active/inactive>`")
            return
        
        status = message.command[2].lower()
        settings_ref = get_settings_ref()
        if settings_ref:
            settings = settings_ref.get() or {}
            settings['ad_status'] = 'active' if status == 'active' else 'inactive'
            settings_ref.set(settings)
            status_text = "active" if settings['ad_status'] == 'active' else "inactive"
            await message.reply_text(f"✅ **Ad status changed to {status_text}**")

@app.on_message(filters.command("admin") & filters.private)
async def admin_panel(client: Client, message: Message):
    """এডমিন প্যানেল"""
    user_id = message.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) == 1:
        await message.reply_text(
            "🔒 **Admin Panel**\n\n"
            "Please enter your PIN:\n"
            "`/admin <pin>`"
        )
        return
    
    pin = message.command[1]
    if pin != ADMIN_PIN:
        await message.reply_text("❌ **Invalid PIN**")
        return
    
    settings = get_settings_ref().get() or {} if get_settings_ref() else {}
    
    text = "👑 **Admin Panel**\n\n"
    text += "**Admin Commands:**\n"
    text += "• `/channel` - Channel management\n"
    text += "• `/add <url> <title>` - Add new video\n"
    text += "• `/ads` - Ad network management\n"
    text += "• `/adconfig` - Ad configuration\n"
    text += "• `/earnings` - Earnings dashboard\n"
    text += "• `/broadcast <message>` - Broadcast to users\n"
    
    await message.reply_text(text)

@app.on_callback_query(filters.regex(r'^update_all_posts$'))
async def update_all_posts_callback(client: Client, callback_query: CallbackQuery):
    """সকল পোস্ট আপডেট"""
    user_id = callback_query.from_user.id
    
    if user_id not in ADMIN_USER_IDS:
        await callback_query.answer("Access Denied", show_alert=True)
        return
    
    await callback_query.answer("Updating posts...")
    await callback_query.message.edit_text("🔄 Updating all channel posts...")
    await channel_manager.update_channel_posts()
    await callback_query.message.edit_text("✅ All channel posts updated successfully!")

@app.on_callback_query(filters.regex(r'^check_join$'))
async def check_join_callback(client: Client, callback_query: CallbackQuery):
    """জয়েন চেক"""
    user_id = callback_query.from_user.id
    
    if await check_subscription(user_id):
        await callback_query.answer("✅ You're subscribed! Now you can use the bot.", show_alert=True)
        await callback_query.message.delete()
        
        user = callback_query.from_user
        message = type('obj', (object,), {'from_user': user, 'reply_text': callback_query.message.reply_text})
        await start_command(client, message)
    else:
        await callback_query.answer("❌ You haven't joined the channel yet!", show_alert=True)

# ========== লগ রিপোর্টিং ==========
async def send_log_report(report_type: str, data: Dict):
    """লগ চ্যানেলে রিপোর্ট পাঠান"""
    try:
        if report_type == "download":
            text = f"📥 **Download Report**\n\n"
            text += f"👤 User: {data.get('user_name', 'Unknown')}\n"
            text += f"🆔 ID: `{data.get('user_id', '')}`\n"
            text += f"🎬 Video: {data.get('video_title', 'Unknown')}\n"
            text += f"💰 Earning: ${data.get('earning', 0):.2f}\n"
            text += f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
        elif report_type == "user":
            text = f"👤 **New User Report**\n\n"
            text += f"👤 Name: {data.get('name', 'Unknown')}\n"
            text += f"🆔 ID: `{data.get('id', '')}`\n"
            text += f"📅 Joined: {data.get('joined', '')}\n"
            
        elif report_type == "earning":
            text = f"💰 **Earning Report**\n\n"
            text += f"💵 Amount: ${data.get('amount', 0):.2f}\n"
            text += f"📊 Source: {data.get('source', 'Unknown')}\n"
            text += f"📅 Today Total: ${data.get('today_total', 0):.2f}\n"
        
        await app.send_message(LOG_CHANNEL, text)
        
    except Exception as e:
        logger.error(f"Log report error: {e}")

# ========== মেইন ফাংশন ==========
async def main():
    global app, channel_manager, earnings_tracker, ad_manager
    
    logger.info("🚀 Starting YouTube Channel-Bot Ecosystem...")
    
    if not initialize_firebase():
        logger.warning("Running without Firebase...")
    
    initialize_database()
    web_runner = await start_web_server()
    
    app = Client(
        "youtube_SR_bot",
        bot_token=BOT_TOKEN,
        api_id=2040,
        api_hash="8258934199:AAGP9aCB7KdI74bZvh2mkpl6cTL7S2aYyfE"
    )
    
    try:
        await app.start()
        bot_info = await app.get_me()
        
        channel_manager = ChannelManager(app)
        earnings_tracker = EarningsTracker()
        ad_manager = AdManager()
        
        logger.info(f"✅ Bot started: @{bot_info.username}")
        logger.info(f"✅ Channel: {PUBLIC_CHANNEL}")
        logger.info(f"✅ Log channel: {LOG_CHANNEL}")
        
        startup_data = {
            'bot': bot_info.username,
            'channel': PUBLIC_CHANNEL,
            'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'status': 'online'
        }
        await send_log_report("system", startup_data)
        
        logger.info("Updating channel posts...")
        await channel_manager.update_channel_posts()
        
        logger.info("✅ Ecosystem is ready!")
        
        await idle()
        
    except KeyboardInterrupt:
        logger.info("⚠️ Bot stopped by user")
    except Exception as e:
        logger.error(f"❌ Bot error: {e}", exc_info=True)
    finally:
        logger.info("🛑 Stopping ecosystem...")
        try:
            shutdown_data = {
                'status': 'offline',
                'time': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            await send_log_report("system", shutdown_data)
            await app.stop()
            logger.info("✅ Bot stopped")
        except Exception as e:
            logger.error(f"Stop error: {e}")
        
        if web_runner:
            try:
                await web_runner.cleanup()
                logger.info("✅ Web server stopped")
            except Exception as e:
                logger.error(f"Web server stop error: {e}")

# ========== বট রান ==========
if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Bot shutdown complete")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)