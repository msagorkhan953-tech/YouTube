import os
import json
import asyncio
import tempfile
import time
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path

import firebase_admin
from firebase_admin import credentials, db
from pyrogram import Client, filters, idle
from pyrogram.types import (
    Message, InlineKeyboardMarkup, 
    InlineKeyboardButton, CallbackQuery,
    InputMediaPhoto, InputMediaVideo
)
from pyrogram.errors import FloodWait, UserNotParticipant
import yt_dlp
import requests

# ========== HARDCODED CREDENTIALS ==========
BOT_TOKEN = "8258934199:AAGP9aCB7KdI74bZvh2mkpl6cTL7S2aYyfE"
FIREBASE_DB_URL = "https://fir-ca7b5-default-rtdb.firebaseio.com/"
PUBLIC_CHANNEL = "@youtube_SR_200K"
LOG_CHANNEL = "@youtube_SR_200K"
ADMIN_EMAIL = "msagorkhan953@gmail.com"
ADMIN_PIN = "sagor7171SR"
AD_SCRIPT_ID = "5585"

# Initialize Firebase
cred = credentials.Certificate('config.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': FIREBASE_DB_URL
})

# Initialize Pyrogram Client
app = Client(
    "youtube_downloader_bot",
    bot_token=BOT_TOKEN,
    api_id=2040,
    api_hash="b18441a1ff607e10a989891a5462e627"
)

# ========== DATABASE HELPERS ==========
def get_user_ref(user_id: int):
    """Get reference to user data"""
    return db.reference(f'/users/{user_id}')

def get_settings_ref():
    """Get reference to settings"""
    return db.reference('/settings')

def initialize_database():
    """Initialize database with default settings"""
    settings_ref = get_settings_ref()
    current_settings = settings_ref.get()
    
    if not current_settings:
        default_settings = {
            'ad_link': f'https://gigapub.com/ad?id={AD_SCRIPT_ID}',
            'ad_image': 'https://via.placeholder.com/600x400/FF0000/FFFFFF?text=Watch+Ad+To+Unlock',
            'ad_timer': 10,
            'notice_text': 'Welcome to YouTube Downloader Bot!',
            'high_quality_enabled': True,
            'admin_api_key': 'default_key_12345'
        }
        settings_ref.set(default_settings)
        print("Database initialized with default settings")

# ========== YOUTUBE DOWNLOADER ==========
class YouTubeDownloader:
    def __init__(self):
        self.ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': True,
        }
    
    def get_video_info(self, url: str) -> Dict:
        """Get video information"""
        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                return info
        except Exception as e:
            print(f"Error getting video info: {e}")
            return None
    
    def get_available_formats(self, url: str) -> List[Dict]:
        """Get available formats for a video"""
        try:
            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'listformats': True,
            }
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                formats = []
                
                if 'formats' in info:
                    for fmt in info['formats']:
                        if fmt.get('vcodec') != 'none':  # Video formats
                            quality = self._parse_quality(fmt)
                            if quality:
                                formats.append({
                                    'quality': quality,
                                    'format_id': fmt['format_id'],
                                    'filesize': fmt.get('filesize', 0),
                                    'ext': fmt.get('ext', 'mp4')
                                })
                
                # Add audio format
                formats.append({
                    'quality': 'MP3 (128kbps)',
                    'format_id': 'bestaudio',
                    'filesize': 0,
                    'ext': 'mp3'
                })
                
                return sorted(formats, key=lambda x: self._quality_sort_key(x['quality']))
        except Exception as e:
            print(f"Error getting formats: {e}")
            return []
    
    def _parse_quality(self, fmt: Dict) -> Optional[str]:
        """Parse quality from format info"""
        height = fmt.get('height', 0)
        if height:
            return f"{height}p"
        return None
    
    def _quality_sort_key(self, quality: str) -> int:
        """Sort key for quality strings"""
        if quality == 'MP3 (128kbps)':
            return 0
        try:
            return int(quality.replace('p', ''))
        except:
            return 999
    
    def download_video(self, url: str, format_id: str, user_id: int) -> Optional[str]:
        """Download video in specified format"""
        try:
            # Create temp directory for user
            temp_dir = tempfile.mkdtemp(prefix=f"ytdl_{user_id}_")
            
            ydl_opts = {
                'format': format_id,
                'outtmpl': os.path.join(temp_dir, '%(title)s.%(ext)s'),
                'quiet': False,
                'no_warnings': True,
                'max_filesize': 2000 * 1024 * 1024,  # 2GB limit
            }
            
            if format_id == 'bestaudio':
                ydl_opts.update({
                    'format': 'bestaudio/best',
                    'postprocessors': [{
                        'key': 'FFmpegExtractAudio',
                        'preferredcodec': 'mp3',
                        'preferredquality': '128',
                    }],
                })
            
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.download([url])
                
                # Find downloaded file
                for file in os.listdir(temp_dir):
                    if file.endswith(('.mp4', '.mkv', '.webm', '.mp3')):
                        return os.path.join(temp_dir, file)
            
            return None
        except Exception as e:
            print(f"Download error: {e}")
            return None

# ========== BOT HANDLERS ==========
downloader = YouTubeDownloader()

async def check_subscription(user_id: int) -> bool:
    """Check if user is subscribed to the channel"""
    try:
        await app.get_chat_member(PUBLIC_CHANNEL, user_id)
        return True
    except UserNotParticipant:
        return False
    except Exception as e:
        print(f"Subscription check error: {e}")
        return False

@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    """Handle /start command"""
    user_id = message.from_user.id
    user_name = message.from_user.first_name
    
    # Check subscription
    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Join Channel", url=PUBLIC_CHANNEL),
            InlineKeyboardButton("✅ Check Join", callback_data="check_join")
        ]])
        await message.reply_text(
            "⚠️ **Please join our channel first to use this bot!**\n\n"
            "Join the channel below and then click 'Check Join'",
            reply_markup=keyboard
        )
        return
    
    # Save user to database
    user_ref = get_user_ref(user_id)
    if not user_ref.get():
        user_ref.set({
            'name': user_name,
            'joined_date': datetime.now().isoformat(),
            'last_activity': datetime.now().isoformat(),
            'total_downloads': 0
        })
    
    # Show notice if set
    settings = get_settings_ref().get()
    notice = settings.get('notice_text', '')
    
    text = f"👋 **Welcome {user_name}!**\n\n"
    if notice:
        text += f"📢 **Notice:** {notice}\n\n"
    text += "📥 **Send me a YouTube link to download videos!**\n\n"
    text += "**Supported:**\n"
    text += "• YouTube Videos\n"
    text += "• YouTube Shorts\n"
    text += "• YouTube Playlists (first 10 videos)\n"
    text += "• Multiple quality options\n\n"
    text += "**Just paste a YouTube URL!**"
    
    await message.reply_text(text)

@app.on_message(filters.text & filters.regex(r'(youtube\.com|youtu\.be)'))
async def handle_youtube_link(client: Client, message: Message):
    """Handle YouTube links"""
    user_id = message.from_user.id
    
    # Check subscription
    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 Join Channel", url=PUBLIC_CHANNEL),
            InlineKeyboardButton("✅ Check Join", callback_data="check_join")
        ]])
        await message.reply_text(
            "⚠️ **Please join our channel first to use this bot!**",
            reply_markup=keyboard
        )
        return
    
    # Show processing message
    processing_msg = await message.reply_text("🔍 **Fetching video information...**")
    
    # Get video info
    url = message.text.strip()
    info = downloader.get_video_info(url)
    
    if not info:
        await processing_msg.edit_text("❌ **Could not fetch video information. Please check the URL.**")
        return
    
    # Get available formats
    formats = downloader.get_available_formats(url)
    
    if not formats:
        await processing_msg.edit_text("❌ **No downloadable formats found.**")
        return
    
    # Check high quality setting
    settings = get_settings_ref().get()
    if not settings.get('high_quality_enabled', True):
        # Filter out high quality formats
        formats = [f for f in formats if not f['quality'].endswith(('720p', '1080p', '1440p', '2160p'))]
    
    # Prepare quality buttons
    buttons = []
    row = []
    
    for fmt in formats[:10]:  # Limit to 10 formats
        quality = fmt['quality']
        size_mb = f" ({fmt['filesize'] // (1024*1024)}MB)" if fmt['filesize'] > 0 else ""
        
        row.append(InlineKeyboardButton(
            f"{quality}{size_mb}",
            callback_data=f"format_{fmt['format_id']}_{user_id}"
        ))
        
        if len(row) == 2:
            buttons.append(row)
            row = []
    
    if row:
        buttons.append(row)
    
    # Add cancel button
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel_download")])
    
    keyboard = InlineKeyboardMarkup(buttons)
    
    # Prepare video info text
    title = info.get('title', 'Unknown Title')[:100]
    duration = info.get('duration', 0)
    duration_str = f"{duration // 60}:{duration % 60:02d}" if duration else "Unknown"
    
    text = f"🎬 **{title}**\n\n"
    text += f"⏱️ Duration: {duration_str}\n"
    text += f"👁️ Views: {info.get('view_count', 'N/A'):,}\n\n"
    text += "⬇️ **Select download quality:**"
    
    # Update message with formats
    await processing_msg.edit_text(text, reply_markup=keyboard)
    
    # Save URL for later use
    user_ref = get_user_ref(user_id)
    user_data = user_ref.get() or {}
    user_data['last_url'] = url
    user_ref.set(user_data)

@app.on_callback_query(filters.regex(r'^format_'))
async def handle_format_selection(client: Client, callback_query: CallbackQuery):
    """Handle format selection"""
    user_id = callback_query.from_user.id
    data_parts = callback_query.data.split('_')
    
    if len(data_parts) < 3:
        await callback_query.answer("Invalid selection", show_alert=True)
        return
    
    format_id = data_parts[1]
    target_user_id = int(data_parts[2])
    
    # Verify user
    if user_id != target_user_id:
        await callback_query.answer("This is not for you!", show_alert=True)
        return
    
    await callback_query.answer()
    
    # Get user's last URL
    user_ref = get_user_ref(user_id)
    user_data = user_ref.get() or {}
    last_url = user_data.get('last_url')
    
    if not last_url:
        await callback_query.message.edit_text("❌ **No video URL found. Please send a new link.**")
        return
    
    # Show ad button
    settings = get_settings_ref().get()
    ad_link = settings.get('ad_link', f'https://gigapub.com/ad?id={AD_SCRIPT_ID}')
    ad_timer = settings.get('ad_timer', 10)
    
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🔓 Unlock Video (Watch Ad)", 
            url=ad_link
        ),
        InlineKeyboardButton(
            "✅ I Watched Ad", 
            callback_data=f"verify_ad_{format_id}_{user_id}"
        )
    ], [
        InlineKeyboardButton("❌ Cancel", callback_data="cancel_download")
    ]])
    
    await callback_query.message.edit_text(
        f"⚠️ **Please watch an ad to unlock the download!**\n\n"
        f"1. Click 'Unlock Video' button\n"
        f"2. Watch the ad for {ad_timer} seconds\n"
        f"3. Click 'I Watched Ad' button\n\n"
        f"**This helps us keep the bot running!** 🚀",
        reply_markup=keyboard
    )

@app.on_callback_query(filters.regex(r'^verify_ad_'))
async def handle_ad_verification(client: Client, callback_query: CallbackQuery):
    """Handle ad verification"""
    user_id = callback_query.from_user.id
    data_parts = callback_query.data.split('_')
    
    if len(data_parts) < 4:
        await callback_query.answer("Invalid request", show_alert=True)
        return
    
    format_id = data_parts[2]
    target_user_id = int(data_parts[3])
    
    # Verify user
    if user_id != target_user_id:
        await callback_query.answer("This is not for you!", show_alert=True)
        return
    
    await callback_query.answer("⏳ Starting download...")
    
    # Get user's last URL
    user_ref = get_user_ref(user_id)
    user_data = user_ref.get() or {}
    last_url = user_data.get('last_url')
    
    if not last_url:
        await callback_query.message.edit_text("❌ **No video URL found.**")
        return
    
    # Update download count
    user_data['total_downloads'] = user_data.get('total_downloads', 0) + 1
    user_ref.set(user_data)
    
    # Start download
    download_msg = await callback_query.message.edit_text("⬇️ **Downloading video... This may take a moment.**")
    
    # Download video
    file_path = downloader.download_video(last_url, format_id, user_id)
    
    if not file_path or not os.path.exists(file_path):
        await download_msg.edit_text("❌ **Download failed. Please try again.**")
        return
    
    # Get file info
    file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
    file_ext = os.path.splitext(file_path)[1].lower()
    
    # Check file size limit (Telegram limit: 2GB for bots)
    if file_size > 2000:
        await download_msg.edit_text(f"❌ **File too large ({file_size:.1f}MB). Telegram limit is 2000MB.**")
        os.remove(file_path)
        return
    
    # Send file
    try:
        if file_ext == '.mp3':
            await client.send_audio(
                chat_id=user_id,
                audio=file_path,
                caption=f"🎵 **Audio Downloaded Successfully!**\n\n"
                       f"📁 Size: {file_size:.1f}MB\n"
                       f"✅ Format: MP3\n\n"
                       f"**Enjoy! 🎧**"
            )
        else:
            await client.send_video(
                chat_id=user_id,
                video=file_path,
                caption=f"🎬 **Video Downloaded Successfully!**\n\n"
                       f"📁 Size: {file_size:.1f}MB\n"
                       f"✅ Format: {file_ext.upper()}\n\n"
                       f"**Enjoy! 🎥**"
            )
        
        await download_msg.edit_text("✅ **Download complete! Check your messages.**")
        
        # Log to channel
        try:
            settings = get_settings_ref().get()
            log_channel = settings.get('log_channel', LOG_CHANNEL)
            
            log_text = f"📥 **Download Log**\n\n"
            log_text += f"👤 User: {callback_query.from_user.mention}\n"
            log_text += f"🆔 ID: `{user_id}`\n"
            log_text += f"📁 Format: {format_id}\n"
            log_text += f"💾 Size: {file_size:.1f}MB\n"
            log_text += f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            await client.send_message(log_channel, log_text)
        except Exception as e:
            print(f"Log error: {e}")
        
    except Exception as e:
        await download_msg.edit_text(f"❌ **Error sending file: {str(e)[:100]}**")
    finally:
        # Cleanup
        if os.path.exists(file_path):
            os.remove(file_path)
        temp_dir = os.path.dirname(file_path)
        if os.path.exists(temp_dir):
            try:
                os.rmdir(temp_dir)
            except:
                pass

@app.on_callback_query(filters.regex(r'^check_join$'))
async def check_join_callback(client: Client, callback_query: CallbackQuery):
    """Handle join check"""
    user_id = callback_query.from_user.id
    
    if await check_subscription(user_id):
        await callback_query.answer("✅ You're subscribed! Now you can use the bot.", show_alert=True)
        await callback_query.message.delete()
        
        # Show start message
        await start_command(client, callback_query.message)
    else:
        await callback_query.answer("❌ You haven't joined the channel yet!", show_alert=True)

@app.on_callback_query(filters.regex(r'^cancel_download$'))
async def cancel_download(client: Client, callback_query: CallbackQuery):
    """Handle download cancellation"""
    await callback_query.answer("Download cancelled")
    await callback_query.message.delete()

# ========== ADMIN COMMANDS ==========
@app.on_message(filters.command("admin") & filters.private)
async def admin_panel(client: Client, message: Message):
    """Admin panel access"""
    user_id = message.from_user.id
    
    # For now, allow only the hardcoded admin
    if message.from_user.username != "sagorkhan7171":  # Replace with actual admin username
        await message.reply_text("❌ **Access Denied**")
        return
    
    # Ask for PIN
    if len(message.command) == 1:
        await message.reply_text("🔒 **Admin Panel**\n\nPlease enter your PIN:")
        return
    
    pin = message.command[1]
    if pin != ADMIN_PIN:
        await message.reply_text("❌ **Invalid PIN**")
        return
    
    # Show admin panel
    settings = get_settings_ref().get()
    
    text = "👑 **Admin Panel**\n\n"
    text += "**Current Settings:**\n"
    text += f"• Ad Link: `{settings.get('ad_link', 'Not set')}`\n"
    text += f"• Ad Timer: {settings.get('ad_timer', 10)} seconds\n"
    text += f"• Notice: {settings.get('notice_text', 'Not set')[:50]}...\n"
    text += f"• HQ Downloads: {'✅ Enabled' if settings.get('high_quality_enabled', True) else '❌ Disabled'}\n\n"
    
    text += "**Commands:**\n"
    text += "• `/admin_set ad_link <URL>` - Set ad URL\n"
    text += "• `/admin_set ad_timer <seconds>` - Set ad timer\n"
    text += "• `/admin_set notice <text>` - Set notice text\n"
    text += "• `/admin_set hq <on/off>` - Toggle HQ downloads\n"
    text += "• `/broadcast <message>` - Broadcast to all users\n"
    text += "• `/stats` - Show bot statistics\n"
    
    await message.reply_text(text)

@app.on_message(filters.command("admin_set") & filters.private)
async def admin_set(client: Client, message: Message):
    """Admin settings"""
    user_id = message.from_user.id
    
    # Check admin
    if message.from_user.username != "sagorkhan7171":
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) < 3:
        await message.reply_text("❌ **Usage:** `/admin_set <key> <value>`")
        return
    
    key = message.command[1]
    value = ' '.join(message.command[2:])
    
    settings_ref = get_settings_ref()
    settings = settings_ref.get() or {}
    
    if key == 'ad_link':
        settings['ad_link'] = value
        await message.reply_text(f"✅ **Ad link updated:**\n`{value}`")
    
    elif key == 'ad_timer':
        try:
            timer = int(value)
            settings['ad_timer'] = timer
            await message.reply_text(f"✅ **Ad timer updated to {timer} seconds**")
        except ValueError:
            await message.reply_text("❌ **Invalid timer value**")
    
    elif key == 'notice':
        settings['notice_text'] = value
        await message.reply_text(f"✅ **Notice updated:**\n{value}")
    
    elif key == 'hq':
        if value.lower() in ['on', 'true', 'yes', '1']:
            settings['high_quality_enabled'] = True
            await message.reply_text("✅ **High quality downloads enabled**")
        else:
            settings['high_quality_enabled'] = False
            await message.reply_text("❌ **High quality downloads disabled**")
    
    else:
        await message.reply_text("❌ **Invalid setting key**")
        return
    
    # Save settings
    settings_ref.set(settings)

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_message(client: Client, message: Message):
    """Broadcast to all users"""
    user_id = message.from_user.id
    
    # Check admin
    if message.from_user.username != "sagorkhan7171":
        await message.reply_text("❌ **Access Denied**")
        return
    
    if len(message.command) < 2:
        await message.reply_text("❌ **Usage:** `/broadcast <message>`")
        return
    
    broadcast_text = ' '.join(message.command[1:])
    
    # Get all users
    users_ref = db.reference('/users')
    users = users_ref.get()
    
    if not users:
        await message.reply_text("❌ **No users in database**")
        return
    
    # Confirm broadcast
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Yes", callback_data=f"confirm_broadcast_{user_id}"),
        InlineKeyboardButton("❌ No", callback_data="cancel_broadcast")
    ]])
    
    await message.reply_text(
        f"⚠️ **Confirm Broadcast**\n\n"
        f"**Message:** {broadcast_text[:100]}...\n\n"
        f"**Total Users:** {len(users)}\n\n"
        f"Do you want to proceed?",
        reply_markup=keyboard
    )
    
    # Store broadcast data temporarily
    user_ref = get_user_ref(user_id)
    user_data = user_ref.get() or {}
    user_data['pending_broadcast'] = broadcast_text
    user_ref.set(user_data)

@app.on_callback_query(filters.regex(r'^confirm_broadcast_'))
async def confirm_broadcast(client: Client, callback_query: CallbackQuery):
    """Confirm and send broadcast"""
    user_id = int(callback_query.data.split('_')[2])
    
    if callback_query.from_user.id != user_id:
        await callback_query.answer("Not for you!", show_alert=True)
        return
    
    await callback_query.answer("Starting broadcast...")
    
    # Get broadcast message
    user_ref = get_user_ref(user_id)
    user_data = user_ref.get() or {}
    broadcast_text = user_data.get('pending_broadcast', '')
    
    if not broadcast_text:
        await callback_query.message.edit_text("❌ **No broadcast message found**")
        return
    
    # Get all users
    users_ref = db.reference('/users')
    users = users_ref.get()
    
    success = 0
    failed = 0
    
    progress_msg = await callback_query.message.edit_text(
        f"📢 **Broadcasting...**\n\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"📊 Total: {len(users)}"
    )
    
    # Send to all users
    for uid_str in users.keys():
        try:
            uid = int(uid_str)
            await client.send_message(
                chat_id=uid,
                text=broadcast_text
            )
            success += 1
            
            # Update progress every 10 users
            if success % 10 == 0:
                await progress_msg.edit_text(
                    f"📢 **Broadcasting...**\n\n"
                    f"✅ Success: {success}\n"
                    f"❌ Failed: {failed}\n"
                    f"📊 Total: {len(users)}\n"
                    f"📈 Progress: {(success+failed)/len(users)*100:.1f}%"
                )
            
            await asyncio.sleep(0.1)  # Avoid flood
            
        except Exception as e:
            failed += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            continue
    
    # Clear pending broadcast
    if 'pending_broadcast' in user_data:
        del user_data['pending_broadcast']
        user_ref.set(user_data)
    
    await progress_msg.edit_text(
        f"✅ **Broadcast Complete**\n\n"
        f"✅ Success: {success}\n"
        f"❌ Failed: {failed}\n"
        f"📊 Total: {len(users)}\n"
        f"📈 Success Rate: {success/len(users)*100:.1f}%"
    )

@app.on_callback_query(filters.regex(r'^cancel_broadcast$'))
async def cancel_broadcast(client: Client, callback_query: CallbackQuery):
    """Cancel broadcast"""
    await callback_query.answer("Broadcast cancelled")
    await callback_query.message.delete()

@app.on_message(filters.command("stats") & filters.private)
async def show_stats(client: Client, message: Message):
    """Show bot statistics"""
    user_id = message.from_user.id
    
    # Check admin
    if message.from_user.username != "sagorkhan7171":
        await message.reply_text("❌ **Access Denied**")
        return
    
    # Get all users
    users_ref = db.reference('/users')
    users = users_ref.get() or {}
    
    # Calculate stats
    total_users = len(users)
    total_downloads = sum(user.get('total_downloads', 0) for user in users.values())
    
    # Recent users (last 7 days)
    recent_users = 0
    week_ago = datetime.now().timestamp() - 7*24*60*60
    
    for user in users.values():
        joined_date = user.get('joined_date', '')
        if joined_date:
            try:
                joined_dt = datetime.fromisoformat(joined_date)
                if joined_dt.timestamp() > week_ago:
                    recent_users += 1
            except:
                pass
    
    text = "📊 **Bot Statistics**\n\n"
    text += f"👥 **Total Users:** {total_users}\n"
    text += f"📈 **Recent Users (7 days):** {recent_users}\n"
    text += f"📥 **Total Downloads:** {total_downloads}\n"
    text += f"📅 **Bot Started:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    # Top users by downloads
    top_users = sorted(
        [(uid, data.get('total_downloads', 0)) for uid, data in users.items()],
        key=lambda x: x[1],
        reverse=True
    )[:5]
    
    if top_users:
        text += "🏆 **Top Users by Downloads:**\n"
        for i, (uid, downloads) in enumerate(top_users, 1):
            try:
                user = await client.get_users(int(uid))
                name = user.first_name or f"User {uid}"
            except:
                name = f"User {uid}"
            text += f"{i}. {name}: {downloads} downloads\n"
    
    await message.reply_text(text)

# ========== MAIN FUNCTION ==========
async def main():
    """Main function"""
    print("🚀 Starting YouTube Downloader Bot...")
    
    # Initialize database
    initialize_database()
    print("✅ Database initialized")
    
    # Start the bot
    await app.start()
    print(f"✅ Bot started: @{(await app.get_me()).username}")
    
    # Send startup message to log channel
    try:
        await app.send_message(
            LOG_CHANNEL,
            f"🤖 **Bot Started Successfully!**\n\n"
            f"⏰ Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"👤 Admin: {ADMIN_EMAIL}\n"
            f"🚀 Ready to download YouTube videos!"
        )
    except Exception as e:
        print(f"Warning: Could not send startup message: {e}")
    
    # Keep running
    await idle()
    
    # Stop the bot
    await app.stop()
    print("✅ Bot stopped")

if __name__ == "__main__":
    # Create config file if it doesn't exist
    if not os.path.exists('config.json'):
        with open('config.json', 'w') as f:
            json.dump({
                "type": "service_account",
                "project_id": "fir-ca7b5",
                "private_key_id": "c4a0f7e8d9c6b5a4f3e2d1c0b9a8f7e6",
                "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDNh8UfWgYqLw4d\nkI6lS8rK9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV7t\nJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXv\nT2sD3aK7pL9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV\n7tJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXv\nT2sD3aK7pL9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV\n7tJ8yZxMvQ1cFbYgHnRqXvT2sD3aK7pL9mNpP3q2sV7tJ8yZxMvQ1cFbYgHnRqXv\n-----END PRIVATE KEY-----\n",
                "client_email": "firebase-adminsdk-f0a1b@fir-ca7b5.iam.gserviceaccount.com",
                "client_id": "112233445566778899000",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-f0a1b%40fir-ca7b5.iam.gserviceaccount.com"
            }, f, indent=2)
        print("✅ Created config.json")
    
    # Run the bot
    asyncio.run(main())