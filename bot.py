 import os, re, hmac, hashlib, time, asyncio
from urllib.parse import quote
from aiohttp import web
from telethon import TelegramClient, events

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
BASE_URL = os.environ["BASE_URL"].rstrip("/")
STREAM_SECRET = os.environ["STREAM_SECRET"].encode()

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
CHUNK_SIZE = 512 * 1024
LINK_VALIDITY = 7 * 24 * 60 * 60
ALLOWED_EXTENSIONS = {".mp4", ".mkv", ".webm"}
bot = TelegramClient("play_max_file_bot", API_ID, API_HASH)

def create_signature(chat_id, message_id, expiry):
    data = f"{chat_id}:{message_id}:{expiry}".encode()
    return hmac.new(STREAM_SECRET, data, hashlib.sha256).hexdigest()

def verify_signature(chat_id, message_id, expiry, token):
    try:
        expiry = int(expiry)
        if expiry < int(time.time()):
            return False
        expected = create_signature(chat_id, message_id, expiry)
        return hmac.compare_digest(expected, token)
    except (ValueError, TypeError):
        return False

def parse_range(header, size):
    if not header:
        return 0, size - 1
    match = re.fullmatch(r"bytes=(\\d*)-(\\d*)", header.strip())
    if not match:
        raise ValueError("Invalid range")
    first, last = match.groups()
    if not first:
        length = int(last)
        if length <= 0:
            raise ValueError("Invalid range")
        start, end = max(0, size - length), size - 1
    else:
        start, end = int(first), int(last) if last else size - 1
    if start >= size or end < start:
        raise ValueError("Range not satisfiable")
    return start, min(end, size - 1)

def get_file_info(message):
    document = getattr(message, "document", None)
    if not document:
        return None
    name = "video"
    for attr in document.attributes:
        if getattr(attr, "file_name", None):
            name = attr.file_name
            break
    ext = os.path.splitext(name)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return None
    return {"size": document.size, "name": name, "media": message.media, "ext": ext}

def make_links(chat_id, message_id):
    expiry = int(time.time()) + LINK_VALIDITY
    token = create_signature(chat_id, message_id, expiry)
    base = f"{BASE_URL}/file/{chat_id}/{message_id}"
    query = f"expiry={expiry}&token={token}"
    return f"{base}/stream?{query}", f"{base}/download?{query}"

@bot.on(events.NewMessage(pattern=r"^/(start|help)$"))
async def welcome(event):
    if event.is_private:
        await event.reply(
            "🎬 PLAY MAX FILE BOT\\n\\n"
            "Send an authorized MP4, MKV, or WEBM video.\\n"
            "I will create a Streaming Link and a Download Link.\\n\\n"
            "Maximum size: 2 GB | Link validity: 7 days\\n"
            "Use only files you own or are authorized to share."
        )

@bot.on(events.NewMessage(incoming=True))
async def receive_file(event):
    if not event.is_private or not event.media:
        return
    info = get_file_info(event.message)
    if not info:
        await event.reply("⚠️ Supported formats: MP4, MKV, WEBM.")
        return
    if info["size"] > MAX_FILE_SIZE:
        await event.reply("⚠️ Maximum file size is 2 GB.")
        return
    stream_link, download_link = make_links(event.chat_id, event.id)
    await event.reply(
        "✅ PLAY MAX: File received!\\n\\n"
        f"📁 File: {info['name']}\\n"
        f"📦 Size: {info['size'] / (1024 ** 2):.2f} MB\\n"
        "⏳ Link valid for: 7 days\\n\\n"
        f"▶️ Streaming Link:\\n{stream_link}\\n\\n"
        f"⬇️ Download Link:\\n{download_link}"
    )

async def health(request):
    return web.json_response({"status": "online", "service": "PLAY MAX FILE SERVER",
                              "supported_formats": sorted(ALLOWED_EXTENSIONS)})

async def serve_file(request):
    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])
    except ValueError:
        raise web.HTTPBadRequest(text="Invalid file reference")
    if not verify_signature(chat_id, message_id, request.query.get("expiry", ""),
                            request.query.get("token", "")):
        raise web.HTTPForbidden(text="Invalid or expired link")
    try:
        message = await bot.get_messages(chat_id, ids=message_id)
    except Exception:
        raise web.HTTPBadGateway(text="Unable to access Telegram file")
    if not message or not message.media:
        raise web.HTTPNotFound(text="File not found")
    info = get_file_info(message)
    if not info:
        raise web.HTTPUnsupportedMediaType(text="Unsupported file type")
    size = info["size"]
    if size <= 0 or size > MAX_FILE_SIZE:
        raise web.HTTPRequestEntityTooLarge(max_size=MAX_FILE_SIZE, actual_size=size)
    try:
        start, end = parse_range(request.headers.get("Range"), size)
    except ValueError:
        return web.Response(status=416, headers={"Content-Range": f"bytes */{size}"})
    length = end - start + 1
    mime = {".mp4": "video/mp4", ".mkv": "video/x-matroska",
            ".webm": "video/webm"}.get(info["ext"], "application/octet-stream")
    download = request.path.endswith("/download")
    disposition = "attachment" if download else "inline"
    headers = {
        "Accept-Ranges": "bytes", "Content-Length": str(length),
        "Content-Type": mime,
        "Content-Disposition": f"{disposition}; filename*=UTF-8''{quote(info['name'])}",
        "Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"
    }
    status = 206 if request.headers.get("Range") else 200
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    response = web.StreamResponse(status=status, headers=headers)
    await response.prepare(request)
    if request.method == "HEAD":
        await response.write_eof()
        return response
    remaining = length
    try:
        async for chunk in bot.iter_download(info["media"], offset=start,
                                             request_size=CHUNK_SIZE,
                                             chunk_size=CHUNK_SIZE):
            if remaining <= 0:
                break
            chunk = chunk[:remaining]
            if not chunk:
                break
            await response.write(chunk)
            remaining -= len(chunk)
    except asyncio.CancelledError:
        raise
    except Exception:
        print("PLAY MAX Telegram transfer error")
        raise
    if remaining:
        raise ConnectionError("Incomplete Telegram transfer")
    await response.write_eof()
    return response

async def start_server():
    app = web.Application(client_max_size=1024 * 1024)
    app.router.add_get("/health", health)
    app.router.add_route("*", "/file/{chat_id}/{message_id}/stream", serve_file)
    app.router.add_route("*", "/file/{chat_id}/{message_id}/download", serve_file)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", "10000"))
    await web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"PLAY MAX FILE SERVER running on port {port}")
    return runner

async def main():
    await bot.start(bot_token=BOT_TOKEN)
    print("PLAY MAX File Bot connected")
    runner = await start_server()
    try:
        await bot.run_until_disconnected()
    finally:
        await runner.cleanup()
        await bot.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
