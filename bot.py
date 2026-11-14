 
import os
import re
import hmac
import hashlib
import time
import asyncio
from urllib.parse import quote

from aiohttp import web
from telethon import TelegramClient, events


# ENVIRONMENT SETTINGS
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
BASE_URL = os.environ["BASE_URL"].rstrip("/")
STREAM_SECRET = os.environ["STREAM_SECRET"].encode()

MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024
CHUNK_SIZE = 512 * 1024
LINK_VALIDITY = 7 * 24 * 60 * 60

bot = TelegramClient("movie_blaster", API_ID, API_HASH)


# SIGNED STREAMING LINK
def create_signature(chat_id, message_id, expiry):
    data = f"{chat_id}:{message_id}:{expiry}".encode()

    return hmac.new(
        STREAM_SECRET,
        data,
        hashlib.sha256
    ).hexdigest()


def verify_signature(chat_id, message_id, expiry, token):
    try:
        expiry = int(expiry)

        if expiry < int(time.time()):
            return False

        expected = create_signature(
            chat_id, message_id, expiry
        )

        return hmac.compare_digest(expected, token)

    except (ValueError, TypeError):
        return False


# RANGE PARSER
def parse_range(header, size):
    if not header:
        return 0, size - 1

    match = re.fullmatch(r"bytes=(\d*)-(\d*)", header.strip())

    if not match:
        raise ValueError("Invalid range")

    first, last = match.groups()

    if not first:
        length = int(last)

        if length <= 0:
            raise ValueError("Invalid range")

        start = max(0, size - length)
        end = size - 1

    else:
        start = int(first)
        end = int(last) if last else size - 1

    if start >= size or end < start:
        raise ValueError("Range not satisfiable")

    return start, min(end, size - 1)


# FILE INFORMATION
def get_file_info(message):
    document = getattr(message, "document", None)

    if not document:
        return None

    file_name = "video"

    for attribute in document.attributes:
        if hasattr(attribute, "file_name"):
            file_name = attribute.file_name
            break

    return {
        "size": document.size,
        "name": file_name,
        "media": message.media
    }


# RECEIVE TELEGRAM FILES
@bot.on(events.NewMessage(incoming=True))
async def receive_movie(event):

    if not event.is_private or not event.media:
        return

    info = get_file_info(event.message)

    if not info:
        return

    if info["size"] > MAX_FILE_SIZE:
        await event.reply("Maximum file size is 2 GB.")
        return

    expiry = int(time.time()) + LINK_VALIDITY

    token = create_signature(
        event.chat_id,
        event.id,
        expiry
    )

    link = (
        f"{BASE_URL}/stream/"
        f"{event.chat_id}/{event.id}"
        f"?expiry={expiry}&token={token}"
    )

    await event.reply(
        "Movie received successfully!\n\n"
        f"File: {info['name']}\n"
        f"Size: {info['size'] / (1024 ** 2):.2f} MB\n\n"
        f"Streaming Link:\n{link}"
    )


# HEALTH CHECK
async def health(request):
    return web.json_response({
        "status": "online",
        "service": "TFI-MOVIES-SERVER"
    })


# STREAMING
async def stream_movie(request):

    try:
        chat_id = int(request.match_info["chat_id"])
        message_id = int(request.match_info["message_id"])

    except ValueError:
        raise web.HTTPBadRequest()

    expiry = request.query.get("expiry", "")
    token = request.query.get("token", "")

    if not verify_signature(
        chat_id, message_id, expiry, token
    ):
        raise web.HTTPForbidden(text="Invalid or expired link")

    # Get Telegram message before starting HTTP response
    try:
        message = await bot.get_messages(
            chat_id,
            ids=message_id
        )

    except Exception:
        raise web.HTTPBadGateway(
            text="Unable to access Telegram file"
        )

    if not message or not message.media:
        raise web.HTTPNotFound(text="File not found")

    info = get_file_info(message)

    if not info:
        raise web.HTTPUnsupportedMediaType()

    size = info["size"]

    if size <= 0 or size > MAX_FILE_SIZE:
        raise web.HTTPRequestEntityTooLarge(
            max_size=MAX_FILE_SIZE,
            actual_size=size
        )

    try:
        start, end = parse_range(
            request.headers.get("Range"),
            size
        )

    except ValueError:
        return web.Response(
            status=416,
            headers={"Content-Range": f"bytes */{size}"}
        )

    length = end - start + 1

    extension = os.path.splitext(
        info["name"]
    )[1].lower()

    content_types = {
        ".mp4": "video/mp4",
        ".mkv": "video/x-matroska",
        ".webm": "video/webm"
    }

    content_type = content_types.get(
        extension,
        "application/octet-stream"
    )

    status = (
        206 if request.headers.get("Range") else 200
    )

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Content-Type": content_type,
        "Content-Disposition": (
            f'inline; filename="{quote(info["name"])}"'
        ),
        "Cache-Control": "private, no-store"
    }

    if status == 206:
        headers["Content-Range"] = (
            f"bytes {start}-{end}/{size}"
        )

    response = web.StreamResponse(
        status=status,
        headers=headers
    )

    await response.prepare(request)

    if request.method == "HEAD":
        await response.write_eof()
        return response

    remaining = length

    try:
        async for chunk in bot.iter_download(
            info["media"],
            offset=start,
            request_size=CHUNK_SIZE,
            chunk_size=CHUNK_SIZE
        ):

            if remaining <= 0:
                break

            chunk = chunk[:remaining]

            await response.write(chunk)

            remaining -= len(chunk)

    except asyncio.CancelledError:
        raise

    except Exception:
        # The response has already started.
        # Log the failure; the client must retry.
        print("Telegram streaming error")
        raise

    if remaining != 0:
        raise ConnectionError("Incomplete Telegram download")

    await response.write_eof()

    return response


# START HTTP SERVER
async def start_server():

    app = web.Application(
        client_max_size=1024 * 1024
    )

    app.router.add_get("/health", health)

    app.router.add_route(
        "*",
        "/stream/{chat_id}/{message_id}",
        stream_movie
    )

    runner = web.AppRunner(app)

    await runner.setup()

    port = int(os.environ.get("PORT", "10000"))

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        port
    )

    await site.start()

    print(f"TFI-MOVIES-SERVER running on port {port}")

    return runner


# MAIN
async def main():

    await bot.start(bot_token=BOT_TOKEN)

    print("Movie Blaster Bot connected")

    runner = await start_server()

    try:
        await bot.run_until_disconnected()

    finally:
        await runner.cleanup()
        await bot.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
