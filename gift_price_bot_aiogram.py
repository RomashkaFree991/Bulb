import asyncio
import json
import logging
import os
import re
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Tuple
from urllib.request import Request, urlopen

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup

try:
    from portalsmp import giftsFloors, search, update_auth
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Install dependencies first: pip install -r requirements_aiogram.txt"
    ) from exc


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger("gift-price-bot-aiogram")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
PORTALS_AUTH_DATA = os.getenv("PORTALS_AUTH_DATA", "")
AUTO_REFRESH_AUTH = os.getenv("AUTO_REFRESH_AUTH", "1") == "1"
GIFTS_FILE = os.getenv("GIFTS_FILE", "gift_links.txt")
CACHE_TTL = int(os.getenv("CACHE_TTL", "60"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
REMOTE_GIFTS_JSON = "https://cdn.changes.tg/gifts/id-to-name.json"

if not BOT_TOKEN:
    raise SystemExit("Set BOT_TOKEN in your environment or .env file.")


class TTLCache:
    def __init__(self, ttl_seconds: int = 60):
        self.ttl = ttl_seconds
        self._store: Dict[str, Tuple[float, Any]] = {}

    def get(self, key: str) -> Any:
        item = self._store.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > self.ttl:
            self._store.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any) -> None:
        self._store[key] = (time.time(), value)


cache = TTLCache(CACHE_TTL)
chat_locks: defaultdict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
auth_lock = asyncio.Lock()
router = Router()


def normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9а-яё]+", "", text.lower())


def try_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip().replace(",", ".")
        if re.fullmatch(r"\d+(?:\.\d+)?", value):
            return float(value)
    return None


def extract_price(value: Any) -> Optional[float]:
    direct = try_float(value)
    if direct is not None:
        return direct

    if isinstance(value, dict):
        for key in (
            "floor_price",
            "floor",
            "price",
            "min_price",
            "minPrice",
            "value",
            "ton",
        ):
            if key in value:
                nested = try_float(value[key])
                if nested is not None:
                    return nested
        for nested_value in value.values():
            nested = try_float(nested_value)
            if nested is not None:
                return nested

    return None


def parse_markdown_gifts_file(content: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line.startswith("|"):
            continue
        parts = [part.strip() for part in line.strip("|").split("|")]
        if len(parts) < 2:
            continue
        name, gift_id = parts[0], parts[1]
        if gift_id.isdigit() and name.lower() != "gift name":
            result[gift_id] = name
    return result


def parse_plain_gifts_file(content: str) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        pipe_match = re.match(r"^(.+?)\s*[|;\t]\s*(\d{10,})\b", line)
        if pipe_match:
            name, gift_id = pipe_match.groups()
            result[gift_id] = name.strip()
            continue

        reverse_match = re.match(r"^(\d{10,})\s*[|;\t]\s*(.+)$", line)
        if reverse_match:
            gift_id, name = reverse_match.groups()
            result[gift_id] = name.strip()

    return result


def fetch_remote_mapping_sync() -> Dict[str, str]:
    request = Request(
        REMOTE_GIFTS_JSON,
        headers={"User-Agent": "Mozilla/5.0 gift-price-bot-aiogram"},
    )
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("Unexpected id-to-name.json format")
    return {str(k): str(v) for k, v in data.items()}


async def load_gift_mapping(force_refresh: bool = False) -> Dict[str, str]:
    cached = None if force_refresh else cache.get("gift_mapping")
    if cached:
        return cached

    if GIFTS_FILE and os.path.exists(GIFTS_FILE):
        with open(GIFTS_FILE, "r", encoding="utf-8") as f:
            content = f.read()
        if GIFTS_FILE.endswith(".json"):
            data = json.loads(content)
            mapping = {str(k): str(v) for k, v in data.items()}
        else:
            mapping = parse_markdown_gifts_file(content)
            if not mapping:
                mapping = parse_plain_gifts_file(content)
        if mapping:
            cache.set("gift_mapping", mapping)
            return mapping

    mapping = await asyncio.to_thread(fetch_remote_mapping_sync)
    cache.set("gift_mapping", mapping)
    return mapping


async def load_name_indexes() -> Tuple[Dict[str, str], Dict[str, str]]:
    mapping = await load_gift_mapping()
    by_id = mapping
    by_name = {normalize(name): gift_id for gift_id, name in mapping.items()}
    return by_id, by_name


def normalize_floors(raw: Any) -> Dict[str, float]:
    result: Dict[str, float] = {}

    if isinstance(raw, dict):
        for key, value in raw.items():
            price = extract_price(value)
            if price is None:
                continue
            result[normalize(str(key))] = price
        return result

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            name = item.get("name") or item.get("gift_name") or item.get("collection") or item.get("title")
            price = extract_price(item)
            if not name or price is None:
                continue
            result[normalize(str(name))] = price
        return result

    raise ValueError("Unsupported floors format from Portals")


async def ensure_portals_auth(force_refresh: bool = False) -> str:
    global PORTALS_AUTH_DATA

    if PORTALS_AUTH_DATA and not force_refresh:
        return PORTALS_AUTH_DATA

    if not AUTO_REFRESH_AUTH:
        return PORTALS_AUTH_DATA

    if not API_ID or not API_HASH:
        return PORTALS_AUTH_DATA

    async with auth_lock:
        if PORTALS_AUTH_DATA and not force_refresh:
            return PORTALS_AUTH_DATA

        logger.info("Refreshing Portals authData via update_auth()")
        token = await asyncio.to_thread(lambda: asyncio.run(update_auth(API_ID, API_HASH)))
        if not token:
            raise RuntimeError("update_auth() did not return authData")
        PORTALS_AUTH_DATA = token
        cache.set("portals_auth_data", token)
        return token


async def fetch_floors(force_refresh: bool = False) -> Dict[str, float]:
    cached = None if force_refresh else cache.get("floors")
    if cached:
        return cached

    auth_data = await ensure_portals_auth(force_refresh=False)

    def _call() -> Any:
        if auth_data:
            return giftsFloors(authData=auth_data)
        return giftsFloors()

    raw = await asyncio.to_thread(_call)
    floors = normalize_floors(raw)
    cache.set("floors", floors)
    return floors


async def fetch_min_price_for_name(gift_name: str) -> Optional[float]:
    floors = await fetch_floors()
    normalized = normalize(gift_name)
    if normalized in floors:
        return floors[normalized]

    auth_data = await ensure_portals_auth(force_refresh=False)

    def _search() -> Any:
        kwargs = {
            "sort": "price_asc",
            "limit": 1,
            "gift_name": gift_name,
        }
        if auth_data:
            kwargs["authData"] = auth_data
        return search(**kwargs)

    raw = await asyncio.to_thread(_search)
    if isinstance(raw, list) and raw:
        price = extract_price(raw[0].get("price"))
        if price is None:
            price = extract_price(raw[0].get("floor_price"))
        return price
    return None


async def resolve_query_to_name(query: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    by_id, by_name = await load_name_indexes()
    q = query.strip()

    if q.isdigit() and q in by_id:
        return by_id[q], q, None

    normalized_query = normalize(q)
    if normalized_query in by_name:
        gift_id = by_name[normalized_query]
        return by_id[gift_id], gift_id, None

    partial = []
    for gift_id, name in by_id.items():
        n = normalize(name)
        if normalized_query and normalized_query in n:
            partial.append((name, gift_id))

    if len(partial) == 1:
        return partial[0][0], partial[0][1], None

    if len(partial) > 1:
        preview = "\n".join(f"• {name} — {gift_id}" for name, gift_id in partial[:10])
        return None, None, f"Найдено несколько вариантов:\n{preview}"

    return None, None, "Не нашёл такой подарок по названию или ID."


async def safe_send_chunked(message: Message, text: str) -> None:
    max_len = 3500
    chunks = []
    current = []
    current_len = 0

    for line in text.splitlines():
        if current_len + len(line) + 1 > max_len:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line) + 1
        else:
            current.append(line)
            current_len += len(line) + 1
    if current:
        chunks.append("\n".join(current))

    for i, chunk in enumerate(chunks):
        await message.answer(chunk)
        if i < len(chunks) - 1:
            await asyncio.sleep(1.1)


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Узнать все цены")],
            [KeyboardButton(text="Помощь")],
        ],
        resize_keyboard=True,
    )


@router.message(CommandStart())
async def start_handler(message: Message) -> None:
    text = (
        "Привет.\n\n"
        "Отправь название подарка или его ID — я покажу минимальную цену на Portals в TON.\n"
        "Или нажми «Узнать все цены», чтобы получить список сразу по всем подаркам."
    )
    await message.answer(text, reply_markup=main_keyboard())


@router.message(Command("help"))
async def help_handler(message: Message) -> None:
    text = (
        "Что умеет бот:\n"
        "• по ID подарка показать минимальную цену\n"
        "• по названию подарка показать минимальную цену\n"
        "• по кнопке «Узнать все цены» вывести общий список\n\n"
        "Примеры:\n"
        "• 5936085638515261992\n"
        "• Signet Ring\n"
        "• /price Toy Bear"
    )
    await message.answer(text, reply_markup=main_keyboard())


@router.message(Command("price"))
async def price_command_handler(message: Message) -> None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("После /price пришли название подарка или его ID.")
        return
    await handle_single_lookup(message, parts[1].strip())


async def handle_single_lookup(message: Message, query: str) -> None:
    chat_id = message.chat.id
    async with chat_locks[chat_id]:
        name, gift_id, error = await resolve_query_to_name(query)
        if error:
            await message.answer(error)
            return

        try:
            price = await fetch_min_price_for_name(name)
        except Exception:
            logger.exception("Price lookup failed")
            await message.answer(
                "Не удалось получить цену с Portals. Проверь API_ID/API_HASH или временно задай PORTALS_AUTH_DATA."
            )
            return

        if price is None:
            await message.answer(f"Для {name} сейчас не удалось получить цену.")
            return

        text = (
            f"{name}\n"
            f"ID: {gift_id}\n"
            f"Минимальная цена: {price:g} TON"
        )
        await message.answer(text)


async def all_prices(message: Message) -> None:
    chat_id = message.chat.id
    async with chat_locks[chat_id]:
        await message.answer("Собираю минимальные цены...")
        try:
            floors = await fetch_floors(force_refresh=True)
            by_id, _ = await load_name_indexes()
        except Exception:
            logger.exception("All prices lookup failed")
            await message.answer(
                "Не получилось загрузить список цен. Проверь API_ID/API_HASH или временно задай PORTALS_AUTH_DATA."
            )
            return

        rows = []
        for gift_id, name in by_id.items():
            price = floors.get(normalize(name))
            if price is not None:
                rows.append((price, name, gift_id))

        if not rows:
            await message.answer("Не нашёл цен. Проверь авторизацию Portals.")
            return

        rows.sort(key=lambda item: (item[0], item[1].lower()))
        lines = ["Минимальные цены Portals"]
        for price, name, gift_id in rows:
            lines.append(f"• {name} — {price:g} TON — {gift_id}")

        await safe_send_chunked(message, "\n".join(lines))


@router.message(F.text == "Узнать все цены")
async def all_prices_handler(message: Message) -> None:
    await all_prices(message)


@router.message(F.text == "Помощь")
async def help_button_handler(message: Message) -> None:
    await help_handler(message)


@router.message(F.text)
async def text_router(message: Message) -> None:
    text = (message.text or "").strip()
    if not text:
        return
    await handle_single_lookup(message, text)


async def main() -> None:
    logger.info("Starting aiogram bot")
    if API_ID and API_HASH and AUTO_REFRESH_AUTH:
        logger.info("API_ID/API_HASH are set, authData will be refreshed via update_auth() when needed.")

    bot = Bot(BOT_TOKEN)
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
