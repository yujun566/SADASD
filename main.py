from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import sys
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from random import Random
from typing import Any, AsyncIterator, Iterable, List, Optional

# ──────────────────────────────────────────────────────────────────────────
# 0. .env 파일 로드
# ──────────────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    load_dotenv(_env_path)
except ImportError:
    pass

import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("rpg_bot")


# ══════════════════════════════════════════════════════════════════════════
# 1. 설정 (Settings)
# ══════════════════════════════════════════════════════════════════════════
def _parse_dev_ids(value: str | None) -> List[int]:
    if not value:
        return []
    result: List[int] = []
    for token in value.split(","):
        token = token.strip()
        if token.isdigit():
            result.append(int(token))
    return result


@dataclass
class Settings:
    @property
    def token(self) -> str:
        return os.getenv("DISCORD_TOKEN", "PUT_YOUR_DISCORD_BOT_TOKEN_HERE")

    @property
    def database_path(self) -> str:
        return os.getenv("RPG_DB_PATH", "discord_rpg.sqlite3")

    @property
    def dev_ids(self) -> List[int]:
        return _parse_dev_ids(os.getenv("DEV_IDS", ""))

    world_width: int = 120
    world_height: int = 120
    default_zoom: int = 1
    global_chat_limit: int = 20
    trade_tax_percent: int = 5
    daily_reward_coins: int = 300
    daily_reward_gems: int = 2


settings = Settings()


# ══════════════════════════════════════════════════════════════════════════
# 2. 데이터베이스 계층
# ──────────────────────────────────────────────────────────────────────────
# [수정사항 / FIX]
# 기존 코드는 쿼리 1번마다 새 SQLite 커넥션을 열고 닫았습니다 (필드/화면 렌더링 시
# 타일 하나하나마다 커넥션을 새로 여는 코드도 있었습니다 - 최대 수백 번/렌더).
# 이로 인해:
#   1) 매 버튼 클릭마다 처리 시간이 과도하게 길어져 디스코드의 3초 응답 제한을
#      가끔 넘기게 되고 "이 상호작용을 실패했습니다" 오류가 발생했습니다.
#   2) 여러 쿼리가 동시에 실행되며 SQLite 파일 잠금(database is locked) 예외가
#      간헐적으로 발생, 미처리 예외로 인해 상호작용 응답 자체가 누락되었습니다.
# 아래에서는 프로세스 전역에서 커넥션을 하나만 유지하고 asyncio.Lock 으로
# 직렬화하여 잠금 충돌을 없애고, 커넥션 재생성 비용도 제거했습니다.
# ══════════════════════════════════════════════════════════════════════════
DB_PATH = Path(settings.database_path)

_db_conn: aiosqlite.Connection | None = None
_db_lock = asyncio.Lock()


async def _get_conn() -> aiosqlite.Connection:
    global _db_conn
    if _db_conn is None:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db_conn = await aiosqlite.connect(DB_PATH, timeout=30)
        _db_conn.row_factory = aiosqlite.Row
        await _db_conn.execute("PRAGMA journal_mode=WAL;")
        await _db_conn.execute("PRAGMA foreign_keys=ON;")
        await _db_conn.execute("PRAGMA busy_timeout=8000;")
        await _db_conn.commit()
    return _db_conn


async def execute(query: str, params: Iterable[Any] = ()) -> None:
    async with _db_lock:
        db = await _get_conn()
        await db.execute(query, tuple(params))
        await db.commit()


async def execute_insert(query: str, params: Iterable[Any] = ()) -> int:
    """INSERT 실행 후 lastrowid 반환 (기존 코드가 매번 새 커넥션을 여는 대신
    공유 커넥션을 사용하도록 통합)."""
    async with _db_lock:
        db = await _get_conn()
        cursor = await db.execute(query, tuple(params))
        await db.commit()
        return cursor.lastrowid


async def fetch_one(query: str, params: Iterable[Any] = ()) -> Optional[aiosqlite.Row]:
    async with _db_lock:
        db = await _get_conn()
        cursor = await db.execute(query, tuple(params))
        return await cursor.fetchone()


async def fetch_all(query: str, params: Iterable[Any] = ()) -> list[aiosqlite.Row]:
    async with _db_lock:
        db = await _get_conn()
        cursor = await db.execute(query, tuple(params))
        return await cursor.fetchall()


async def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with _db_lock:
        db = await _get_conn()
        await db.executescript(
            """
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT NOT NULL,
                guild_id INTEGER,
                x INTEGER NOT NULL DEFAULT 60,
                y INTEGER NOT NULL DEFAULT 60,
                hp INTEGER NOT NULL DEFAULT 120,
                max_hp INTEGER NOT NULL DEFAULT 120,
                mp INTEGER NOT NULL DEFAULT 40,
                max_mp INTEGER NOT NULL DEFAULT 40,
                stamina INTEGER NOT NULL DEFAULT 100,
                max_stamina INTEGER NOT NULL DEFAULT 100,
                level INTEGER NOT NULL DEFAULT 1,
                exp INTEGER NOT NULL DEFAULT 0,
                coins INTEGER NOT NULL DEFAULT 500,
                gems INTEGER NOT NULL DEFAULT 0,
                attack INTEGER NOT NULL DEFAULT 8,
                defense INTEGER NOT NULL DEFAULT 5,
                crit INTEGER NOT NULL DEFAULT 3,
                facing TEXT NOT NULL DEFAULT 'S',
                zoom INTEGER NOT NULL DEFAULT 1,
                biome TEXT NOT NULL DEFAULT '평원',
                mount_name TEXT,
                pet_name TEXT,
                guild_name TEXT,
                house_x INTEGER,
                house_y INTEGER,
                last_daily_at TEXT,
                last_fished_at TEXT,
                last_farm_at TEXT,
                skill_points INTEGER NOT NULL DEFAULT 0,
                mining_level INTEGER NOT NULL DEFAULT 1,
                lumber_level INTEGER NOT NULL DEFAULT 1,
                fishing_level INTEGER NOT NULL DEFAULT 1,
                farming_level INTEGER NOT NULL DEFAULT 1,
                archery_level INTEGER NOT NULL DEFAULT 1,
                pvp_rating INTEGER NOT NULL DEFAULT 1000,
                avatar TEXT NOT NULL DEFAULT '😺',
                equipment_json TEXT NOT NULL DEFAULT '{}',
                state_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS world_tiles (
                x INTEGER NOT NULL,
                y INTEGER NOT NULL,
                tile_type TEXT NOT NULL,
                variant TEXT NOT NULL DEFAULT '',
                hp INTEGER NOT NULL DEFAULT 1,
                placed_by INTEGER,
                data_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY (x, y)
            );

            CREATE TABLE IF NOT EXISTS inventory_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                item_code TEXT NOT NULL,
                item_name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                rarity TEXT NOT NULL,
                qty INTEGER NOT NULL DEFAULT 1,
                power INTEGER NOT NULL DEFAULT 0,
                defense INTEGER NOT NULL DEFAULT 0,
                heal INTEGER NOT NULL DEFAULT 0,
                meta_json TEXT NOT NULL DEFAULT '{}',
                FOREIGN KEY (user_id) REFERENCES players(user_id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS chat_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_scope TEXT NOT NULL DEFAULT 'global',
                room_key TEXT NOT NULL DEFAULT 'global-lobby',
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL,
                guild_id INTEGER,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS trade_listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER NOT NULL,
                seller_name TEXT NOT NULL,
                item_code TEXT NOT NULL,
                item_name TEXT NOT NULL,
                price INTEGER NOT NULL,
                qty INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS battle_sessions (
                battle_id TEXT PRIMARY KEY,
                battle_type TEXT NOT NULL,
                challenger_id INTEGER NOT NULL,
                defender_id INTEGER,
                monster_code TEXT,
                state_json TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS guilds (
                guild_name TEXT PRIMARY KEY,
                owner_id INTEGER NOT NULL,
                treasury INTEGER NOT NULL DEFAULT 0,
                notice TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS player_mailbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                reward_json TEXT NOT NULL DEFAULT '{}',
                claimed INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS worm_scores (
                user_id INTEGER PRIMARY KEY,
                best_score INTEGER NOT NULL DEFAULT 0,
                total_games INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_inventory_user ON inventory_items(user_id);
            CREATE INDEX IF NOT EXISTS idx_players_xy ON players(x, y);
            """
        )
        await db.commit()


def dict_to_json(data: dict[str, Any]) -> str:
    return json.dumps(data, ensure_ascii=False)


def json_to_dict(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


# ══════════════════════════════════════════════════════════════════════════
# 3. 아이템 / 인벤토리 시스템
# ══════════════════════════════════════════════════════════════════════════
@dataclass(slots=True)
class Item:
    code: str
    name: str
    item_type: str  # 무기, 방어구, 도구, 소비, 재료, 블럭, 설치물, 스킬, 펫, 탈것
    rarity: str  # 일반, 희귀, 영웅, 전설, 신화
    power: int = 0
    defense: int = 0
    heal: int = 0
    meta: dict[str, Any] = field(default_factory=dict)


ITEM_CATALOG: dict[str, Item] = {}
DEV_ITEM_CODES = {"dev_blackmarket_sword", "dev_inf_armor"}


def register_item(item: Item):
    ITEM_CATALOG[item.code] = item


register_item(Item("wood", "🪵 나무", "재료", "일반"))
register_item(Item("stone", "🪨 돌", "재료", "일반"))
register_item(Item("herb", "🌱 약초", "재료", "일반"))
register_item(Item("iron_ore", "🥈 철광석", "재료", "일반"))
register_item(Item("gold_ore", "🥇 금광석", "재료", "희귀"))
register_item(Item("diamond", "💎 다이아몬드", "재료", "전설"))
register_item(Item("fiber", "🌿 섬유", "재료", "일반"))
register_item(Item("leather", "🐂 가죽", "재료", "일반"))

register_item(Item("small_potion", "🧪 작은 포션", "소비", "일반", heal=30))
register_item(Item("medium_potion", "🧪 보통 포션", "소비", "희귀", heal=70))
register_item(Item("large_potion", "🧪 큰 포션", "소비", "영웅", heal=150))
register_item(Item("mana_potion", "🔵 마나 포션", "소비", "일반", heal=0))

register_item(Item("dev_blackmarket_sword", "🗡 암시장 검", "무기", "신화", power=10000, meta={"equip_slot": "active", "dev_only": True}))
register_item(Item("dev_inf_armor", "🛡 개발자의 갑옷", "방어구", "신화", defense=99999, meta={"equip_slot": "armor", "dev_only": True, "inf_hp": True}))

for _i in range(10):
    for _tier in range(6):
        _w_code = f"weapon_{_i}_{_tier}"
        _w_pwr = 10 + _i * 5 + _tier * 12
        register_item(Item(_w_code, f"🗡 검 T{_tier}-{_i}", "무기", ["일반", "희귀", "영웅", "전설", "신화", "신화"][_tier], power=_w_pwr, meta={"equip_slot": "active"}))

        _a_code = f"armor_{_i}_{_tier}"
        _a_def = 5 + _i * 3 + _tier * 8
        register_item(Item(_a_code, f"🛡 갑옷 T{_tier}-{_i}", "방어구", ["일반", "희귀", "영웅", "전설", "신화", "신화"][_tier], defense=_a_def, meta={"equip_slot": "armor"}))

CRAFT_RECIPES: dict[str, dict[str, int]] = {
    "small_potion": {"herb": 2},
    "medium_potion": {"herb": 5, "iron_ore": 1},
    "large_potion": {"herb": 10, "gold_ore": 2},
    "mana_potion": {"herb": 2, "stone": 1},
    "wood_pickaxe": {"wood": 3, "stone": 2},
}
for _i in range(10):
    for _tier in range(6):
        CRAFT_RECIPES[f"weapon_{_i}_{_tier}"] = {"wood": 2 + _tier, "stone": 1 + _i}
        CRAFT_RECIPES[f"armor_{_i}_{_tier}"] = {"fiber": 2 + _tier, "stone": 1 + _i}


def get_item(code: str) -> Optional[Item]:
    return ITEM_CATALOG.get(code)


def is_dev_item(code: str) -> bool:
    return code in DEV_ITEM_CODES


async def add_item(user_id: int, item_code: str, qty: int = 1, meta_override: dict | None = None) -> int:
    """아이템 추가. 무기/방어구는 스택되지 않고 고유 ID를 가짐. 재료/소비는 스택됨."""
    item = get_item(item_code)
    if not item:
        return 0

    is_stackable = item.item_type in {"재료", "소비", "블럭", "설치물"}

    if is_stackable:
        row = await fetch_one("SELECT id, qty FROM inventory_items WHERE user_id = ? AND item_code = ?", (user_id, item_code))
        if row:
            new_qty = row["qty"] + qty
            await execute("UPDATE inventory_items SET qty = ? WHERE id = ?", (new_qty, row["id"]))
            return row["id"]
        meta = item.meta.copy()
        if meta_override:
            meta.update(meta_override)
        return await execute_insert(
            "INSERT INTO inventory_items (user_id, item_code, item_name, item_type, rarity, qty, power, defense, heal, meta_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, item_code, item.name, item.item_type, item.rarity, qty, item.power, item.defense, item.heal, dict_to_json(meta)),
        )

    # 무기/방어구 등은 1개씩 고유하게 추가
    last_id = 0
    for _ in range(qty):
        meta = item.meta.copy()
        if meta_override:
            meta.update(meta_override)
        last_id = await execute_insert(
            "INSERT INTO inventory_items (user_id, item_code, item_name, item_type, rarity, qty, power, defense, heal, meta_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (user_id, item_code, item.name, item.item_type, item.rarity, 1, item.power, item.defense, item.heal, dict_to_json(meta)),
        )
    return last_id


async def remove_item_by_id(user_id: int, inv_id: int, qty: int = 1) -> bool:
    row = await fetch_one("SELECT qty FROM inventory_items WHERE id = ? AND user_id = ?", (inv_id, user_id))
    if not row or row["qty"] < qty:
        return False
    if row["qty"] == qty:
        await execute("DELETE FROM inventory_items WHERE id = ?", (inv_id,))
    else:
        await execute("UPDATE inventory_items SET qty = qty - ? WHERE id = ?", (qty, inv_id))
    return True


async def remove_item(user_id: int, item_code: str, qty: int = 1) -> bool:
    row = await fetch_one("SELECT id, qty FROM inventory_items WHERE user_id = ? AND item_code = ?", (user_id, item_code))
    if not row or row["qty"] < qty:
        return False
    return await remove_item_by_id(user_id, row["id"], qty)


async def has_items(user_id: int, requirements: dict[str, int]) -> bool:
    for code, qty in requirements.items():
        row = await fetch_one("SELECT qty FROM inventory_items WHERE user_id = ? AND item_code = ?", (user_id, code))
        if not row or row["qty"] < qty:
            return False
    return True


async def list_inventory(user_id: int) -> list[dict[str, Any]]:
    rows = await fetch_all("SELECT * FROM inventory_items WHERE user_id = ? ORDER BY id ASC", (user_id,))
    items = []
    for r in rows:
        d = dict(r)
        d["meta"] = json_to_dict(r["meta_json"])
        items.append(d)
    return items


async def get_inventory_item(user_id: int, inv_id: int) -> Optional[dict[str, Any]]:
    row = await fetch_one("SELECT * FROM inventory_items WHERE id = ? AND user_id = ?", (inv_id, user_id))
    if not row:
        return None
    d = dict(row)
    d["meta"] = json_to_dict(row["meta_json"])
    return d


async def craft_item(user_id: int, item_code: str) -> tuple[bool, str]:
    if is_dev_item(item_code):
        return False, "⛔ 개발자 전용 아이템은 제작할 수 없습니다."

    recipe = CRAFT_RECIPES.get(item_code)
    if not recipe:
        return False, "제작할 수 없는 아이템입니다."

    for mat_code, needed in recipe.items():
        row = await fetch_one("SELECT qty FROM inventory_items WHERE user_id = ? AND item_code = ?", (user_id, mat_code))
        if not row or row["qty"] < needed:
            mat_item = get_item(mat_code)
            have = row["qty"] if row else 0
            return False, f"재료 부족: {mat_item.name if mat_item else mat_code} (필요: {needed}, 보유: {have})"

    for mat_code, needed in recipe.items():
        await remove_item(user_id, mat_code, needed)

    inv_id = await add_item(user_id, item_code, 1)
    item = get_item(item_code)
    return True, f"🛠 {item.name} 제작 완료! (ID: {inv_id})"


def get_recipe_text(item_code: str) -> str:
    if is_dev_item(item_code):
        return "⛔ 개발자 전용 아이템 (제작 불가)"
    recipe = CRAFT_RECIPES.get(item_code)
    if not recipe:
        return "제작 불가"
    parts = []
    for code, qty in recipe.items():
        item = get_item(code)
        parts.append(f"{item.name if item else code} x{qty}")
    return " + ".join(parts)


ENCHANT_SUCCESS_CHANCE = 0.6
ENCHANT_COST_COINS = 500


async def enchant_item(user_id: int, query: str) -> tuple[bool, str]:
    player = await get_player(user_id)
    if not player:
        return False, "플레이어를 찾을 수 없습니다."

    if player.coins < ENCHANT_COST_COINS:
        return False, f"코인이 부족합니다. (필요: {ENCHANT_COST_COINS})"

    inv_item = await find_item_in_inventory(user_id, query)
    if not inv_item or inv_item["item_type"] != "무기":
        return False, f"인벤토리에 인챈트 가능한 무기 '{query}'이(가) 없습니다."

    inv_id = inv_item["id"]

    player.coins -= ENCHANT_COST_COINS
    await save_player(player)

    if random.random() > ENCHANT_SUCCESS_CHANCE:
        return False, "💥 인챈트 실패! 코인만 소모되었습니다."

    meta = inv_item["meta"]
    enchant_lv = meta.get("enchant", 0) + 1
    meta["enchant"] = enchant_lv

    new_power = inv_item["power"] + int(inv_item["power"] * 0.15) + 5
    new_name = f"{inv_item['item_name']} (+{enchant_lv})"

    await execute(
        "UPDATE inventory_items SET item_name = ?, power = ?, meta_json = ? WHERE id = ?",
        (new_name, new_power, dict_to_json(meta), inv_id),
    )

    return True, f"✨ 인챈트 성공! {new_name} (공격력: {new_power})"


GACHA_COST_GEMS = 10


async def gacha_item(user_id: int) -> tuple[bool, str]:
    player = await get_player(user_id)
    if not player:
        return False, "플레이어를 찾을 수 없습니다."

    if player.gems < GACHA_COST_GEMS:
        return False, f"젬이 부족합니다. (필요: {GACHA_COST_GEMS})"

    player.gems -= GACHA_COST_GEMS
    await save_player(player)

    r = random.random()
    if r < 0.01:
        rarity, tier = "전설", 5
    elif r < 0.10:
        rarity, tier = "영웅", 3
    elif r < 0.40:
        rarity, tier = "희귀", 2
    else:
        rarity, tier = "일반", 0

    is_weapon = random.choice([True, False])
    idx = random.randint(0, 9)
    item_code = f"{'weapon' if is_weapon else 'armor'}_{idx}_{tier}"

    inv_id = await add_item(user_id, item_code, 1)
    item = get_item(item_code)

    return True, f"🎁 뽑기 결과: **[{rarity}]** {item.name} 획득! (ID: {inv_id})"


async def ensure_starter_pack(user_id: int):
    await add_item(user_id, "wood", 10)
    await add_item(user_id, "stone", 5)
    await add_item(user_id, "small_potion", 3)
    await add_item(user_id, "weapon_0_0", 1)


# ══════════════════════════════════════════════════════════════════════════
# 4. 플레이어 모델
# ══════════════════════════════════════════════════════════════════════════
@dataclass(slots=True)
class PlayerRecord:
    user_id: int
    username: str
    guild_id: int | None = None
    x: int = 60
    y: int = 60
    hp: int = 120
    max_hp: int = 120
    mp: int = 40
    max_mp: int = 40
    stamina: int = 100
    max_stamina: int = 100
    level: int = 1
    exp: int = 0
    coins: int = 500
    gems: int = 0
    attack: int = 8
    defense: int = 5
    crit: int = 3
    facing: str = "S"
    zoom: int = 1
    biome: str = "평원"
    mount_name: str | None = None
    pet_name: str | None = None
    guild_name: str | None = None
    house_x: int | None = None
    house_y: int | None = None
    last_daily_at: str | None = None
    skill_points: int = 0
    mining_level: int = 1
    lumber_level: int = 1
    fishing_level: int = 1
    farming_level: int = 1
    archery_level: int = 1
    pvp_rating: int = 1000
    avatar: str = "😺"
    equipment: dict[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_row(cls, row: Any) -> "PlayerRecord":
        return cls(
            user_id=row["user_id"],
            username=row["username"],
            guild_id=row["guild_id"],
            x=row["x"],
            y=row["y"],
            hp=row["hp"],
            max_hp=row["max_hp"],
            mp=row["mp"],
            max_mp=row["max_mp"],
            stamina=row["stamina"],
            max_stamina=row["max_stamina"],
            level=row["level"],
            exp=row["exp"],
            coins=row["coins"],
            gems=row["gems"],
            attack=row["attack"],
            defense=row["defense"],
            crit=row["crit"],
            facing=row["facing"],
            zoom=row["zoom"],
            biome=row["biome"],
            mount_name=row["mount_name"],
            pet_name=row["pet_name"],
            guild_name=row["guild_name"],
            house_x=row["house_x"],
            house_y=row["house_y"],
            last_daily_at=row["last_daily_at"],
            skill_points=row["skill_points"],
            mining_level=row["mining_level"],
            lumber_level=row["lumber_level"],
            fishing_level=row["fishing_level"],
            farming_level=row["farming_level"],
            archery_level=row["archery_level"],
            pvp_rating=row["pvp_rating"],
            avatar=row["avatar"] if "avatar" in row.keys() else "😺",
            equipment=json_to_dict(row["equipment_json"]),
            state=json_to_dict(row["state_json"]),
        )


def _spawn_position_for_user(user_id: int) -> tuple[int, int]:
    rng = Random(user_id)
    x = rng.randint(5, settings.world_width - 6)
    y = rng.randint(5, settings.world_height - 6)
    return x, y


def _get_random_avatar(user_id: int) -> str:
    avatars = ["😺", "🐶", "🦊", "🦁", "🐯", "🐼", "🐻", "🐨", "🐸", "🐷", "🐮", "🐵", "🐥", "🐧", "🦉", "🦄", "🐉", "🐢"]
    seed = abs((user_id * 77777) ^ 0xDEADBEEF)
    return avatars[seed % len(avatars)]


def _default_state() -> dict[str, Any]:
    return {
        "chat_room": "global-lobby",
        "auto_refresh": True,
        "sfx": True,
        "kills": 0,
        "panel_message_id": None,
        "panel_channel_id": None,
        "panel_guild_id": None,
    }


async def ensure_player(user_id: int, username: str, guild_id: int | None) -> PlayerRecord:
    row = await fetch_one("SELECT * FROM players WHERE user_id = ?", (user_id,))
    if row:
        player = PlayerRecord.from_row(row)
        changed = False
        if player.username != username:
            player.username = username
            changed = True
        if player.guild_id != guild_id:
            player.guild_id = guild_id
            changed = True
        defaults = _default_state()
        for key, value in defaults.items():
            if key not in player.state:
                player.state[key] = value
                changed = True
        if changed:
            await save_player(player)
        return player

    x, y = _spawn_position_for_user(user_id)
    avatar = _get_random_avatar(user_id)
    await execute(
        """
        INSERT INTO players (
            user_id, username, guild_id, x, y, avatar, equipment_json, state_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            user_id,
            username,
            guild_id,
            x,
            y,
            avatar,
            dict_to_json({"active": None, "armor": None, "pet": None, "mount": None}),
            dict_to_json(_default_state()),
        ),
    )
    row = await fetch_one("SELECT * FROM players WHERE user_id = ?", (user_id,))
    return PlayerRecord.from_row(row)


async def get_player(user_id: int) -> PlayerRecord | None:
    row = await fetch_one("SELECT * FROM players WHERE user_id = ?", (user_id,))
    return PlayerRecord.from_row(row) if row else None


async def save_player(player: PlayerRecord) -> None:
    await execute(
        """
        UPDATE players SET
            username = ?, guild_id = ?, x = ?, y = ?, hp = ?, max_hp = ?, mp = ?, max_mp = ?,
            stamina = ?, max_stamina = ?, level = ?, exp = ?, coins = ?, gems = ?, attack = ?, defense = ?,
            crit = ?, facing = ?, zoom = ?, biome = ?, mount_name = ?, pet_name = ?, guild_name = ?,
            house_x = ?, house_y = ?, last_daily_at = ?, skill_points = ?, mining_level = ?, lumber_level = ?,
            fishing_level = ?, farming_level = ?, archery_level = ?, pvp_rating = ?,
            avatar = ?, equipment_json = ?, state_json = ?, updated_at = CURRENT_TIMESTAMP
        WHERE user_id = ?
        """,
        (
            player.username, player.guild_id, player.x, player.y, player.hp, player.max_hp, player.mp, player.max_mp,
            player.stamina, player.max_stamina, player.level, player.exp, player.coins, player.gems, player.attack,
            player.defense, player.crit, player.facing, player.zoom, player.biome, player.mount_name, player.pet_name,
            player.guild_name, player.house_x, player.house_y, player.last_daily_at, player.skill_points,
            player.mining_level, player.lumber_level, player.fishing_level, player.farming_level,
            player.archery_level, player.pvp_rating, player.avatar, dict_to_json(player.equipment),
            dict_to_json(player.state), player.user_id,
        ),
    )


async def get_players_in_area(x: int, y: int, radius: int = 2) -> list[PlayerRecord]:
    rows = await fetch_all(
        "SELECT * FROM players WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ? ORDER BY level DESC, username ASC",
        (x - radius, x + radius, y - radius, y + radius),
    )
    return [PlayerRecord.from_row(row) for row in rows]


async def get_all_players() -> list[PlayerRecord]:
    rows = await fetch_all("SELECT * FROM players ORDER BY level DESC, coins DESC, username ASC")
    return [PlayerRecord.from_row(row) for row in rows]


async def award_player(user_id: int, *, coins: int = 0, gems: int = 0, exp: int = 0) -> None:
    await execute(
        "UPDATE players SET coins = coins + ?, gems = gems + ?, exp = exp + ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
        (coins, gems, exp, user_id),
    )


# ══════════════════════════════════════════════════════════════════════════
# 5. 플레이어 유틸 함수
# ══════════════════════════════════════════════════════════════════════════
DIRECTION_VECTORS: dict[str, tuple[int, int]] = {
    "W": (0, -1),
    "A": (-1, 0),
    "S": (0, 1),
    "D": (1, 0),
}
DEV_INF_HP = 999999


def clamp(value: int, min_value: int, max_value: int) -> int:
    return max(min_value, min(max_value, value))


def facing_arrow(facing: str) -> str:
    return {"W": "⬆", "A": "⬅", "S": "⬇", "D": "➡"}.get(facing, "⬇")


def move_position(x: int, y: int, direction: str) -> tuple[int, int]:
    dx, dy = DIRECTION_VECTORS.get(direction, (0, 0))
    return x + dx, y + dy


def facing_position(player: PlayerRecord) -> tuple[int, int]:
    return move_position(player.x, player.y, player.facing)


async def recompute_stats(player: PlayerRecord) -> None:
    equipment = player.equipment or {}
    active_id = equipment.get("active")
    armor_id = equipment.get("armor")
    pet_id = equipment.get("pet")
    mount_id = equipment.get("mount")

    base_attack = 8 + (player.level - 1) * 2
    base_defense = 5 + (player.level - 1)
    max_hp = 120 + (player.level - 1) * 15
    max_mp = 40 + (player.level - 1) * 5

    has_inf_armor = False

    for inv_id in [active_id, armor_id, pet_id, mount_id]:
        if not inv_id:
            continue
        inv_item = await get_inventory_item(player.user_id, int(inv_id))
        if inv_item:
            base_attack += inv_item["power"]
            base_defense += inv_item["defense"]
            if inv_item["item_type"] == "펫":
                max_hp += 20
            if inv_item["item_type"] == "탈것":
                player.max_stamina = 100 + (player.level // 3) * 5 + 15

            meta = inv_item.get("meta", {})
            if meta.get("inf_hp"):
                has_inf_armor = True

    player.attack = base_attack
    player.defense = base_defense

    if has_inf_armor:
        player.max_hp = DEV_INF_HP
        player.hp = DEV_INF_HP
    else:
        player.max_hp = max_hp
        player.hp = min(player.hp, player.max_hp)

    player.max_mp = max_mp
    player.mp = min(player.mp, player.max_mp)
    player.max_stamina = 100 + (player.level // 3) * 5
    player.stamina = min(player.stamina, player.max_stamina)


def exp_to_next(level: int) -> int:
    return 80 + level * 35


async def grant_exp(player: PlayerRecord, amount: int) -> list[str]:
    logs: list[str] = []
    player.exp += amount
    while player.exp >= exp_to_next(player.level):
        player.exp -= exp_to_next(player.level)
        player.level += 1
        player.skill_points += 1
        logs.append(f"🎉 레벨 업! Lv.{player.level}")
    await recompute_stats(player)
    return logs


async def find_item_in_inventory(user_id: int, query: str) -> dict[str, Any] | None:
    """ID(숫자) 또는 이름으로 인벤토리에서 아이템 검색"""
    items = await list_inventory(user_id)

    if query.isdigit():
        inv_id = int(query)
        for item in items:
            if item["id"] == inv_id:
                return item

    query_clean = query.strip().lower()
    for item in items:
        name_clean = item["item_name"].strip().lower()
        if query_clean in name_clean:
            return item

    return None


async def equip_item_by_query(player: PlayerRecord, query: str) -> tuple[bool, str]:
    inv_item = await find_item_in_inventory(player.user_id, query)
    if not inv_item:
        return False, f"인벤토리에 '{query}' 아이템이 없습니다."

    item_type = inv_item["item_type"]
    slot = "active"
    if item_type == "방어구":
        slot = "armor"
    elif item_type == "펫":
        slot = "pet"
    elif item_type == "탈것":
        slot = "mount"
    elif item_type != "무기":
        return False, f"{inv_item['item_name']}은(는) 장착 가능한 아이템이 아닙니다."

    player.equipment[slot] = inv_item["id"]
    if item_type == "펫":
        player.pet_name = inv_item["item_name"]
    if item_type == "탈것":
        player.mount_name = inv_item["item_name"]

    await recompute_stats(player)
    return True, f"✅ {inv_item['item_name']} (ID: {inv_item['id']}) 장착 완료"


def heal_player(player: PlayerRecord, amount: int) -> int:
    before = player.hp
    player.hp = min(player.max_hp, player.hp + amount)
    return player.hp - before


def restore_mana(player: PlayerRecord, amount: int) -> int:
    before = player.mp
    player.mp = min(player.max_mp, player.mp + amount)
    return player.mp - before


def set_random_spawn(player: PlayerRecord):
    player.x = random.randint(20, 100)
    player.y = random.randint(20, 100)


def set_state(player: PlayerRecord, key: str, value: Any) -> None:
    player.state[key] = value


def get_state(player: PlayerRecord, key: str, default: Any = None) -> Any:
    return player.state.get(key, default)


# ══════════════════════════════════════════════════════════════════════════
# 6. 월드 시스템
# ──────────────────────────────────────────────────────────────────────────
# [수정사항 / FIX] 기존 render_world()는 화면에 보이는 타일 하나하나마다
# tile_at() → get_overlay_tile() → DB 쿼리(개별 커넥션) 를 호출했습니다.
# 확대(zoom) 단계에 따라 한 번의 화면 렌더링에 최대 289회의 개별 DB 접근이
# 발생했고, 이것이 "가끔 상호작용 실패" 의 핵심 원인이었습니다 (특히 여러
# 유저가 동시에 이동/새로고침 할 때 누적 지연이 3초 응답 제한을 초과함).
# 아래에서는 화면에 보이는 범위의 설치물을 한 번의 쿼리로 모두 가져온 뒤
# 순수 동기 함수로 타일을 계산하도록 변경했습니다.
# ══════════════════════════════════════════════════════════════════════════
WORLD_WIDTH = settings.world_width
WORLD_HEIGHT = settings.world_height

RESOURCE_TILES = {"🌲", "🪨", "⛓", "🥇", "💎", "🌾", "🎣"}
BLOCKING_TILES = {"🌲", "🪨", "⛓", "🥇", "💎", "🏠", "🧱", "📦", "🔥", "🛠"}
BUILDABLE_ITEMS = {
    "wood_wall": "🧱",
    "stone_wall": "⬜",
    "craft_table": "🛠",
    "chest": "📦",
    "furnace": "🔥",
    "farm_plot": "🌾",
}


@dataclass(slots=True)
class Tile:
    icon: str
    name: str
    biome: str
    walkable: bool = True
    resource_code: str | None = None
    hp: int = 0
    placeable: bool = False
    encounter_rate: float = 0.0
    house_enterable: bool = False


BIOME_BASE = {
    "평원": ("🟩", 0.08),
    "숲": ("🟫", 0.14),
    "광산": ("⬛", 0.18),
    "호수": ("🟦", 0.05),
    "설원": ("⬜", 0.12),
    "사막": ("🟨", 0.11),
}


def current_world_state() -> dict[str, str]:
    now = datetime.now(timezone.utc)
    hour = now.hour
    phase = "낮 ☀" if 6 <= hour < 18 else "밤 🌙"
    weather_cycle = ["맑음 ☀", "비 🌧", "흐림 ☁", "눈 ❄"]
    weather = weather_cycle[(now.timetuple().tm_yday + hour // 3) % len(weather_cycle)]
    return {"phase": phase, "weather": weather, "hour": f"{hour:02d}:00 UTC"}


def _noise(x: int, y: int, salt: int = 0) -> float:
    import math
    return (math.sin((x + salt) * 0.13) + math.cos((y - salt) * 0.11) + math.sin((x + y) * 0.07)) / 3


def biome_at(x: int, y: int) -> str:
    value = _noise(x, y)
    if value < -0.33:
        return "호수"
    if value < -0.08:
        return "평원"
    if value < 0.18:
        return "숲"
    if value < 0.42:
        return "광산"
    if value < 0.62:
        return "설원"
    return "사막"


async def get_overlay_tile(x: int, y: int) -> dict[str, Any] | None:
    row = await fetch_one("SELECT tile_type, variant, hp, data_json FROM world_tiles WHERE x = ? AND y = ?", (x, y))
    if not row:
        return None
    return {"tile_type": row["tile_type"], "variant": row["variant"], "hp": row["hp"], "data_json": row["data_json"]}


async def get_overlay_tiles_in_area(x0: int, y0: int, x1: int, y1: int) -> dict[tuple[int, int], dict[str, Any]]:
    """지정된 사각 범위의 설치물/채집지 오버레이를 단 한 번의 쿼리로 가져옵니다."""
    rows = await fetch_all(
        "SELECT x, y, tile_type, variant, hp, data_json FROM world_tiles WHERE x BETWEEN ? AND ? AND y BETWEEN ? AND ?",
        (x0, x1, y0, y1),
    )
    return {
        (row["x"], row["y"]): {"tile_type": row["tile_type"], "variant": row["variant"], "hp": row["hp"], "data_json": row["data_json"]}
        for row in rows
    }


def _compute_tile(x: int, y: int, overlay: dict[str, Any] | None) -> Tile:
    """DB 접근 없이 오버레이 정보(이미 조회된 dict)로 타일을 계산하는 순수 함수."""
    x = max(0, min(WORLD_WIDTH - 1, x))
    y = max(0, min(WORLD_HEIGHT - 1, y))
    if overlay:
        tile_type = overlay["tile_type"]
        if tile_type == "house":
            return Tile(icon="🏠", name="집", biome="주거지", walkable=False, house_enterable=True)
        if tile_type == "wood_wall":
            return Tile(icon="🧱", name="나무 벽", biome="건축", walkable=False)
        if tile_type == "stone_wall":
            return Tile(icon="⬜", name="돌 벽", biome="건축", walkable=False)
        if tile_type == "craft_table":
            return Tile(icon="🛠", name="제작대", biome="건축", walkable=False)
        if tile_type == "chest":
            return Tile(icon="📦", name="상자", biome="건축", walkable=False)
        if tile_type == "furnace":
            return Tile(icon="🔥", name="화로", biome="건축", walkable=False)
        if tile_type == "farm_plot":
            return Tile(icon="🌾", name="밭", biome="농지", walkable=False, resource_code="wheat", hp=1)
        if tile_type in {"depleted", "depleted_ore"}:
            biome = biome_at(x, y)
            icon, encounter_rate = BIOME_BASE[biome]
            return Tile(icon=icon, name="채집 완료 지역", biome=biome, walkable=True, encounter_rate=encounter_rate)

    biome = biome_at(x, y)
    icon, encounter_rate = BIOME_BASE[biome]
    rng = Random(x * 10000 + y * 97)

    if biome == "숲" and rng.random() < 0.22:
        return Tile(icon="🌲", name="나무", biome=biome, walkable=False, resource_code="wood", hp=3, encounter_rate=encounter_rate)
    if biome == "광산":
        roll = rng.random()
        if roll < 0.2:
            return Tile(icon="🪨", name="바위", biome=biome, walkable=False, resource_code="stone", hp=2, encounter_rate=encounter_rate)
        if roll < 0.32:
            return Tile(icon="⛓", name="철광맥", biome=biome, walkable=False, resource_code="iron_ore", hp=3, encounter_rate=encounter_rate + 0.04)
        if roll < 0.38:
            return Tile(icon="🥇", name="금광맥", biome=biome, walkable=False, resource_code="gold_ore", hp=4, encounter_rate=encounter_rate + 0.05)
        if roll < 0.41:
            return Tile(icon="💎", name="다이아 광맥", biome=biome, walkable=False, resource_code="diamond", hp=5, encounter_rate=encounter_rate + 0.08)
    if biome == "호수" and rng.random() < 0.15:
        return Tile(icon="🎣", name="낚시 포인트", biome=biome, walkable=False, resource_code="fish", hp=1, encounter_rate=encounter_rate)
    if biome == "평원" and rng.random() < 0.10:
        return Tile(icon="🌾", name="야생 밀", biome=biome, walkable=False, resource_code="wheat", hp=1, encounter_rate=encounter_rate)
    if biome == "사막" and rng.random() < 0.08:
        return Tile(icon="🌵", name="선인장", biome=biome, walkable=False, resource_code="fiber", hp=2, encounter_rate=encounter_rate)
    if biome == "설원" and rng.random() < 0.08:
        return Tile(icon="🧊", name="얼음 허브", biome=biome, walkable=False, resource_code="herb", hp=2, encounter_rate=encounter_rate)

    return Tile(icon=icon, name=biome, biome=biome, walkable=True, encounter_rate=encounter_rate)


async def tile_at(x: int, y: int) -> Tile:
    """단일 타일 조회 (이동/채집 등 개별 지점 확인용, 렌더링에는 사용하지 않음)."""
    x = max(0, min(WORLD_WIDTH - 1, x))
    y = max(0, min(WORLD_HEIGHT - 1, y))
    overlay = await get_overlay_tile(x, y)
    return _compute_tile(x, y, overlay)


async def try_move(player: Any, direction: str) -> tuple[bool, str, str | None]:
    player.facing = direction
    nx, ny = move_position(player.x, player.y, direction)
    nx = max(0, min(WORLD_WIDTH - 1, nx))
    ny = max(0, min(WORLD_HEIGHT - 1, ny))
    tile = await tile_at(nx, ny)

    if tile.walkable:
        player.x, player.y = nx, ny
        player.biome = tile.biome
        player.stamina = max(0, player.stamina - 1)
        return True, f"{direction} 이동 완료", None

    if tile.resource_code:
        message = await gather_facing(player, nx, ny, tile)
        return False, message, "gather"

    if tile.house_enterable:
        return False, "🏠 집 앞입니다. 집 버튼으로 들어갈 수 있어요.", "house"

    return False, f"{tile.icon} {tile.name} 때문에 이동할 수 없습니다.", None


async def gather_facing(player: Any, tx: int, ty: int, tile: Tile | None = None) -> str:
    tile = tile or await tile_at(tx, ty)
    if not tile.resource_code:
        return "채집할 대상이 없습니다."

    tool_bonus = 0
    active = (player.equipment or {}).get("active")
    if active:
        if "pickaxe" in str(active) and tile.resource_code in {"stone", "iron_ore", "gold_ore", "diamond"}:
            tool_bonus = 2
        elif "axe" in str(active) and tile.resource_code == "wood":
            tool_bonus = 2
        elif active == "fishing_rod" and tile.resource_code == "fish":
            tool_bonus = 2
        elif "weapon" in str(active) or str(active).startswith("skill_"):
            tool_bonus = 1

    yield_qty = 1 + max(0, tool_bonus - 1)
    if tile.resource_code == "diamond":
        yield_qty = 1
    await add_item(player.user_id, tile.resource_code, yield_qty)

    if tile.resource_code == "wood":
        player.lumber_level += 1 if player.lumber_level < 99 and (tx + ty) % 7 == 0 else 0
    elif tile.resource_code in {"stone", "iron_ore", "gold_ore", "diamond"}:
        player.mining_level += 1 if player.mining_level < 99 and (tx * ty + 3) % 11 == 0 else 0
    elif tile.resource_code == "fish":
        player.fishing_level += 1 if player.fishing_level < 99 else 0
    elif tile.resource_code == "wheat":
        player.farming_level += 1 if player.farming_level < 99 else 0

    depleted_type = "depleted" if tile.resource_code in {"wood", "stone", "wheat", "fish", "fiber", "herb"} else "depleted_ore"
    await execute(
        "INSERT OR REPLACE INTO world_tiles (x, y, tile_type, variant, hp, data_json) VALUES (?, ?, ?, ?, ?, ?)",
        (tx, ty, depleted_type, tile.resource_code, 0, dict_to_json({"respawn": datetime.now(timezone.utc).isoformat()})),
    )

    return f"🪄 {tile.icon} {tile.name} 채집 성공! {tile.resource_code} +{yield_qty}"


async def place_structure(player: Any, item_code: str) -> tuple[bool, str]:
    target_x, target_y = facing_position(player)
    overlay = await get_overlay_tile(target_x, target_y)
    if overlay:
        return False, "이미 설치물이 있습니다."
    if item_code not in BUILDABLE_ITEMS:
        return False, "설치 가능한 아이템이 아닙니다."
    ok = await remove_item(player.user_id, item_code, 1)
    if not ok:
        return False, "설치할 아이템이 인벤토리에 없습니다."
    await execute(
        "INSERT OR REPLACE INTO world_tiles (x, y, tile_type, variant, hp, placed_by, data_json) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (target_x, target_y, item_code, item_code, 5, player.user_id, dict_to_json({"owner": player.user_id})),
    )
    return True, f"{BUILDABLE_ITEMS[item_code]} 설치 완료"


async def build_house(player: Any) -> tuple[bool, str]:
    target_x, target_y = facing_position(player)
    requirements = {"wood": 20, "stone": 10}
    if not await has_items(player.user_id, requirements):
        return False, "집을 짓기 위한 재료가 부족합니다. (나무20/돌10)"
    for code, qty in requirements.items():
        await remove_item(player.user_id, code, qty)
    await execute(
        "INSERT OR REPLACE INTO world_tiles (x, y, tile_type, variant, hp, placed_by, data_json) VALUES (?, ?, 'house', 'small_house', 20, ?, ?)",
        (target_x, target_y, player.user_id, dict_to_json({"owner": player.user_id})),
    )
    player.house_x = target_x
    player.house_y = target_y
    return True, "🏠 집 건설 완료"


async def open_chest(player: Any) -> str:
    rewards = [("coins", 120), ("small_potion", 2), ("iron_ore", 3), ("weapon_2_4", 1), ("diamond", 1)]
    pick = rewards[(player.x * 31 + player.y * 17 + player.level) % len(rewards)]
    if pick[0] == "coins":
        player.coins += int(pick[1])
        return f"📦 상자에서 코인 {pick[1]} 획득"
    await add_item(player.user_id, str(pick[0]), int(pick[1]))
    return f"📦 상자에서 {pick[0]} x{pick[1]} 획득"


async def enter_house(player: Any) -> str:
    if player.house_x is None or player.house_y is None:
        return "아직 집이 없습니다."
    player.state["inside_house"] = not player.state.get("inside_house", False)
    return "🏠 집 내부로 들어왔습니다." if player.state["inside_house"] else "🌍 집 밖으로 나왔습니다."


def should_encounter(x: int, y: int, level: int) -> bool:
    biome = biome_at(x, y)
    _, rate = BIOME_BASE[biome]
    state = current_world_state()
    bonus = 0.03 if "밤" in state["phase"] else 0
    rng = Random(x * 503 + y * 991 + level * 17 + datetime.now(timezone.utc).minute)
    return rng.random() < rate + bonus


# ══════════════════════════════════════════════════════════════════════════
# 7. 전투 시스템
# ══════════════════════════════════════════════════════════════════════════
MONSTER_CATALOG: dict[str, dict[str, Any]] = {}
BASE_SPECIES = ["슬라임", "늑대", "고블린", "오크", "박쥐", "해골", "좀비", "거미", "정령", "도적"]
BIOME_ADJ = ["숲", "그림자", "용암", "서리", "폭풍", "사막", "심해", "광산", "달빛", "맹독"]
TIER_ADJ = ["새끼", "야생", "흉포한", "강인한", "정예", "영웅", "군단장", "고대", "전설", "악몽"]

for _bi, _biome_name in enumerate(BIOME_ADJ):
    for _tj, _tier in enumerate(TIER_ADJ):
        for _sk, _species in enumerate(BASE_SPECIES):
            _code = f"mob_{_bi}_{_tj}_{_sk}"
            _level = 1 + _bi + _tj + _sk % 7
            MONSTER_CATALOG[_code] = {
                "code": _code,
                "name": f"{_biome_name} {_tier} {_species}",
                "icon": "👾",
                "level": _level,
                "max_hp": 40 + _level * 18,
                "attack": 6 + _level * 3,
                "defense": 2 + _level * 2,
                "exp": 16 + _level * 8,
                "coins": 10 + _level * 5,
                "skills": ["기본공격"],
            }

WORLD_BOSSES = [
    {"code": "boss_kraken", "name": "🌍 월드보스 크라켄", "level": 45, "max_hp": 2400, "attack": 85, "defense": 45, "exp": 1200, "coins": 2500, "icon": "🐙"},
]

DEV_ARMOR_DROP_CHANCE = 0.0007
NORMAL_ARMOR_DROP_CHANCE = 0.0005


@dataclass(slots=True)
class BattleResult:
    finished: bool
    message: str
    reward_lines: list[str]


def pick_monster(x: int, y: int, player_level: int) -> dict[str, Any]:
    biome = biome_at(x, y)
    keys = list(MONSTER_CATALOG.keys())
    seed = abs(x * 10007 + y * 131 + player_level * 17)
    rng = Random(seed)
    pool = [MONSTER_CATALOG[k] for k in keys if biome in MONSTER_CATALOG[k]["name"] or rng.random() < 0.08]
    if not pool:
        pool = [MONSTER_CATALOG[keys[seed % len(keys)]]]
    monster = pool[seed % len(pool)].copy()
    monster["hp"] = monster["max_hp"]
    return monster


def pick_world_boss(x: int, y: int) -> dict[str, Any]:
    boss = WORLD_BOSSES[(x + y) % len(WORLD_BOSSES)].copy()
    boss["hp"] = boss["max_hp"]
    return boss


def _get_dev_ids() -> list[int]:
    try:
        return list(settings.dev_ids)
    except Exception:
        return []


async def start_monster_battle(player: Any, monster: dict[str, Any], battle_type: str = "monster") -> str:
    battle_id = uuid.uuid4().hex[:16]
    state = {
        "battle_id": battle_id,
        "player_hp": player.hp,
        "player_mp": player.mp,
        "turn": "player",
        "log": [f"⚔ {monster['name']} 등장!"],
        "monster": monster,
        "battle_type": battle_type,
    }
    if battle_type == "worldboss":
        state["location"] = {"x": player.x, "y": player.y}
        state["participants"] = {str(player.user_id): {"name": player.username, "damage": 0}}

    await execute(
        "INSERT INTO battle_sessions (battle_id, battle_type, challenger_id, monster_code, state_json) VALUES (?, ?, ?, ?, ?)",
        (battle_id, battle_type, player.user_id, monster["code"], dict_to_json(state)),
    )
    player.state["battle_id"] = battle_id
    return battle_id


async def start_pvp_battle(challenger: Any, defender: Any) -> str:
    battle_id = uuid.uuid4().hex[:16]
    state = {
        "battle_id": battle_id,
        "player_hp": challenger.hp,
        "player_mp": challenger.mp,
        "enemy_hp": defender.hp,
        "enemy_name": defender.username,
        "enemy_attack": defender.attack,
        "enemy_defense": defender.defense,
        "turn": "player",
        "log": [f"⚔ PVP 시작! {defender.username} 와의 결투"],
        "battle_type": "pvp",
        "defender_id": defender.user_id,
        "defender_is_dev": defender.user_id in _get_dev_ids(),
    }
    await execute(
        "INSERT INTO battle_sessions (battle_id, battle_type, challenger_id, defender_id, state_json) VALUES (?, 'pvp', ?, ?, ?)",
        (battle_id, challenger.user_id, defender.user_id, dict_to_json(state)),
    )
    challenger.state["battle_id"] = battle_id
    defender.state["battle_id"] = battle_id
    return battle_id


async def get_battle_state(battle_id: str) -> dict[str, Any] | None:
    row = await fetch_one("SELECT state_json FROM battle_sessions WHERE battle_id = ?", (battle_id,))
    if not row:
        return None
    return json_to_dict(row["state_json"])


async def save_battle_state(battle_id: str, state: dict[str, Any]) -> None:
    await execute("UPDATE battle_sessions SET state_json = ?, updated_at = CURRENT_TIMESTAMP WHERE battle_id = ?", (dict_to_json(state), battle_id))


async def close_battle(battle_id: str) -> None:
    await execute("DELETE FROM battle_sessions WHERE battle_id = ?", (battle_id,))


async def _loot_player(winner_id: int, loser_id: int) -> list[str]:
    loot_logs = []
    loser_items = await list_inventory(loser_id)
    if not loser_items:
        return ["상대방의 인벤토리가 비어있습니다."]

    num_loot = min(len(loser_items), random.randint(1, 2))
    to_loot = random.sample(loser_items, num_loot)
    for item in to_loot:
        if item["item_code"] in {"dev_blackmarket_sword", "dev_inf_armor"}:
            continue
        qty_to_steal = max(1, item["qty"] // 2) if item["item_type"] == "재료" else 1
        if await remove_item_by_id(loser_id, item["id"], qty_to_steal):
            await add_item(winner_id, item["item_code"], qty_to_steal, meta_override=item["meta"])
            loot_logs.append(f"🏴‍☠️ 약탈 성공! {item['item_name']} x{qty_to_steal} 획득!")
    return loot_logs


async def _try_drop_armor(player: Any, is_dev_kill: bool = False) -> list[str]:
    drop_lines = []
    if is_dev_kill and random.random() < DEV_ARMOR_DROP_CHANCE:
        await add_item(player.user_id, "dev_inf_armor", 1)
        drop_lines.append("🎊 ✨ [개발자의 갑옷] 드롭! (0.07%)")
    elif random.random() < NORMAL_ARMOR_DROP_CHANCE:
        armor_code = f"armor_{random.randint(0, 9)}_{random.randint(0, 5)}"
        await add_item(player.user_id, armor_code, 1)
        drop_lines.append(f"🎁 방어구 드롭! {get_item(armor_code).name} 획득!")
    return drop_lines


async def perform_battle_action(player: Any, action: str) -> BattleResult:
    battle_id = player.state.get("battle_id")
    if not battle_id:
        return BattleResult(True, "전투 없음", [])
    state = await get_battle_state(battle_id)
    if not state:
        player.state.pop("battle_id", None)
        return BattleResult(True, "세션 없음", [])

    logs = state.get("log", [])[-8:]
    battle_type = state.get("battle_type", "monster")

    if action == "run":
        await close_battle(battle_id)
        player.state.pop("battle_id", None)
        return BattleResult(True, "🏃 도주했습니다.", [])

    if action == "potion":
        item = await find_item_in_inventory(player.user_id, "포션")
        if not item or item["item_type"] != "소비" or not item.get("heal"):
            return BattleResult(False, "🧪 사용 가능한 포션이 없습니다.", [])
        healed = heal_player(player, item["heal"])
        await remove_item_by_id(player.user_id, item["id"], 1)
        logs.append(f"🧪 {item['item_name']} 사용, HP {healed} 회복")
        state["log"] = logs
        await save_battle_state(battle_id, state)
        return BattleResult(False, "\n".join(logs[-6:]), [])

    enemy_name = state.get("enemy_name") if battle_type == "pvp" else state["monster"]["name"]
    enemy_hp = state.get("enemy_hp", 0) if battle_type == "pvp" else state["monster"]["hp"]
    enemy_def = state.get("enemy_defense", 0) if battle_type == "pvp" else state["monster"]["defense"]
    enemy_atk = state.get("enemy_attack", 0) if battle_type == "pvp" else state["monster"]["attack"]

    dmg = max(1, player.attack - enemy_def // 2)
    enemy_hp -= dmg
    logs.append(f"⚔ {enemy_name}에게 {dmg} 피해!")

    if enemy_hp <= 0:
        rewards = []
        if battle_type == "pvp":
            rewards.append("🏆 PVP 승리!")
            rewards.extend(await _loot_player(player.user_id, state["defender_id"]))
            rewards.extend(await _try_drop_armor(player, is_dev_kill=state.get("defender_is_dev")))
        else:
            monster = state["monster"]
            player.coins += monster["coins"]
            rewards.append(f"🏆 승리! 코인 +{monster['coins']}")
            rewards.extend(await grant_exp(player, monster["exp"]))
            rewards.extend(await _try_drop_armor(player))

        await close_battle(battle_id)
        player.state.pop("battle_id", None)
        return BattleResult(True, "\n".join(logs[-6:]), rewards)

    incoming = max(1, enemy_atk - player.defense // 2)
    player.hp -= incoming
    logs.append(f"💢 {enemy_name}의 반격! {incoming} 피해!")

    if player.hp <= 0:
        player.hp = player.max_hp // 2
        await close_battle(battle_id)
        player.state.pop("battle_id", None)
        return BattleResult(True, "💀 쓰러졌습니다.", [])

    if battle_type == "pvp":
        state["enemy_hp"] = enemy_hp
    else:
        state["monster"]["hp"] = enemy_hp
    state["log"] = logs
    await save_battle_state(battle_id, state)
    return BattleResult(False, "\n".join(logs[-6:]), [f"적 HP: {max(0, enemy_hp)}"])


def battle_preview(state: dict[str, Any]) -> str:
    if not state:
        return "탐험 중"
    battle_type = state.get("battle_type")
    hp = state.get("enemy_hp", 0) if battle_type == "pvp" else state.get("monster", {}).get("hp", 0)
    name = state.get("enemy_name", "상대") if battle_type == "pvp" else state.get("monster", {}).get("name", "적")
    return f"⚔ {name}와 전투 중! (HP: {hp})"


# ══════════════════════════════════════════════════════════════════════════
# 8. 렌더링
# ══════════════════════════════════════════════════════════════════════════
def _bar(current: int, maximum: int, filled: str, empty: str, size: int = 10) -> str:
    maximum = max(1, maximum)
    ratio = max(0.0, min(1.0, current / maximum))
    filled_count = int(round(ratio * size))
    filled_count = max(0, min(size, filled_count))
    return filled * filled_count + empty * (size - filled_count)


def _safe_lines(lines: list[str], fallback: str, limit: int) -> list[str]:
    picked = lines[:limit] if lines else [fallback]
    return [line[:84] for line in picked]


async def render_world(
    player: Any,
    nearby_players: list[Any],
    chat_lines: list[str],
    battle_state: dict[str, Any] | None = None,
) -> str:
    zoom = max(1, min(3, int(player.zoom or 1)))
    radius = 4 + (zoom - 1) * 2
    lines: list[str] = []
    lines.append("```")
    lines.append("╔═══════════════════════ 🌸 CUTE OPEN WORLD RPG 🌸 ═══════════════════════╗")
    state = current_world_state()
    lines.append(
        f"║ 좌표 ({player.x:03},{player.y:03})  방향 {facing_arrow(player.facing)}  지역 {player.biome:<6}  시간 {state['phase']:<6}  날씨 {state['weather']:<8} ║"
    )
    lines.append("╠══════════════════════════════════════════════════════════════════════════╣")

    x0, x1 = player.x - radius, player.x + radius
    y0, y1 = player.y - radius, player.y + radius
    # [FIX] 화면 범위의 오버레이 타일을 단 한 번의 쿼리로 가져와서 사용 (기존:
    # 셀 하나당 쿼리 1개, 최악의 경우 289회 → 지금은 항상 1회)
    overlays = await get_overlay_tiles_in_area(
        max(0, x0), max(0, y0), min(WORLD_WIDTH - 1, x1), min(WORLD_HEIGHT - 1, y1)
    )

    player_positions = {(p.x, p.y): p for p in nearby_players if p.user_id != player.user_id}
    for y in range(y0, y1 + 1):
        row_cells = []
        for x in range(x0, x1 + 1):
            if x == player.x and y == player.y:
                row_cells.append(player.avatar or "😺")
                continue
            if (x, y) in player_positions:
                p = player_positions[(x, y)]
                row_cells.append(getattr(p, "avatar", "🧑"))
                continue
            cx, cy = max(0, min(WORLD_WIDTH - 1, x)), max(0, min(WORLD_HEIGHT - 1, y))
            tile = _compute_tile(cx, cy, overlays.get((cx, cy)))
            row_cells.append(tile.icon)
        lines.append("║ " + " ".join(row_cells) + " ║")

    active_id = (player.equipment or {}).get("active")
    armor_id = (player.equipment or {}).get("armor")

    lines.append("╠══════════════════════════════════════════════════════════════════════════╣")
    lines.append(f"║ ❤️ HP  {_bar(player.hp, player.max_hp, '🟥', '⬛')} {player.hp:>3}/{player.max_hp:<3}   💙 MP  {_bar(player.mp, player.max_mp, '🟦', '⬛')} {player.mp:>3}/{player.max_mp:<3} ║")
    lines.append(f"║ 💚 STA {_bar(player.stamina, player.max_stamina, '🟩', '⬛')} {player.stamina:>3}/{player.max_stamina:<3}   💰 {player.coins:<6}  ⭐ Lv.{player.level:<3}  💎 {player.gems:<3} ║")
    lines.append(f"║ 공격 {player.attack:<3} 방어 {player.defense:<3} 장착무기 ID: {str(active_id or '없음'):<10} 장착방어구 ID: {str(armor_id or '없음'):<10} ║")

    lines.append("╠══════════════════════════════════════════════════════════════════════════╣")
    lines.append("║ 💬 글로벌 채팅                                                           ║")
    for chat in _safe_lines(chat_lines or [], "대화 없음", 3):
        lines.append(f"║ {chat:<84} ║")

    lines.append("╠══════════════════════════════════════════════════════════════════════════╣")
    battle_text = battle_preview(battle_state) if battle_state else "탐험 중 · 버튼으로 이동하고 전투/거래/채팅/설정을 열 수 있어요"
    for raw in battle_text.splitlines()[:5]:
        lines.append(f"║ {raw[:84]:<84} ║")
    lines.append("╚══════════════════════════════════════════════════════════════════════════╝")
    lines.append("```")
    return "\n".join(lines)


async def render_inventory(items: list[dict[str, Any]], equipment: dict[str, Any]) -> str:
    lines = ["```"]
    lines.append("╔══════════════════════════════════════════════════╗")
    lines.append("║              🎒 인벤토리 (고유 ID 기반)          ║")
    lines.append("╠══════════════════════════════════════════════════╣")
    lines.append("║ ID    이름                     수량    정보      ║")
    lines.append("╠══════════════════════════════════════════════════╣")

    if not items:
        lines.append("║ (비어 있음)                                      ║")
    else:
        for item in items[:20]:
            info = ""
            if item.get("power"):
                info = f"ATK {item['power']}"
            elif item.get("defense"):
                info = f"DEF {item['defense']}"
            elif item.get("heal"):
                info = f"HEAL {item['heal']}"

            line = f"║ {item['id']:<5} {item['item_name'][:18]:<18} {item['qty']:<7} {info:<10} ║"
            lines.append(line)

    lines.append("╚══════════════════════════════════════════════════╝")
    lines.append("```")
    lines.append("💡 무기/방어구는 고유 ID를 사용하여 장착/인챈트하세요.")
    return "\n".join(lines)


def render_craft_list() -> str:
    lines = ["```"]
    lines.append("╔══════════════════════════════════════════════════════════════════════╗")
    lines.append("║                    🛠 제작 가능 아이템 목록                          ║")
    lines.append("╠══════════════════════════════════════════════════════════════════════╣")

    categories: dict[str, list[tuple[str, str, str]]] = {}
    for code, recipe in CRAFT_RECIPES.items():
        if code in DEV_ITEM_CODES:
            continue
        item = get_item(code)
        if not item:
            continue
        cat = item.item_type
        categories.setdefault(cat, []).append((item.name, code, get_recipe_text(code)))

    for cat, items_list in categories.items():
        lines.append(f"║ ▶ [{cat}]                                                              ║")
        for item_name, code, recipe_text in sorted(items_list, key=lambda x: x[0])[:5]:
            name_line = f"  {item_name} ({code})"
            recipe_line = f"    재료: {recipe_text}"
            lines.append(f"║ {name_line[:68]:<68} ║")
            lines.append(f"║ {recipe_line[:68]:<68} ║")
        lines.append("║                                                                      ║")

    lines.append("╚══════════════════════════════════════════════════════════════════════╝")
    lines.append("```")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════
# 9. 지렁이 게임
# ══════════════════════════════════════════════════════════════════════════
@dataclass(slots=True)
class WormState:
    width: int = 12
    height: int = 12
    worm: list[tuple[int, int]] = field(default_factory=lambda: [(5, 5), (4, 5), (3, 5)])
    direction: str = "D"
    food: tuple[int, int] = (8, 5)
    score: int = 0
    alive: bool = True


WORM_DIR = {"W": (0, -1), "A": (-1, 0), "S": (0, 1), "D": (1, 0)}


def worm_spawn_food(state: WormState) -> tuple[int, int]:
    rng = Random(state.score * 97 + len(state.worm) * 11)
    while True:
        pos = (rng.randint(0, state.width - 1), rng.randint(0, state.height - 1))
        if pos not in state.worm:
            return pos


def worm_step(state: WormState, direction: str | None = None) -> WormState:
    if not state.alive:
        return state
    if direction in WORM_DIR:
        state.direction = direction
    dx, dy = WORM_DIR[state.direction]
    head_x, head_y = state.worm[0]
    new_head = (head_x + dx, head_y + dy)
    if not (0 <= new_head[0] < state.width and 0 <= new_head[1] < state.height):
        state.alive = False
        return state
    if new_head in state.worm:
        state.alive = False
        return state
    state.worm.insert(0, new_head)
    if new_head == state.food:
        state.score += 1
        state.food = worm_spawn_food(state)
    else:
        state.worm.pop()
    return state


def worm_to_dict(state: WormState) -> dict:
    return {
        "width": state.width,
        "height": state.height,
        "worm": state.worm,
        "direction": state.direction,
        "food": state.food,
        "score": state.score,
        "alive": state.alive,
    }


def worm_from_dict(data: dict) -> WormState:
    return WormState(
        width=int(data.get("width", 12)),
        height=int(data.get("height", 12)),
        worm=[tuple(seg) for seg in data.get("worm", [(5, 5), (4, 5), (3, 5)])],
        direction=str(data.get("direction", "D")),
        food=tuple(data.get("food", (8, 5))),
        score=int(data.get("score", 0)),
        alive=bool(data.get("alive", True)),
    )


def render_worm(state: WormState) -> str:
    lines = ["```", "╔════════ 지렁이 게임 ════════╗"]
    for y in range(state.height):
        row = []
        for x in range(state.width):
            pos = (x, y)
            if pos == state.worm[0]:
                row.append("😎")
            elif pos in state.worm:
                row.append("🟩")
            elif pos == state.food:
                row.append("🍎")
            else:
                row.append("⬛")
        lines.append("║ " + "".join(row) + " ║")
    status = "생존중" if state.alive else "게임오버"
    lines.append(f"║ 점수 {state.score:<2} 상태 {status:<8} ║")
    lines.append("╚════════════════════════════╝")
    lines.append("```")
    lines.append("W/A/S/D 버튼으로 이동, 재시작 버튼 제공")
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════════════
# 10. UI 컴포넌트 (View / Modal)
# ──────────────────────────────────────────────────────────────────────────
# [수정사항 / FIX]
#  1) 모든 버튼/모달 콜백은 실제 작업(DB 조회 등)을 하기 전에 먼저
#     interaction.response.defer() 를 호출하여 디스코드의 3초 응답 제한
#     내에 반드시 "확인 응답(ack)"을 보내도록 했습니다. 이전에는 DB 작업이
#     끝난 뒤에야 최초 응답을 보냈기 때문에, 작업이 조금만 느려져도
#     (동시 접속자 증가, SQLite 지연 등) "상호작용 실패" 메시지가 떴습니다.
#  2) 모든 View에 on_error 를 재정의하여, 콜백 내부에서 예외가 발생해도
#     사용자에게 안내 메시지가 보이도록 하고 서버 로그에 스택트레이스를
#     남기도록 했습니다. 이전에는 처리되지 않은 예외가 발생하면 상호작용에
#     어떠한 응답도 가지 않아 "상호작용 실패" 로 보였습니다.
#  3) WormView 클래스가 실제로는 정의되어 있지 않아 worm_cog.py 의
#     `from utils.buttons import WormView` 가 항상 ImportError 를 일으켜
#     지렁이 게임 기능 전체(및 해당 코그)가 로드조차 되지 않던 문제를
#     수정했습니다.
# ══════════════════════════════════════════════════════════════════════════
async def _safe_ack(interaction: discord.Interaction) -> None:
    """아직 응답하지 않은 상호작용을 즉시 defer 하여 3초 타임아웃을 방지합니다."""
    if not interaction.response.is_done():
        try:
            await interaction.response.defer()
        except discord.HTTPException:
            pass


class BaseGameView(discord.ui.View):
    """모든 게임 View 의 공통 에러 처리를 담당하는 베이스 클래스."""

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item) -> None:
        log.error("View 콜백 처리 중 오류 발생: %s", error, exc_info=error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⚠️ 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ 처리 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.", ephemeral=True)
        except discord.HTTPException:
            pass


class ChatModal(discord.ui.Modal, title="글로벌 채팅"):
    message = discord.ui.TextInput(label="채팅 내용", style=discord.TextStyle.paragraph, max_length=180)

    def __init__(self, cog, user_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_chat_submit(interaction, self.user_id, str(self.message))

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        log.error("ChatModal 오류: %s", error, exc_info=error)
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⚠️ 채팅 전송 중 오류가 발생했습니다.", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ 채팅 전송 중 오류가 발생했습니다.", ephemeral=True)
        except discord.HTTPException:
            pass


class EquipModal(discord.ui.Modal, title="아이템 장착"):
    query = discord.ui.TextInput(label="아이템 ID 또는 이름", placeholder="예: 42 또는 검")

    def __init__(self, cog, user_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_equip_submit(interaction, self.user_id, str(self.query))


class CraftModal(discord.ui.Modal, title="아이템 제작"):
    item_code = discord.ui.TextInput(label="제작할 아이템 코드", placeholder="예: weapon_0_1")

    def __init__(self, cog, user_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_craft_submit(interaction, self.user_id, str(self.item_code))


class EnchantModal(discord.ui.Modal, title="무기 인챈트 (500코인)"):
    query = discord.ui.TextInput(label="무기 ID 또는 이름", placeholder="예: 42 또는 검")

    def __init__(self, cog, user_id: int):
        super().__init__()
        self.cog = cog
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_enchant_submit(interaction, self.user_id, str(self.query))


class RPGView(BaseGameView):
    def __init__(self, cog, user_id: int):
        super().__init__(timeout=None)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("자신의 캐릭터만 조작할 수 있습니다.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="W", style=discord.ButtonStyle.primary, row=0)
    async def move_up(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_move(interaction, "W")

    @discord.ui.button(label="⚔ 공격", style=discord.ButtonStyle.danger, row=0)
    async def attack(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_battle_action(interaction, "attack")

    @discord.ui.button(label="🏃 도주", style=discord.ButtonStyle.secondary, row=0)
    async def run(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_battle_action(interaction, "run")

    @discord.ui.button(label="🧪 포션", style=discord.ButtonStyle.success, row=0)
    async def potion(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_battle_action(interaction, "potion")

    @discord.ui.button(label="A", style=discord.ButtonStyle.primary, row=1)
    async def move_left(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_move(interaction, "A")

    @discord.ui.button(label="S", style=discord.ButtonStyle.primary, row=1)
    async def move_down(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_move(interaction, "S")

    @discord.ui.button(label="D", style=discord.ButtonStyle.primary, row=1)
    async def move_right(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_move(interaction, "D")

    @discord.ui.button(label="🎒 인벤", style=discord.ButtonStyle.success, row=2)
    async def inventory(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_inventory(interaction)

    @discord.ui.button(label="🧰 장착", style=discord.ButtonStyle.secondary, row=2)
    async def equip(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(EquipModal(self.cog, self.user_id))

    @discord.ui.button(label="🛠 제작", style=discord.ButtonStyle.secondary, row=2)
    async def craft(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CraftModal(self.cog, self.user_id))

    @discord.ui.button(label="📖 제작목록", style=discord.ButtonStyle.secondary, row=2)
    async def craft_list(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_craft_list(interaction)

    @discord.ui.button(label="✨ 인챈트", style=discord.ButtonStyle.primary, row=3)
    async def enchant(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(EnchantModal(self.cog, self.user_id))

    @discord.ui.button(label="🎁 젬뽑기(10젬)", style=discord.ButtonStyle.primary, row=3)
    async def gacha(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.handle_gacha(interaction)

    @discord.ui.button(label="💬 채팅", style=discord.ButtonStyle.success, row=3)
    async def chat(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(ChatModal(self.cog, self.user_id))

    @discord.ui.button(label="🔁 새로고침", style=discord.ButtonStyle.secondary, row=3)
    async def refresh(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await _safe_ack(interaction)
        await self.cog.refresh_gui(interaction)

    @discord.ui.button(label="⚙️ 설정", style=discord.ButtonStyle.secondary, row=3)
    async def settings_btn(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_message("설정 메뉴를 엽니다.", view=SettingsView(self.cog, self.user_id), ephemeral=True)


class SettingsView(BaseGameView):
    def __init__(self, cog, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id

    @discord.ui.button(label="자동새로고침", style=discord.ButtonStyle.success)
    async def auto_refresh(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.toggle_setting(interaction, self.user_id, "auto_refresh")


class TradeHubView(BaseGameView):
    def __init__(self, cog, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id

    @discord.ui.button(label="새로고침", style=discord.ButtonStyle.secondary)
    async def list_market(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.show_market(interaction)


class WormView(BaseGameView):
    """[FIX] 기존 코드에서 누락되어 있던 지렁이 게임용 View. 이 클래스가 없어서
    worm_cog 가 임포트 단계에서부터 실패해 /worm 명령어 전체가 등록되지 않았습니다."""

    def __init__(self, cog, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("자신의 게임만 조작할 수 있습니다.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="⬆", style=discord.ButtonStyle.primary, row=0)
    async def up(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.handle_worm_move(interaction, "W")

    @discord.ui.button(label="⬅", style=discord.ButtonStyle.primary, row=1)
    async def left(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.handle_worm_move(interaction, "A")

    @discord.ui.button(label="⬇", style=discord.ButtonStyle.primary, row=1)
    async def down(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.handle_worm_move(interaction, "S")

    @discord.ui.button(label="➡", style=discord.ButtonStyle.primary, row=1)
    async def right(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.handle_worm_move(interaction, "D")

    @discord.ui.button(label="🔁 재시작", style=discord.ButtonStyle.secondary, row=2)
    async def restart(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cog.restart_worm(interaction)


# ══════════════════════════════════════════════════════════════════════════
# 11. 코그 (Cogs)
# ══════════════════════════════════════════════════════════════════════════
class RPGCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.active_panels: dict[int, discord.Interaction] = {}  # user_id -> last_interaction
        self.auto_refresh_loop.start()

    def cog_unload(self):
        self.auto_refresh_loop.cancel()

    @tasks.loop(seconds=5.0)
    async def auto_refresh_loop(self):
        to_remove = []
        for user_id, interaction in list(self.active_panels.items()):
            try:
                player = await ensure_player(user_id, interaction.user.display_name, interaction.guild_id)
                if not player.state.get("auto_refresh", True):
                    continue

                content, view = await self._refresh_payload(player)
                await interaction.edit_original_response(content=content, view=view)
            except (discord.NotFound, discord.HTTPException) as e:
                # 상호작용 토큰이 만료되었거나(15분 초과) 메시지가 삭제된 경우.
                # 이건 배경 작업의 자체 정리이며 사용자에게 "상호작용 실패"로
                # 보이지 않으므로 조용히 패널 목록에서만 제거합니다.
                log.info(f"자동 새로고침 갱신 실패, 패널 제거 (User {user_id}): {e}")
                to_remove.append(user_id)
            except Exception as e:
                log.error(f"자동 새로고침 중 예기치 못한 오류 (User {user_id}): {e}", exc_info=e)
                to_remove.append(user_id)

        for user_id in to_remove:
            self.active_panels.pop(user_id, None)

    @auto_refresh_loop.before_loop
    async def before_auto_refresh_loop(self):
        await self.bot.wait_until_ready()

    async def _player(self, interaction: discord.Interaction) -> PlayerRecord:
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        if not player.state.get("spawned"):
            set_random_spawn(player)
            player.state["spawned"] = True
            await save_player(player)
        await recompute_stats(player)
        return player

    async def _chat_lines(self) -> list[str]:
        try:
            rows = await fetch_all(
                "SELECT username, message FROM chat_messages WHERE room_key = 'global-lobby' ORDER BY id DESC LIMIT 3"
            )
            return [f"💬 {row['username']}: {row['message']}" for row in rows][::-1]
        except Exception as e:
            log.warning(f"채팅 로그 조회 실패: {e}")
            return []

    async def _refresh_payload(self, player: PlayerRecord) -> tuple[str, RPGView]:
        nearby = await get_players_in_area(player.x, player.y, radius=2)
        chat_lines = await self._chat_lines()
        battle_state = None
        battle_id = player.state.get("battle_id")
        if battle_id:
            battle_state = await get_battle_state(battle_id)
        content = await render_world(player, nearby, chat_lines, battle_state)
        return content, RPGView(self, player.user_id)

    async def refresh_gui(self, interaction: discord.Interaction, note: str | None = None) -> None:
        try:
            player = await self._player(interaction)
            content, view = await self._refresh_payload(player)
            if note:
                content = f"{note}\n\n{content}"

            if interaction.response.is_done():
                try:
                    await interaction.edit_original_response(content=content, view=view)
                except discord.HTTPException:
                    await interaction.followup.send(content=content, view=view, ephemeral=True)
            else:
                try:
                    await interaction.response.edit_message(content=content, view=view)
                except discord.HTTPException:
                    await interaction.response.send_message(content=content, view=view, ephemeral=True)
        except Exception as e:
            log.error(f"refresh_gui 처리 중 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def _notify_error(self, interaction: discord.Interaction) -> None:
        """어떤 단계에서든 예외가 발생하면 상호작용이 무응답 상태로 남아
        "상호작용 실패" 로 보이지 않도록, 항상 사용자에게 최소한의 응답을 보냅니다."""
        try:
            if interaction.response.is_done():
                await interaction.followup.send("⚠️ 처리 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)
            else:
                await interaction.response.send_message("⚠️ 처리 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)
        except discord.HTTPException:
            pass

    @app_commands.command(name="rpg", description="RPG 게임 패널을 엽니다")
    async def rpg(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        player = await self._player(interaction)
        if not player.state.get("starter_pack_given"):
            await ensure_starter_pack(player.user_id)
            player.state["starter_pack_given"] = True
            await save_player(player)
        content, view = await self._refresh_payload(player)
        await interaction.followup.send(content, view=view, ephemeral=True)
        self.active_panels[interaction.user.id] = interaction

    @app_commands.command(name="craft_list", description="제작 가능한 아이템 목록을 확인합니다")
    async def craft_list_cmd(self, interaction: discord.Interaction) -> None:
        content = render_craft_list()
        await interaction.response.send_message(content, ephemeral=True)

    @app_commands.command(name="dev_tp", description="[개발자] 특정 유저의 위치로 이동합니다")
    @app_commands.describe(user="이동할 대상 유저")
    async def dev_tp(self, interaction: discord.Interaction, user: discord.User) -> None:
        if interaction.user.id not in settings.dev_ids:
            await interaction.response.send_message(f"❌ 개발자 권한이 없습니다. (내 ID: {interaction.user.id})", ephemeral=True)
            return
        player = await self._player(interaction)
        target = await get_player(user.id)
        if not target:
            await interaction.response.send_message("❌ 대상을 찾을 수 없습니다.", ephemeral=True)
            return
        player.x, player.y = target.x, target.y
        await save_player(player)
        await interaction.response.send_message(f"📍 {user.display_name}에게 이동했습니다.", ephemeral=True)

    @app_commands.command(name="dev_item", description="[개발자] 아이템을 지급합니다")
    async def dev_item(self, interaction: discord.Interaction, user: discord.User, item_code: str, amount: int = 1) -> None:
        if interaction.user.id not in settings.dev_ids:
            await interaction.response.send_message(f"❌ 개발자 권한이 없습니다. (내 ID: {interaction.user.id})", ephemeral=True)
            return
        if item_code not in ITEM_CATALOG:
            await interaction.response.send_message(f"❌ 존재하지 않는 아이템 코드입니다: `{item_code}`", ephemeral=True)
            return
        inv_id = await add_item(user.id, item_code, amount)
        await interaction.response.send_message(f"🎁 {user.display_name}에게 {ITEM_CATALOG[item_code].name} x{amount} 지급 완료! (ID: {inv_id})", ephemeral=True)

    @app_commands.command(name="dev_items_list", description="[개발자] 개발자 전용 아이템 목록 확인")
    async def dev_items_list(self, interaction: discord.Interaction) -> None:
        if interaction.user.id not in settings.dev_ids:
            await interaction.response.send_message("❌ 개발자 권한이 없습니다.", ephemeral=True)
            return
        info = "🗡 암시장 검: `dev_blackmarket_sword` (ATK 10000)\n🛡 개발자의 갑옷: `dev_inf_armor` (HP INF)"
        await interaction.response.send_message(info, ephemeral=True)

    @app_commands.command(name="dev_id", description="[개발자] 본인의 디스코드 ID를 확인합니다")
    async def dev_id_cmd(self, interaction: discord.Interaction) -> None:
        is_dev = interaction.user.id in settings.dev_ids
        await interaction.response.send_message(f"🆔 본인 ID: `{interaction.user.id}`\n👑 개발자 등록 여부: {'✅ 예' if is_dev else '❌ 아니오'}", ephemeral=True)

    # ── 버튼 핸들러들 (모두 View 콜백에서 이미 _safe_ack() 로 defer 된 상태) ──
    async def handle_move(self, interaction: discord.Interaction, direction: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            player = await self._player(interaction)
            moved, msg, _ = await try_move(player, direction)
            note = msg
            nearby = await get_players_in_area(player.x, player.y, radius=0)
            enemies = [p for p in nearby if p.user_id != player.user_id]
            if enemies and not player.state.get("battle_id"):
                enemy = enemies[0]
                await start_pvp_battle(player, enemy)
                note = f"⚔ {enemy.username}와 전투 시작!"
            elif moved and should_encounter(player.x, player.y, player.level) and not player.state.get("battle_id"):
                monster = pick_monster(player.x, player.y, player.level)
                await start_monster_battle(player, monster)
                note = f"👾 {monster['name']} 조우!"
            await save_player(player)
            await self.refresh_gui(interaction, note=note)
        except Exception as e:
            log.error(f"handle_move 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def handle_battle_action(self, interaction: discord.Interaction, action: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            player = await self._player(interaction)
            result = await perform_battle_action(player, action)
            await save_player(player)
            note = result.message + ("\n" + "\n".join(result.reward_lines) if result.reward_lines else "")
            await self.refresh_gui(interaction, note=note)
        except Exception as e:
            log.error(f"handle_battle_action 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def show_inventory(self, interaction: discord.Interaction) -> None:
        try:
            player = await self._player(interaction)
            items = await list_inventory(player.user_id)
            content = await render_inventory(items, player.equipment)
            await interaction.response.send_message(content, ephemeral=True)
        except Exception as e:
            log.error(f"show_inventory 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def handle_equip_submit(self, interaction: discord.Interaction, user_id: int, query: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            player = await self._player(interaction)
            ok, msg = await equip_item_by_query(player, query)
            await save_player(player)
            await self.refresh_gui(interaction, note=msg)
        except Exception as e:
            log.error(f"handle_equip_submit 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def handle_craft_submit(self, interaction: discord.Interaction, user_id: int, item_code: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            ok, msg = await craft_item(user_id, item_code)
            await self.refresh_gui(interaction, note=msg)
        except Exception as e:
            log.error(f"handle_craft_submit 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def show_craft_list(self, interaction: discord.Interaction) -> None:
        content = render_craft_list()
        await interaction.response.send_message(content, ephemeral=True)

    async def handle_enchant_submit(self, interaction: discord.Interaction, user_id: int, query: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            ok, msg = await enchant_item(user_id, query)
            await self.refresh_gui(interaction, note=msg)
        except Exception as e:
            log.error(f"handle_enchant_submit 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def handle_gacha(self, interaction: discord.Interaction) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            player = await self._player(interaction)
            ok, msg = await gacha_item(player.user_id)
            await self.refresh_gui(interaction, note=msg)
        except Exception as e:
            log.error(f"handle_gacha 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def handle_chat_submit(self, interaction: discord.Interaction, user_id: int, message: str) -> None:
        try:
            self.active_panels[interaction.user.id] = interaction
            cleaned = message.strip()
            if cleaned:
                await execute(
                    "INSERT INTO chat_messages (user_id, username, guild_id, message) VALUES (?, ?, ?, ?)",
                    (user_id, interaction.user.display_name, interaction.guild_id, cleaned),
                )
            await self.refresh_gui(interaction, note="✅ 글로벌 채팅 전송 완료!")
        except Exception as e:
            log.error(f"handle_chat_submit 오류: {e}", exc_info=e)
            await self._notify_error(interaction)

    async def toggle_setting(self, interaction: discord.Interaction, user_id: int, key: str) -> None:
        try:
            player = await self._player(interaction)
            current = player.state.get(key, True)
            player.state[key] = not current
            await save_player(player)
            await interaction.response.send_message(f"⚙️ {key} 설정이 {'켜짐' if not current else '꺼짐'}으로 변경되었습니다.", ephemeral=True)
        except Exception as e:
            log.error(f"toggle_setting 오류: {e}", exc_info=e)
            await self._notify_error(interaction)


class EconomyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="market", description="글로벌 거래소 목록을 확인합니다")
    async def market(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        rows = await fetch_all(
            "SELECT id, seller_name, item_name, price, qty FROM trade_listings WHERE status = 'open' ORDER BY id DESC LIMIT 20"
        )
        if not rows:
            await interaction.followup.send("열린 거래가 없습니다.", ephemeral=True)
            return
        lines = ["```", "╔════ 글로벌 거래소 ════╗"]
        for row in rows:
            lines.append(f"#{row['id']} {row['item_name']} x{row['qty']} / {row['price']}코인 / 판매자 {row['seller_name']}")
        lines.append("╚═══════════════════════╝")
        lines.append("```")
        lines.append("구매는 /buy listing_id 로 가능합니다.")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(name="buy", description="글로벌 거래소에서 구매합니다")
    async def buy(self, interaction: discord.Interaction, listing_id: int) -> None:
        await interaction.response.defer(ephemeral=True)
        buyer = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        row = await fetch_one("SELECT * FROM trade_listings WHERE id = ? AND status = 'open'", (listing_id,))
        if not row:
            await interaction.followup.send("거래를 찾을 수 없습니다.", ephemeral=True)
            return
        if buyer.coins < row["price"]:
            await interaction.followup.send("코인이 부족합니다.", ephemeral=True)
            return
        buyer.coins -= row["price"]
        await save_player(buyer)
        seller_fee = row["price"] * (100 - settings.trade_tax_percent) // 100
        await execute("UPDATE players SET coins = coins + ? WHERE user_id = ?", (seller_fee, row["seller_id"]))
        await add_item(buyer.user_id, row["item_code"], row["qty"])
        await execute("UPDATE trade_listings SET status = 'sold' WHERE id = ?", (listing_id,))
        await interaction.followup.send(
            f"구매 완료: {row['item_name']} x{row['qty']} / 세금 {settings.trade_tax_percent}% 적용",
            ephemeral=True,
        )

    @app_commands.command(name="wallet", description="현재 보유 재화를 확인합니다")
    async def wallet(self, interaction: discord.Interaction) -> None:
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        await interaction.response.send_message(f"💰 코인 {player.coins} / 💎 젬 {player.gems}", ephemeral=True)


class SocialCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="chatlog", description="GUI 글로벌 채팅 로그를 확인합니다")
    async def chatlog(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        room_key = player.state.get("chat_room", "global-lobby")
        rows = await fetch_all(
            "SELECT username, message, created_at FROM chat_messages WHERE room_key = ? ORDER BY id DESC LIMIT 15",
            (room_key,),
        )
        if not rows:
            await interaction.followup.send("채팅 로그가 없습니다.", ephemeral=True)
            return
        lines = ["```", f"╔════ {room_key} 채팅 로그 ════╗"]
        for row in rows[::-1]:
            lines.append(f"[{row['created_at'][11:16]}] {row['username']}: {row['message']}")
        lines.append("╚══════════════════════════════╝")
        lines.append("```")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(name="guild_create", description="길드를 생성합니다")
    async def guild_create(self, interaction: discord.Interaction, guild_name: str) -> None:
        await interaction.response.defer(ephemeral=True)
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        exists = await fetch_one("SELECT guild_name FROM guilds WHERE guild_name = ?", (guild_name,))
        if exists:
            await interaction.followup.send("이미 존재하는 길드명입니다.", ephemeral=True)
            return
        await execute("INSERT INTO guilds (guild_name, owner_id) VALUES (?, ?)", (guild_name, player.user_id))
        player.guild_name = guild_name
        await save_player(player)
        await interaction.followup.send(f"🏰 길드 생성 완료: {guild_name}", ephemeral=True)

    @app_commands.command(name="guild_join", description="길드에 가입합니다")
    async def guild_join(self, interaction: discord.Interaction, guild_name: str) -> None:
        await interaction.response.defer(ephemeral=True)
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        exists = await fetch_one("SELECT guild_name FROM guilds WHERE guild_name = ?", (guild_name,))
        if not exists:
            await interaction.followup.send("길드를 찾을 수 없습니다.", ephemeral=True)
            return
        player.guild_name = guild_name
        await save_player(player)
        await interaction.followup.send(f"🏰 {guild_name} 길드 가입 완료", ephemeral=True)

    @app_commands.command(name="whohere", description="현재 좌표의 다른 플레이어를 확인합니다")
    async def whohere(self, interaction: discord.Interaction) -> None:
        await interaction.response.defer(ephemeral=True)
        player = await ensure_player(interaction.user.id, interaction.user.display_name, interaction.guild_id)
        rows = await fetch_all(
            "SELECT username, level, guild_name FROM players WHERE x = ? AND y = ? ORDER BY username ASC",
            (player.x, player.y),
        )
        lines = [f"🧭 현재 좌표 ({player.x},{player.y}) 플레이어"]
        for row in rows:
            lines.append(f"- {row['username']} / Lv.{row['level']} / 길드 {row['guild_name'] or '없음'}")
        await interaction.followup.send("\n".join(lines), ephemeral=True)


class WormCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.sessions: dict[int, dict] = {}

    @app_commands.command(name="worm", description="지렁이 게임 GUI를 엽니다")
    async def worm(self, interaction: discord.Interaction) -> None:
        state = WormState()
        self.sessions[interaction.user.id] = worm_to_dict(state)
        await interaction.response.send_message(render_worm(state), view=WormView(self, interaction.user.id), ephemeral=True)

    async def handle_worm_move(self, interaction: discord.Interaction, direction: str) -> None:
        try:
            await _safe_ack(interaction)
            data = self.sessions.get(interaction.user.id) or worm_to_dict(WormState())
            state = worm_from_dict(data)
            worm_step(state, direction)
            self.sessions[interaction.user.id] = worm_to_dict(state)
            if not state.alive:
                row = await fetch_one("SELECT best_score, total_games FROM worm_scores WHERE user_id = ?", (interaction.user.id,))
                best = max(state.score, row["best_score"] if row else 0)
                total = (row["total_games"] if row else 0) + 1
                await execute(
                    "INSERT OR REPLACE INTO worm_scores (user_id, best_score, total_games, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
                    (interaction.user.id, best, total),
                )
            await interaction.edit_original_response(content=render_worm(state), view=WormView(self, interaction.user.id))
        except Exception as e:
            log.error(f"handle_worm_move 오류: {e}", exc_info=e)

    async def restart_worm(self, interaction: discord.Interaction) -> None:
        await _safe_ack(interaction)
        state = WormState()
        self.sessions[interaction.user.id] = worm_to_dict(state)
        await interaction.edit_original_response(content=render_worm(state), view=WormView(self, interaction.user.id))


# ══════════════════════════════════════════════════════════════════════════
# 12. 봇 본체
# ══════════════════════════════════════════════════════════════════════════
class DiscordRPGBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self) -> None:
        await init_db()

        for cog in (RPGCog(self), EconomyCog(self), SocialCog(self), WormCog(self)):
            try:
                await self.add_cog(cog)
                log.info(f"코그 로드 완료: {cog.__class__.__name__}")
            except Exception as e:
                log.error(f"코그 로드 실패 {cog.__class__.__name__}: {e}", exc_info=e)

        try:
            synced = await self.tree.sync()
            log.info(f"전역 슬래시 커맨드 {len(synced)}개 동기화 완료")
        except Exception as e:
            log.error(f"슬래시 커맨드 동기화 중 오류 발생: {e}", exc_info=e)

    async def on_ready(self) -> None:
        log.info("봇 로그인 완료: %s (%s)", self.user, self.user.id if self.user else "unknown")
        log.info("명령어가 안 보일 경우 디스코드 앱을 껐다 켜보세요(Ctrl+R).")

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return

        if message.content == "!sync" and message.author.id in settings.dev_ids:
            try:
                synced = await self.tree.sync()
                await message.channel.send(f"✅ {len(synced)}개의 슬래시 커맨드를 동기화했습니다!")
            except Exception as e:
                await message.channel.send(f"❌ 동기화 실패: {e}")

        await self.process_commands(message)


bot = DiscordRPGBot()


# [FIX] 슬래시 커맨드(app_commands) 콜백 내부에서 처리되지 않은 예외가 발생하면
# 기존에는 아무 응답도 가지 않아 사용자에게 "상호작용 실패" 로 보였습니다.
# 전역 에러 핸들러를 등록해 항상 최소한의 안내 메시지를 보내도록 합니다.
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError) -> None:
    log.error(f"슬래시 커맨드 처리 중 오류 발생: {error}", exc_info=error)
    try:
        if interaction.response.is_done():
            await interaction.followup.send("⚠️ 명령어 처리 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ 명령어 처리 중 오류가 발생했습니다. 다시 시도해주세요.", ephemeral=True)
    except discord.HTTPException:
        pass


async def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token or token == "PUT_YOUR_DISCORD_BOT_TOKEN_HERE":
        log.error("❌ DISCORD_TOKEN이 설정되지 않았습니다. .env 파일을 확인해주세요.")
        return

    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
