"""Atrapa ESP32 + symulator falownika Deye na RS485.

Uruchamia prawdziwy main.py na CPythonie bez żadnego sprzętu, podstawiając
machine/network/umqtt/uasyncio i UART, który odpowiada jak slave Modbus RTU.
Sprawdza: budowanie i CRC ramek, odczyt bloków, dekodowanie u16/s16/u32,
topiki i payloady MQTT, obsługę braku odpowiedzi, kolejkę publikacji, zegar
falownika (odczyt, dryf, korekta jednym 0x10) i regułę czasu letniego.

Uruchomienie:  python tests/test_mock_inverter.py
"""

import asyncio
import binascii
import json
import struct
import sys
import time as real_time
import types

import os
DEYEMQTT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------- fake time
_t0 = real_time.monotonic()

fake_time = types.ModuleType("time")
fake_time.time = lambda: int(real_time.time() - 946_684_800)
fake_time.ticks_ms = lambda: int((real_time.monotonic() - _t0) * 1000)
fake_time.ticks_diff = lambda a, b: a - b
fake_time.ticks_add = lambda t, d: t + d
fake_time.sleep = real_time.sleep
fake_time.sleep_ms = lambda ms: real_time.sleep(ms / 1000)

# MicroPython: localtime/mktime operuja na epoce od 2000-01-01 i bez stref
# czasowych (czyli faktycznie UTC). weekday: 0=poniedzialek, 6=niedziela.
import calendar
MP_EPOCH = 946_684_800
fake_time.localtime = lambda t=None: real_time.gmtime(
    (fake_time.time() if t is None else t) + MP_EPOCH)
fake_time.mktime = lambda t: calendar.timegm(
    (t[0], t[1], t[2], t[3], t[4], t[5], 0, 1, 0)) - MP_EPOCH

# ------------------------------------------------------------- fake machine
def crc16(data):
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


# Rejestry symulowanego falownika Deye 12k
DEYE_REGS = {
    514: 123,          # 12.3 kWh
    515: 456,          # 45.6 kWh
    516: 0xFFFF, 517: 0x0001,   # total charge = 131071 -> 13107.1 kWh
    518: 5000,  519: 0x0000,    # total discharge = 500.0 kWh
    527: 30000, 528: 0x0002,    # total load = 161072 -> 16107.2 kWh
    588: 74,           # SOC 74 %
    589: 0,
    590: 0xFE70,       # -400 W (ladowanie)
    650: 1200, 651: 800, 652: 0xFC18,   # L3 = -1000 W
    672: 2500, 673: 1800,
}

# Zegar falownika (62-64) ustawiony 5 minut ZA czasem lokalnym - ma wywolac korekte
CLOCK_DRIFT_S = -300
_local_now = real_time.gmtime(real_time.time() + 7200)      # UTC+2 (lato w PL)
_drifted = real_time.gmtime(real_time.time() + 7200 + CLOCK_DRIFT_S)
DEYE_REGS[62] = ((_drifted.tm_year - 2000) << 8) | _drifted.tm_mon
DEYE_REGS[63] = (_drifted.tm_mday << 8) | _drifted.tm_hour
DEYE_REGS[64] = (_drifted.tm_min << 8) | _drifted.tm_sec

# Blok, ktory ma nie odpowiadac - test sciezki bledu i ponowienia
DEAD_BLOCK_START = 650

stats = {"requests": [], "dead_hits": 0, "writes": []}


class FakeUART:
    """UART, ktory zachowuje sie jak slave Modbus RTU pod adresem 1."""

    def __init__(self, *args, **kwargs):
        self._rx = bytearray()

    def any(self):
        return len(self._rx)

    def read(self, n=None):
        if not self._rx:
            return None
        out = bytes(self._rx)
        self._rx = bytearray()
        return out

    def flush(self):
        pass

    def write(self, frame):
        frame = bytes(frame)
        assert crc16(frame[:-2]) == struct.unpack("<H", frame[-2:])[0], "master wyslal zle CRC"
        slave, fc = frame[0], frame[1]
        assert slave == 1, f"zly adres slave {slave}"

        if fc == 3:
            start, count = struct.unpack(">HH", frame[2:6])
            stats["requests"].append((start, count))
            if start == DEAD_BLOCK_START:
                stats["dead_hits"] += 1
                return                      # cisza - symulacja braku odpowiedzi
            body = bytearray([slave, fc, 2 * count])
            for addr in range(start, start + count):
                body += struct.pack(">H", DEYE_REGS.get(addr, 0))
            self._rx += body + struct.pack("<H", crc16(body))

        elif fc == 6:
            reg, val = struct.unpack(">HH", frame[2:6])
            DEYE_REGS[reg] = val
            body = bytearray(frame[:6])
            self._rx += body + struct.pack("<H", crc16(body))

        elif fc == 0x10:
            start, count, nbytes = struct.unpack(">HHB", frame[2:7])
            assert nbytes == 2 * count, "zly licznik bajtow w 0x10"
            for i in range(count):
                DEYE_REGS[start + i] = struct.unpack(">H", frame[7 + 2 * i:9 + 2 * i])[0]
            stats["writes"].append((start, count))
            body = bytearray(frame[:6])         # echo: adres startowy + liczba rejestrow
            self._rx += body + struct.pack("<H", crc16(body))
        return len(frame)


class FakePin:
    OUT = 1
    IN = 0
    def __init__(self, *a, **kw): pass
    def value(self, *a): return 0


class FakeWDT:
    instances = []
    def __init__(self, timeout=0):
        self.timeout = timeout
        self.feeds = 0
        FakeWDT.instances.append(self)
    def feed(self):
        self.feeds += 1


fake_machine = types.ModuleType("machine")
fake_machine.UART = FakeUART
fake_machine.Pin = FakePin
fake_machine.WDT = FakeWDT
fake_machine.unique_id = lambda: b"\xaa\xbb\xcc\xdd\xee\xff"
fake_machine.reset_cause = lambda: 1
fake_machine.PWRON_RESET = 1
fake_machine.HARD_RESET = 2
fake_machine.WDT_RESET = 3
fake_machine.DEEPSLEEP_RESET = 4
fake_machine.SOFT_RESET = 5
fake_machine.reset = lambda: (_ for _ in ()).throw(SystemExit("machine.reset()"))

# ------------------------------------------------------------- fake network
class FakeWLAN:
    STA_IF = 0
    def __init__(self, *a): self._on = False
    def active(self, v=None): self._on = v if v is not None else self._on; return self._on
    def isconnected(self): return self._on
    def connect(self, *a): self._on = True
    def disconnect(self): pass
    def ifconfig(self): return ("192.168.1.42", "255.255.255.0", "192.168.1.1", "8.8.8.8")
    def status(self, what=None): return -55

fake_network = types.ModuleType("network")
fake_network.WLAN = FakeWLAN
fake_network.STA_IF = 0

# ---------------------------------------------------------------- fake mqtt
published = []

# Wiadomosci retained lezace na brokerze - dostarczane raz, po subscribe
RETAINED = {}

class FakeMQTTClient:
    def __init__(self, cid, broker, port=1883, keepalive=0):
        self.subs = []
        self._pending = []
    def set_callback(self, cb): self.cb = cb
    def connect(self): pass
    def disconnect(self): pass
    def subscribe(self, topic):
        name = topic.decode() if isinstance(topic, bytes) else topic
        self.subs.append(name)
        if name in RETAINED:
            self._pending.append((topic, json.dumps(RETAINED.pop(name)).encode()))
    def check_msg(self):
        while self._pending:
            t, p = self._pending.pop(0)
            self.cb(t, p)
    def publish(self, topic, payload, retain=False, qos=0):
        published.append((topic.decode(), json.loads(payload.decode()), retain))

umqtt = types.ModuleType("umqtt")
umqtt_simple = types.ModuleType("umqtt.simple")
umqtt_simple.MQTTClient = FakeMQTTClient
umqtt.simple = umqtt_simple

# --------------------------------------------------------------- fake resztа
class FakeNeoPixel:
    def __init__(self, pin, n): self._buf = [(0, 0, 0)] * n
    def __setitem__(self, i, v): self._buf[i] = v
    def __getitem__(self, i): return self._buf[i]
    def write(self): pass

fake_neopixel = types.ModuleType("neopixel")
fake_neopixel.NeoPixel = FakeNeoPixel

fake_uftpd = types.ModuleType("uftpd")
async def _ftp():
    while True:
        await asyncio.sleep(5)
fake_uftpd.ftp = _ftp

# MicroPython ma gc.mem_free/mem_alloc, CPython nie
import gc as _gc
_gc.mem_free = lambda: 4_000_000
_gc.mem_alloc = lambda: 200_000

fake_ntptime = types.ModuleType("ntptime")
fake_ntptime.host = ""
def _settime(): pass
fake_ntptime.settime = _settime

# uasyncio -> asyncio, ale run() z limitem czasu, bo main() jest nieskonczone
fake_uasyncio = types.ModuleType("uasyncio")
for _name in dir(asyncio):
    setattr(fake_uasyncio, _name, getattr(asyncio, _name))
fake_uasyncio.sleep_ms = lambda ms: asyncio.sleep(ms / 1000)

RUN_SECONDS = 12

# Petle trzeba utworzyc PRZED importem modulow, bo Python 3.8 wiaze
# asyncio.Lock z aktualna petla juz w konstruktorze (MicroPython nie wiaze
# jej wcale - ma jeden globalny scheduler, wiec na plytce to nieistotne).
LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(LOOP)


def _run(coro):
    async def wrapper():
        task = asyncio.ensure_future(coro)
        await asyncio.sleep(RUN_SECONDS)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, SystemExit):
            pass
    LOOP.run_until_complete(wrapper())

fake_uasyncio.run = _run
fake_uasyncio.new_event_loop = asyncio.new_event_loop

# --------------------------------------------------------- cfg z krotszymi czasami
sys.path.insert(0, DEYEMQTT)
for mod, obj in (("time", fake_time), ("machine", fake_machine), ("network", fake_network),
                 ("umqtt", umqtt), ("umqtt.simple", umqtt_simple), ("neopixel", fake_neopixel),
                 ("uftpd", fake_uftpd), ("ntptime", fake_ntptime), ("uasyncio", fake_uasyncio),
                 ("ubinascii", binascii), ("ujson", json), ("ustruct", struct)):
    sys.modules[mod] = obj

try:
    import cfg
except ImportError:
    # Swiezy klon nie ma cfg.py (jest w .gitignore) - do testu wystarczy szablon
    import importlib.util
    _spec = importlib.util.spec_from_file_location(
        "cfg", os.path.join(DEYEMQTT, "cfg.example.py"))
    cfg = importlib.util.module_from_spec(_spec)
    sys.modules["cfg"] = cfg
    _spec.loader.exec_module(cfg)
    print("(uzywam cfg.example.py - brak lokalnego cfg.py)")

cfg.POLL_TICK_S = 1
cfg.BLOCK_GAP_MS = 20
cfg.BLOCK_RETRY_S = 3
cfg.DIAG_INTERVAL_S = 4
cfg.DISCOVERY_INTERVAL_S = 5
cfg.WDT_START_DELAY_S = 2
cfg.TASK_DEADLINE_MS = 60_000
cfg.NTP_ENABLED = True
cfg.FTP_ENABLED = True
cfg.MODBUS_WRITE_ENABLED = True
cfg.MODBUS_WRITE_ALLOWED = (145,)
cfg.CLOCK_CHECK_INTERVAL_S = 3
cfg.CLOCK_SYNC_ENABLED = True
cfg.CLOCK_SYNC_INTERVAL_S = 1
cfg.CLOCK_MAX_DRIFT_S = 60
cfg.RESET_COUNTER_SYNC_S = 3
cfg.RESET_COUNTER_WAIT_MQTT_S = 8
cfg.RESET_COUNTER_PUBLISH_S = 5
# tryb domyslny: liczniki tylko na brokerze, bez dotykania flasha
cfg.RESET_COUNTER_PERSIST_FLASH = False
cfg.RESET_COUNTER_FILE = os.path.join(
    os.environ.get("TEMP", "."), "deyemqtt_resets_test.json")
if os.path.exists(cfg.RESET_COUNTER_FILE):
    os.remove(cfg.RESET_COUNTER_FILE)

# historia lezaca na brokerze - ma zostac odtworzona, bo plik zniknal
RETAINED["cave/deye/diag/deye_ddeeff/resets"] = {
    "device": "deye_ddeeff", "total": 57,
    "by_cause": {"power_on": 3, "watchdog": 54},
    "by_reason": {"watchdog_stale": 54},
    "uptime": 302, "max_uptime": 900, "since": 1780000000,
}
# skroc okresy blokow, zeby test zobaczyl tez energie
import registers
for b in registers.BLOCKS:
    b["period"] = 6 if b["period"] > 100 else b["period"]

print("=== uruchamiam main.py na atrapie ===")
import main  # noqa: E402  - to tutaj startuje asyncio.run(main())

# ------------------------------------------------------------------- wyniki
print("\n=== zapytania Modbus (start, liczba rejestrow) ===")
from collections import Counter
for req, n in sorted(Counter(stats["requests"]).items()):
    print(f"  {req} x{n}")

print("\n=== opublikowane topiki (ostatnia wartosc) ===")
last = {}
for topic, payload, retain in published:
    last[topic] = (payload, retain)
for topic in sorted(last):
    payload, retain = last[topic]
    if topic.endswith("/diag/" + cfg.DEVID) or "diag" in topic:
        keys = {k: payload[k] for k in ("uptime", "mem_free", "blocks_ok", "blocks_failed",
                                        "modbus_fail_streak", "queue_len", "modbus")
                if k in payload}
        print(f"  {topic}\n      {keys}")
    else:
        print(f"  {topic:45s} retain={retain} {payload}")

# ------------------------------------------------------------------ asercje
errors = []

def check(cond, msg):
    if not cond:
        errors.append(msg)

vals = {t: p["value"] for t, (p, _) in last.items() if "value" in p}
check(vals.get("cave/deye/params/battery_soc") == 74, "battery_soc != 74")
check(vals.get("cave/deye/params/battery_power") == -400, f"battery_power = {vals.get('cave/deye/params/battery_power')} (oczekiwano -400, s16)")
check(vals.get("cave/deye/params/battery_charge_total") == 13107.1, f"battery_charge_total = {vals.get('cave/deye/params/battery_charge_total')} (oczekiwano 13107.1, u32 low-word-first)")
check(vals.get("cave/deye/params/load_energy_total") == 16107.2, f"load_energy_total = {vals.get('cave/deye/params/load_energy_total')}")
check(vals.get("cave/deye/params/pv1_power") == 2500, "pv1_power != 2500")
check(vals.get("cave/deye/params/battery_charge_today") == 12.3, "battery_charge_today != 12.3")

# martwy blok 650 nie moze sie opublikowac ani zablokowac reszty
check("cave/deye/params/load_power_l1" not in vals, "martwy blok 650 opublikowal wartosc")
check(stats["dead_hits"] >= 2, f"martwy blok odpytany tylko {stats['dead_hits']} raz - ponowienie nie dziala")

# stare topiki dla zgodnosci w tyl
check("cave/deye/params/regs/588" in last, "brak starego topiku regs/588")
check("cave/deye/params/regs/590" in last, "brak starego topiku regs/590")
check("cave/deye/params/regs/516" not in last, "regs/516 nie powinien istniec (pole u32)")

# jedno zapytanie na blok, nie po jednym rejestrze
reqs = set(stats["requests"])
check((514, 6) in reqs, f"blok 514 nie czytany jako 6 rejestrow: {reqs}")
check((527, 2) in reqs, "blok 527 nie czytany jako 2 rejestry")
check((588, 3) in reqs, "blok 588 nie czytany jako 3 rejestry")
check(not any(c == 1 for _, c in reqs), f"sa zapytania o pojedyncze rejestry: {reqs}")

# diagnostyka i watchdog
diag = [p for t, (p, _) in last.items() if t == "%s/%s" % (cfg.TOPIC_DIAG, cfg.DEVID)]
check(bool(diag), "brak publikacji diagnostycznej")
if diag:
    d = diag[0]
    check(d["blocks_failed"] > 0, "diag nie policzyl nieudanego bloku")
    check(d["modbus"]["timeout"] > 0, "diag nie policzyl timeoutu Modbusa")
    check(d["queue_len"] < 20, f"kolejka rosnie: {d['queue_len']}")
check(FakeWDT.instances and FakeWDT.instances[0].feeds > 0, "watchdog nie byl karmiony")

# ------------------------------------------------------- licznik resetow
print()
print("=== licznik resetow ===")
resets = [p for t, p, _ in published if t.endswith("/resets")]
check(bool(resets), "brak publikacji licznika resetow")
if resets:
    r = resets[-1]
    print("  ", r)
    # historia z brokera (57) + ten start = 58
    check(r.get("total") == 58, f"total = {r.get('total')}, oczekiwano 58 "
                                f"(odtworzenie z brokera + ten start)")
    check(r.get("by_cause", {}).get("power_on") == 4,
          f"by_cause.power_on = {r.get('by_cause', {}).get('power_on')}, oczekiwano 4")
    check(r.get("by_cause", {}).get("watchdog") == 54,
          "historia watchdog z brokera zostala zgubiona")
    check(r.get("last_cause") == "power_on", f"last_cause = {r.get('last_cause')}")
    # dlugosc poprzedniego biegu wziete z retained "uptime" - dziala tez po zaniku zasilania
    check(r.get("prev_uptime") == 302, f"prev_uptime = {r.get('prev_uptime')}, oczekiwano 302")
    check(r.get("max_uptime") == 900, f"max_uptime = {r.get('max_uptime')}, oczekiwano 900")
    check("uptime" in r, "brak biezacego uptime w payloadzie licznika")

# tryb domyslny NIE moze tknac flasha
check(not os.path.exists(cfg.RESET_COUNTER_FILE),
      "przy RESET_COUNTER_PERSIST_FLASH=False powstal plik na flashu!")
print("   flash nietknięty (PERSIST_FLASH=False)")

# powod restartu musi polecieic retained na brokera, i to NATYCHMIAST -
# po machine.reset() kolejka nie mialaby okazji sie oproznic
before = len(published)
main.mark_reset_reason("mqtt_dead")
nowe = [p for t, p, r in published[before:] if t.endswith("/resets")]
check(bool(nowe), "powod restartu nie zostal opublikowany poza kolejka")
if nowe:
    check(nowe[-1].get("pending_reason") == "mqtt_dead",
          f"pending_reason = {nowe[-1].get('pending_reason')}")
    check(nowe[-1].get("pending_uptime") is not None, "brak uptime w znaczniku intencji")
    retained_flag = [r for t, p, r in published[before:] if t.endswith("/resets")][-1]
    check(retained_flag is True, "znacznik powodu opublikowany bez retain - nie przezyje resetu")
    print("   powod odlozony na brokerze:", nowe[-1].get("pending_reason"),
          nowe[-1].get("pending_uptime"), "s, retain =", retained_flag)

# bez brokera powod przepada - to musi byc obsluzone, nie wywalic sie
_saved_client = main.mqtt_client
main.mqtt_client = None
main.mark_reset_reason("modbus_dead")        # nie moze rzucic wyjatku
main.mqtt_client = _saved_client
print("   brak brokera przy odkladaniu powodu: obsluzone")

# --- drugi tryb: zapis na flash jako zrodlo prawdy ---
print()
print("=== licznik resetow: tryb z flashem ===")
main.RESET_COUNTER_PERSIST_FLASH = True
if os.path.exists(cfg.RESET_COUNTER_FILE):
    os.remove(cfg.RESET_COUNTER_FILE)
main.mark_reset_reason("watchdog_stale")
check(os.path.exists(cfg.RESET_COUNTER_FILE),
      "przy PERSIST_FLASH=True powod nie trafil na flash")
if os.path.exists(cfg.RESET_COUNTER_FILE):
    on_disk = json.load(open(cfg.RESET_COUNTER_FILE))
    print("   plik na flashu:", on_disk)
    check(on_disk.get("pending_reason") == "watchdog_stale",
          f"plik ma pending_reason {on_disk.get('pending_reason')}")
main.RESET_COUNTER_PERSIST_FLASH = False
os.remove(cfg.RESET_COUNTER_FILE)

# ------------------------------------------------- watchdog vs dlugie sny
# Regresja z pola: clock_task bil heartbeat raz, potem spal CLOCK_CHECK_INTERVAL_S
# = 3600 s. Po TASK_DEADLINE_MS (300 s) watchdog uznawal go za zawieszonego,
# przestawal karmic i resetowal CALKOWICIE ZDROWA plytke - reset co ~355 s.
print()
print("=== watchdog przy dlugim snie taska ===")

DIAG_TOPIC = "%s/%s" % (cfg.TOPIC_DIAG, cfg.DEVID)
diag_last = [p for t, p, _ in published if t == DIAG_TOPIC]
check(bool(diag_last) and "beats" in diag_last[-1],
      "diag nie raportuje wieku bicia taskow (pole 'beats')")
if diag_last and "beats" in diag_last[-1]:
    beats = diag_last[-1]["beats"]
    print("   wiek bicia:", beats)
    limit = cfg.TASK_DEADLINE_MS // 1000
    przeterminowane = {n: a for n, a in beats.items() if a > limit}
    check(not przeterminowane,
          f"taski z przeterminowanym biciem: {przeterminowane} (limit {limit} s)")
    check("clock" in beats, "clock_task nie raportuje bicia")

# sen na godzine NIE moze pozwolic biciu zwietrzec
main._beats.clear()
main.heartbeat("clock")
_t = LOOP.create_task(main.sleep_beating("clock", 3600, step=1))
LOOP.run_until_complete(asyncio.sleep(3.5))
_t.cancel()
age_ms = fake_time.ticks_diff(fake_time.ticks_ms(), main._beats["clock"])
print(f"   po 3.5 s snu na 3600 s bicie ma {age_ms} ms")
check(age_ms < 1500,
      f"bicie zwietrzalo w trakcie dlugiego snu ({age_ms} ms) - "
      f"watchdog zresetowalby zdrowa plytke")

# a prawdziwe zawieszenie taska musi byc nadal wykrywalne. Uzywamy nazwy,
# ktorej zaden dzialajacy task nie odswieza - taski z glownego biegu nadal zyja.
main.heartbeat("zawieszony_task")
LOOP.run_until_complete(asyncio.sleep(2))
hung = fake_time.ticks_diff(fake_time.ticks_ms(), main._beats["zawieszony_task"])
check(hung >= 1900, f"bicie nie starzeje sie ({hung} ms) - watchdog stracilby sens")
print(f"   task bez bicia po 2 s: {hung} ms (starzenie dziala)")

# ------------------------------------------------------------- zegar falownika
print("\n=== zegar falownika ===")
clock_msgs = [p for t, p, _ in published if t.endswith("/clock")]
for p in clock_msgs[-2:]:
    print("  ", p)

check(bool(clock_msgs), "brak publikacji zegara")
if clock_msgs:
    first = clock_msgs[0]
    check("inverter_time" in first, f"zegar nie zostal zdekodowany: {first}")
    # dekodowanie musi odtworzyc date, ktora wpisalismy do rejestrow
    expected = "%04d-%02d-%02d %02d:%02d:%02d" % (
        _drifted.tm_year, _drifted.tm_mon, _drifted.tm_mday,
        _drifted.tm_hour, _drifted.tm_min, _drifted.tm_sec)
    check(first.get("inverter_time") == expected,
          f"zle dekodowanie zegara: {first.get('inverter_time')} != {expected}")

    with_drift = [p for p in clock_msgs if "drift_s" in p]
    check(bool(with_drift), "nigdy nie policzono dryfu (brak NTP w tescie?)")
    if with_drift:
        d = with_drift[0]["drift_s"]
        lo, hi = CLOCK_DRIFT_S - RUN_SECONDS - 5, CLOCK_DRIFT_S + 5
        check(lo <= d <= hi, f"dryf {d} s, oczekiwano w zakresie {lo}..{hi} s")

    written = [p for p in clock_msgs if p.get("action") == "zapisane"]
    check(bool(written), f"korekta zegara nie zostala wykonana: "
                         f"{[p.get('action') for p in clock_msgs]}")
    if written:
        check(abs(written[0].get("drift_after_s", 999)) <= 3,
              f"po korekcie dryf {written[0].get('drift_after_s')} s")

# zapis zegara MUSI byc jedna transakcja 0x10 na trzy rejestry, nie trzema 0x06
check((62, 3) in stats["writes"], f"zegar nie zapisany jednym 0x10: {stats['writes']}")
# i rejestry falownika faktycznie sie zmienily
decoded_after = (2000 + (DEYE_REGS[62] >> 8), DEYE_REGS[62] & 0xFF,
                 DEYE_REGS[63] >> 8, DEYE_REGS[63] & 0xFF,
                 DEYE_REGS[64] >> 8, DEYE_REGS[64] & 0xFF)
print("   rejestry 62-64 po korekcie ->", "%04d-%02d-%02d %02d:%02d:%02d" % decoded_after)
check(decoded_after[3] == _local_now.tm_hour, "godzina w falowniku nie zostala poprawiona")

# regula czasu letniego (2026: 29 marca -> 25 pazdziernika)
print("\n=== czas letni (regula UE) ===")
for label, (y, mo, d, h, mi), want in (
        ("styczen",         (2026, 1, 15, 12, 0), 0),
        ("lipiec",          (2026, 7, 15, 12, 0), 3600),
        ("29.03 00:30 UTC", (2026, 3, 29, 0, 30), 0),
        ("29.03 01:30 UTC", (2026, 3, 29, 1, 30), 3600),
        ("25.10 00:30 UTC", (2026, 10, 25, 0, 30), 3600),
        ("25.10 01:30 UTC", (2026, 10, 25, 1, 30), 0)):
    epoch = fake_time.mktime((y, mo, d, h, mi, 0, 0, 0))
    got = main._dst_offset(epoch)
    print(f"   {label:18s} -> +{got} s")
    check(got == want, f"czas letni {label}: dostalem +{got}, oczekiwano +{want}")

# --------------------------------------------------- test zapisu do falownika
print("\n=== test zapisu (funkcja 0x06) ===")
main._write_requests.append({"name": cfg.DEVID, "reg": 145, "value": 1234})   # na whitelicie
main._write_requests.append({"name": cfg.DEVID, "reg": 999, "value": 1})      # NIE na whitelicie
LOOP.run_until_complete(main.handle_write_requests())

# Wyniki moga byc juz opublikowane przez zywy mqtt_manager albo jeszcze siedziec
# w kolejce - zbieramy z obu miejsc, inaczej test zalezy od wyscigu.
results = [p for t, p, _ in published if "write_result" in t]
while True:
    try:
        topic, payload, retain = main.publish_queue.get_nowait()
    except IndexError:
        break
    if "write_result" in topic.decode():
        results.append(json.loads(payload.decode()))
for r in results:
    print("  ", r)

check(DEYE_REGS.get(145) == 1234, f"zapis do 145 nie doszedl: {DEYE_REGS.get(145)}")
check(999 not in DEYE_REGS, "rejestr poza whitelista zostal zapisany!")
check(any(r.get("reg") == 999 and not r.get("ok") for r in results),
      "brak odrzucenia dla rejestru poza whitelista")

# komenda dla innego urzadzenia musi byc zignorowana
before = len(main._write_requests)
main.sub_callback(cfg.TOPIC_CMD_WRITE.encode(),
                  json.dumps({"name": "inne_urzadzenie", "reg": 145, "value": 7}).encode())
check(len(main._write_requests) == before, "komenda dla innego DEVID nie zostala zignorowana")

print("\n=== wynik ===")
if errors:
    for e in errors:
        print("  BLAD:", e)
    sys.exit(1)
print(f"  OK - wszystkie asercje przeszly ({len(last)} topikow, "
      f"{len(stats['requests'])} transakcji Modbus, "
      f"{FakeWDT.instances[0].feeds} feedow watchdoga)")
