# main.py - most Deye <-> MQTT dla ESP32-S3 (MicroPython)
# https://github.com/pradki/deye-modbus-mqtt
#
# Czyta rejestry falownika Deye po RS485 (Modbus RTU) i publikuje je na brokera
# MQTT. Wszystko na jednej pętli uasyncio, z automatycznym wznawianiem WiFi
# i MQTT oraz sprzętowym watchdogiem pilnującym, czy taski faktycznie żyją.
#
# Taski:
#   wifi_manager    - utrzymuje połączenie z siecią
#   mqtt_manager    - łączy się z brokerem, odbiera komendy, opróżnia kolejkę
#   deye_task       - odpytuje bloki rejestrów zgodnie z registers.BLOCKS
#   diag_task       - publikuje uptime, pamięć, statystyki Modbusa
#   discovery_task  - ogłasza obecność urządzenia w sieci
#   led_task        - sygnalizuje stan diodą RGB
#   ntp_task        - opcjonalna synchronizacja zegara płytki z NTP
#   clock_task      - pilnuje zegara falownika, opcjonalnie go koryguje
#   reset_counter_task - zlicza restarty urządzenia z podziałem na przyczynę
#   wdt_task        - karmi watchdoga, ale tylko gdy wszystkie taski odbijają
#   uftpd.ftp       - serwer FTP do podmiany plików bez odłączania płytki

import gc
import machine
import network
import time
import ubinascii
import ujson
import uasyncio as asyncio
from umqtt.simple import MQTTClient

import modbusrtu
import registers
import uftpd
from cfg import *
from registers import BLOCKS
from rgbled import RGBLed

# MicroPython liczy czas od 2000-01-01, świat od 1970-01-01
EPOCH_OFFSET = 946_684_800

wlan = network.WLAN(network.STA_IF)

mqtt_client = None
mqtt_connected = False
time_synced = False

_beats = {}                 # nazwa taska -> ticks_ms ostatniej aktywności
_led_flash = None           # (kolor, ticks_ms) - krótki błysk po transakcji Modbus
_write_requests = []        # żądania zapisu z MQTT, obsługiwane w deye_task

state = {
    "cycles": 0,            # przebiegi pętli odpytywania
    "blocks_ok": 0,
    "blocks_failed": 0,
    "modbus_fail_streak": 0,
    "modbus_last_ok": None,     # sekundy uptime
    "mqtt_last_ok": None,       # sekundy uptime
    "mqtt_reconnects": 0,
    "publish_errors": 0,
}

# Adresy pól zajmujących jeden rejestr - tylko one mogą dostać stary topik
# <TOPIC_BASE>/regs/<numer>, bo wartości 32-bitowe historycznie leciały tam
# jako osobne, surowe słowa low/high.
_single_reg_addrs = set()
for _block in BLOCKS:
    _off = 0
    for _field in _block["fields"]:
        if _field[0] and registers.REG_WIDTH[_field[1]] == 1:
            _single_reg_addrs.add(_block["start"] + _off)
        _off += registers.REG_WIDTH[_field[1]]
del _block, _field, _off


def _legacy_wanted(address):
    """Czy dla tego rejestru publikować też stary topik regs/<numer>.

    LEGACY_REG_TOPICS bywa listą adresów (publikuj tylko te), True (wszystkie
    jednorejestrowe pola, dawne zachowanie) albo False. Lista jest tu domyślna,
    bo mapa rejestrów zdążyła spuchnąć i publikowanie wszystkiego podwoiłoby
    liczbę wiadomości dla topików, których nikt nie czyta.
    """
    if address not in _single_reg_addrs:
        return False
    if LEGACY_REG_TOPICS is True:
        return True
    if not LEGACY_REG_TOPICS:
        return False
    return address in LEGACY_REG_TOPICS


# ---------- czas i uptime ----------
# time.ticks_ms() przekręca się po ~6 dniach, a to urządzenie ma chodzić
# miesiącami. Dlatego sekundy akumulujemy w zwykłym liczniku, doliczając
# różnicę przy każdym odczycie - wystarczy, że ktoś zajrzy tu częściej niż raz
# na kilka dni (robi to watchdog co 5 s).
_uptime_acc = 0
_uptime_mark = time.ticks_ms()


def uptime_s():
    global _uptime_acc, _uptime_mark
    delta = time.ticks_diff(time.ticks_ms(), _uptime_mark)
    if delta >= 1000:
        whole = delta // 1000
        _uptime_acc += whole
        _uptime_mark = time.ticks_add(_uptime_mark, whole * 1000)
    return _uptime_acc


def unix_time():
    """Czas uniksowy albo None, jeśli zegar nie był synchronizowany."""
    return time.time() + EPOCH_OFFSET if time_synced else None


# ---------- czas lokalny ----------
# Po NTP zegar płytki chodzi w UTC. Falownik chce czasu lokalnego, bo po nim
# planuje okna ładowania - godzina w drugą stronę psułaby harmonogram.
def _last_sunday(year, month, hour):
    """Ostatnia niedziela miesiąca o podanej godzinie UTC (sekundy od 2000)."""
    for day in range(31, 24, -1):
        try:
            t = time.mktime((year, month, day, hour, 0, 0, 0, 0))
        except Exception:
            continue
        if time.localtime(t)[6] == 6:       # 6 = niedziela
            return t
    return None


def _dst_offset(t):
    """Przesunięcie czasu letniego wg reguły unijnej: od ostatniej niedzieli
    marca 01:00 UTC do ostatniej niedzieli października 01:00 UTC."""
    if not TZ_DST_EU:
        return 0
    year = time.localtime(t)[0]
    start = _last_sunday(year, 3, 1)
    end = _last_sunday(year, 10, 1)
    if start is None or end is None:
        return 0
    return 3600 if start <= t < end else 0


def local_time_tuple():
    """Czas lokalny jako tuple time.localtime() albo None bez synchronizacji."""
    if not time_synced:
        return None
    t = time.time()
    return time.localtime(t + TZ_OFFSET_S + _dst_offset(t))


def heartbeat(name):
    """Task zgłasza, że żyje. Brak zgłoszeń wstrzymuje karmienie watchdoga."""
    _beats[name] = time.ticks_ms()


async def sleep_beating(name, seconds, step=30):
    """Długi sen pocięty na kawałki, z biciem heartbeatu po każdym.

    Watchdog ma krótki deadline (TASK_DEADLINE_MS), a część tasków śpi
    godzinami. Zwykły asyncio.sleep(3600) sprawiłby, że ich bicie zwietrzeje
    i watchdog zresetowałby całkowicie zdrowe urządzenie - co dokładnie się
    zdarzyło, gdy clock_task dostał godzinny okres. Cięcie snu utrzymuje
    watchdog w mocy: gdy task naprawdę zawiśnie, bicie i tak ustanie.
    """
    left = seconds
    while left > 0:
        chunk = step if left > step else left
        await asyncio.sleep(chunk)
        left -= chunk
        heartbeat(name)


# ---------- kolejka publikacji ----------
class LatestQueue:
    """Kolejka, w której dla danego topiku zostaje tylko NAJNOWSZA wartość.

    Wszystkie nasze topiki to stan publikowany z retain=True, więc przy
    zerwanym połączeniu zaległe pomiary nie mają żadnej wartości - lepiej
    dowieźć aktualne. Efekt uboczny jest równie ważny: kolejka nie może rosnąć
    w nieskończoność, bo liczba topików jest z góry znana.
    """

    def __init__(self, limit=160):
        self._items = {}        # topic -> (payload, retain)
        self._order = []        # FIFO topików (MicroPython nie gwarantuje kolejności dict)
        self._limit = limit
        self.dropped = 0

    def put(self, topic, payload, retain=True):
        if topic not in self._items:
            if len(self._order) >= self._limit:
                oldest = self._order.pop(0)
                del self._items[oldest]
                self.dropped += 1
            self._order.append(topic)
        self._items[topic] = (payload, retain)

    def get_nowait(self):
        if not self._order:
            raise IndexError("kolejka pusta")
        topic = self._order.pop(0)
        payload, retain = self._items.pop(topic)
        return topic, payload, retain

    def requeue(self, topic, payload, retain):
        """Zwraca wiadomość na początek kolejki po nieudanej publikacji.

        Jeśli w międzyczasie pojawiła się nowsza wartość dla tego topiku,
        starej nie przywracamy - i tak byłaby nadpisana.
        """
        if topic in self._items:
            return
        self._items[topic] = (payload, retain)
        self._order.insert(0, topic)

    def __len__(self):
        return len(self._order)


publish_queue = LatestQueue()


def enqueue(topic, payload_dict, retain=True):
    """Wstawia JSON do kolejki publikacji. Topik jest pełny, bez prefiksów."""
    publish_queue.put(topic.encode("utf-8"),
                      ujson.dumps(payload_dict).encode("utf-8"),
                      retain)


# ---------- licznik resetów ----------
# Liczniki żyją w retained wiadomości na brokerze: <TOPIC_DIAG>/<DEVID>/resets.
# Przy starcie czytamy tę kopię, dodajemy ten start i publikujemy z powrotem.
# Zero zapisów do flasha - stąd domyślne RESET_COUNTER_PERSIST_FLASH = False.
#
# Trzeba wiedzieć, co się z tym traci: retained przeżywa restart brokera tylko
# przy włączonym "persistence true" (pakiet mosquitto na Debianie ma je
# domyślnie), a zrzut na dysk idzie co autosave_interval, więc nagły zanik
# zasilania serwera może zgubić ostatnie inkrementacje. Kto woli, żeby licznik
# nie zależał od brokera, włącza RESET_COUNTER_PERSIST_FLASH - wtedy plik na
# flashu jest źródłem prawdy, a broker kopią (jeden zapis na start).
#
# Powód restartu jest odkładany tym samym kanałem, którego używa licznik: przy
# zapisie na flashu do pliku, a bez niego natychmiastową publikacją retained -
# po machine.reset() kolejka nie miałaby okazji się opróżnić.

_broker_counters = None     # retained kopia z brokera, tylko do wstępnej synchronizacji
_counters_synced = False
_counters = {}


def topic_resets():
    return "%s/%s/resets" % (TOPIC_DIAG, DEVID)


def _load_counters():
    if not RESET_COUNTER_PERSIST_FLASH:
        return {}
    try:
        with open(RESET_COUNTER_FILE) as f:
            data = ujson.load(f)
        if isinstance(data, dict):
            return data
        print("licznik resetów: plik ma zły format, zaczynam od zera")
    except OSError:
        pass                # pierwszy start - pliku jeszcze nie ma
    except Exception as e:
        print("licznik resetów: nie mogę odczytać pliku:", e)
    return {}


def _save_counters(data):
    if not RESET_COUNTER_PERSIST_FLASH:
        return False
    try:
        with open(RESET_COUNTER_FILE, "w") as f:
            ujson.dump(data, f)
        return True
    except Exception as e:
        print("licznik resetów: nie mogę zapisać pliku:", e)
        return False


def _publish_counters_now(payload):
    """Publikuje liczniki natychmiast, obchodząc kolejkę.

    Używane tuż przed resetem: po machine.reset() kolejka nie miałaby okazji się
    opróżnić, a przy wyłączonym zapisie na flash to jedyne miejsce, gdzie powód
    restartu może przetrwać.
    """
    if mqtt_client is None:
        return False
    try:
        mqtt_client.publish(topic_resets().encode(),
                            ujson.dumps(payload).encode("utf-8"), retain=True)
        return True
    except Exception as e:
        print("licznik resetów: publikacja awaryjna nie wyszła:", e)
        return False


def mark_reset_reason(reason):
    """Zapisuje intencję restartu, żeby następny start umiał ją policzyć.

    machine.reset() wygląda po starcie jak SOFT_RESET, a reset od sprzętowego
    watchdoga jak WDT_RESET - w obu przypadkach sam kod przyczyny nie mówi,
    CZY to my podjęliśmy decyzję i dlaczego. Dlatego powód zapisujemy zawczasu.
    """
    if not RESET_COUNTER_ENABLED:
        return

    if RESET_COUNTER_PERSIST_FLASH:
        data = _load_counters()
        data["pending_reason"] = reason
        data["pending_uptime"] = uptime_s()
        _save_counters(data)
        return

    # bez flasha: odłóż powód w retained wiadomości, natychmiast
    data = dict(_counters)
    data["pending_reason"] = reason
    data["pending_uptime"] = uptime_s()
    data["uptime"] = uptime_s()
    if not _publish_counters_now(data):
        print("licznik resetów: powód '%s' przepadnie - brak połączenia z brokerem"
              % reason)


def request_reset(reason):
    """Świadomy restart urządzenia z odnotowaniem powodu."""
    mark_reset_reason(reason)
    print("reset urządzenia, powód:", reason)
    machine.reset()


async def reset_counter_task():
    """Zlicza restarty z podziałem na przyczynę i publikuje wynik z retain."""
    global _counters, _counters_synced

    if not RESET_COUNTER_ENABLED:
        return

    _counters = _load_counters()

    # Najpierw poczekaj na samo połączenie z brokerem, a dopiero od niego licz
    # czas na dostarczenie retained kopii. Odliczanie od startu płytki byłoby
    # błędem: przy wolnym kojarzeniu z WiFi limit mijałby, zanim broker miałby
    # szansę cokolwiek przysłać, i historia po przeflashowaniu przepadałaby.
    waited = 0
    while waited < RESET_COUNTER_WAIT_MQTT_S and not mqtt_connected:
        await asyncio.sleep(1)
        waited += 1
        heartbeat("resets")

    # Retained kopia przydaje się tylko wtedy, gdy plik na flashu zniknął -
    # wtedy historię odtwarzamy z brokera.
    waited = 0
    while waited < RESET_COUNTER_SYNC_S and _broker_counters is None:
        await asyncio.sleep(1)
        waited += 1
        heartbeat("resets")

    remote = _broker_counters
    if remote and remote.get("total", 0) > _counters.get("total", 0):
        if RESET_COUNTER_PERSIST_FLASH:
            print("licznik resetów: odtwarzam z brokera (total %s > %s)"
                  % (remote.get("total"), _counters.get("total", 0)))
        # znacznik intencji dotyczy TEGO restartu, więc go nie gubimy
        pending = (_counters.get("pending_reason"), _counters.get("pending_uptime"))
        _counters = remote
        if pending[0]:
            _counters["pending_reason"] = pending[0]
            _counters["pending_uptime"] = pending[1]

    _counters_synced = True

    # policz ten start
    cause = _reset_cause_name()
    by_cause = _counters.setdefault("by_cause", {})
    by_cause[cause] = by_cause.get(cause, 0) + 1
    _counters["total"] = _counters.get("total", 0) + 1
    _counters["last_cause"] = cause

    reason = _counters.pop("pending_reason", None)
    prev_uptime = _counters.pop("pending_uptime", None)
    if reason:
        by_reason = _counters.setdefault("by_reason", {})
        by_reason[reason] = by_reason.get(reason, 0) + 1
    _counters["last_reason"] = reason

    # Jak długo trwał poprzedni bieg: ze znacznika (restart zamierzony) albo
    # z ostatniej retained publikacji poprzedniego biegu - to działa nawet po
    # zaniku zasilania, z dokładnością do RESET_COUNTER_PUBLISH_S.
    if prev_uptime is None and remote:
        prev_uptime = remote.get("uptime")
    _counters["prev_uptime"] = prev_uptime
    if prev_uptime:
        _counters["max_uptime"] = max(_counters.get("max_uptime", 0), prev_uptime)

    _counters["device"] = DEVID
    if _counters.get("since") is None:
        _counters["since"] = unix_time()

    _save_counters(_counters)
    print("licznik resetów: start nr %s, przyczyna %s, powód %s, poprzedni bieg %s s"
          % (_counters["total"], cause, reason, prev_uptime))

    while True:
        heartbeat("resets")
        payload = dict(_counters)
        payload["uptime"] = uptime_s()
        enqueue(topic_resets(), payload)
        await sleep_beating("resets", RESET_COUNTER_PUBLISH_S)


# ---------- WiFi ----------
async def wifi_manager():
    print("WiFi: inicjalizacja interfejsu")
    wlan.active(False)
    await asyncio.sleep(1)
    wlan.active(True)
    await asyncio.sleep(1)      # daj czas sterownikowi

    while True:
        heartbeat("wifi")

        if wlan.isconnected():
            await asyncio.sleep(5)
            continue

        print("WiFi: łączę z", WIFI_SSID)
        try:
            wlan.disconnect()   # wyczyść poprzedni stan
        except Exception:
            pass
        await asyncio.sleep(0.5)

        try:
            wlan.connect(WIFI_SSID, WIFI_PASS)
        except Exception as e:
            print("WiFi: wyjątek w connect():", e)
            # pełny restart interfejsu
            wlan.active(False)
            await asyncio.sleep(1)
            wlan.active(True)
            await asyncio.sleep(5)
            continue

        for _ in range(30):     # czekaj max 15 s
            if wlan.isconnected():
                break
            await asyncio.sleep(0.5)

        if wlan.isconnected():
            print("WiFi: połączono, IP", wlan.ifconfig()[0])
        else:
            print("WiFi: nie udało się, ponowię za 5 s")
            await asyncio.sleep(5)


# ---------- MQTT ----------
def make_client_id():
    return b"esp32s3-" + ubinascii.hexlify(machine.unique_id())


def sub_callback(topic, msg):
    """Obsługa wiadomości przychodzących. Wołana synchronicznie z check_msg(),
    musi być krótka - nic tu nie blokuje i nic nie gada po Modbusie."""
    global _broker_counters
    topic = topic.decode() if isinstance(topic, bytes) else topic
    body = msg.decode() if isinstance(msg, bytes) else msg
    print("MQTT RX:", topic, body)

    try:
        data = ujson.loads(body)
    except Exception as e:
        print("MQTT: payload nie jest JSON-em:", e)
        return

    # Retained kopia liczników resetów - to nie komenda, więc leci przed
    # sprawdzeniem "name". Bierzemy tylko pierwszą, do wstępnej synchronizacji;
    # potem sami publikujemy na ten topik i nie chcemy słuchać własnego echa.
    if topic == topic_resets():
        if not _counters_synced:
            _broker_counters = data
        return

    if data.get("name") != DEVID:
        return              # komenda dla innego urządzenia

    if topic == TOPIC_CMD_REBOOT:
        print("MQTT: żądanie restartu")
        request_reset("mqtt_command")

    elif topic == TOPIC_CMD_WRITE:
        # sam zapis idzie przez deye_task, żeby nie wejść w kolizję na magistrali
        _write_requests.append(data)


async def mqtt_manager():
    global mqtt_client, mqtt_connected
    client_id = make_client_id()
    reconnect_delay = 5

    while True:
        heartbeat("mqtt")

        if not wlan.isconnected():
            if mqtt_client:
                _drop_client()
            await asyncio.sleep(reconnect_delay)
            continue

        if not mqtt_client:
            try:
                mqtt_client = MQTTClient(client_id, MQTT_BROKER, port=MQTT_PORT,
                                         keepalive=MQTT_KEEPALIVE)
                mqtt_client.set_callback(sub_callback)
                mqtt_client.connect()
                mqtt_client.subscribe(TOPIC_CMD_REBOOT.encode())
                mqtt_client.subscribe(TOPIC_CMD_WRITE.encode())
                if RESET_COUNTER_ENABLED and not _counters_synced:
                    mqtt_client.subscribe(topic_resets().encode())
                mqtt_connected = True
                state["mqtt_reconnects"] += 1
                state["mqtt_last_ok"] = uptime_s()
                print("MQTT: połączono i zasubskrybowano")
                reconnect_delay = 5
            except Exception as e:
                print("MQTT: błąd połączenia:", e)
                mqtt_client = None
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, 60)
                continue

        try:
            mqtt_client.check_msg()

            for _ in range(5):          # max 5 publikacji na przebieg
                try:
                    topic, payload, retain = publish_queue.get_nowait()
                except IndexError:
                    break
                try:
                    mqtt_client.publish(topic, payload, retain=retain)
                    state["mqtt_last_ok"] = uptime_s()
                except Exception as e:
                    state["publish_errors"] += 1
                    print("MQTT: błąd publikacji:", e, "- wracam z", topic)
                    publish_queue.requeue(topic, payload, retain)
                    raise                # wyjdź do reconnectu

            await asyncio.sleep(0.2)

        except Exception as e:
            print("MQTT: błąd:", e, "- łączę ponownie")
            _drop_client()
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)


def _drop_client():
    global mqtt_client, mqtt_connected
    if mqtt_client:
        try:
            mqtt_client.disconnect()
        except Exception:
            pass
    mqtt_client = None
    mqtt_connected = False


# ---------- odpytywanie falownika ----------
def publish_field(name, address, value, unit, desc):
    payload = {"value": value, "unit": unit, "reg": address, "uptime": uptime_s()}
    ts = unix_time()
    if ts is not None:
        payload["ts"] = ts
    enqueue("%s/%s" % (TOPIC_BASE, name), payload)

    # Zgodność w tył: stary topik i stary kształt payloadu. Do wyłączenia
    # flagą LEGACY_REG_TOPICS, gdy konsumenci przejdą na nazwy pól.
    if _legacy_wanted(address):
        enqueue("%s/regs/%d" % (TOPIC_BASE, address),
                {"value": value, "timestamp": time.time(), "unit": unit, "desc": desc})


async def handle_write_requests():
    """Wykonuje żądania zapisu przyjęte z MQTT. Woła się z deye_task."""
    while _write_requests:
        cmd = _write_requests.pop(0)
        reg = cmd.get("reg")
        value = cmd.get("value")
        result = {"reg": reg, "value": value, "uptime": uptime_s()}

        if not MODBUS_WRITE_ENABLED:
            result["error"] = "zapis wyłączony (MODBUS_WRITE_ENABLED=False)"
        elif not isinstance(reg, int) or not isinstance(value, int):
            result["error"] = "reg i value muszą być liczbami całkowitymi"
        elif reg not in MODBUS_WRITE_ALLOWED:
            result["error"] = "rejestr %s nie jest na białej liście" % reg
        else:
            ok = await modbusrtu.write_register(reg, value)
            result["ok"] = ok
            if not ok:
                result["error"] = "falownik nie potwierdził zapisu"

        if "error" in result:
            result.setdefault("ok", False)
            print("zapis Modbus odrzucony:", result["error"])

        # Numer rejestru w topiku, bo kolejka zostawia tylko najnowszą wiadomość
        # dla danego topiku - wspólny "write_result" gubiłby wcześniejsze wyniki.
        enqueue("%s/write_result/%s" % (TOPIC_BASE, reg), result, retain=False)


async def deye_task():
    # Kiedy dany blok jest znowu "należny" (ticks_ms). Start rozłożony w czasie,
    # żeby po restarcie nie wysłać wszystkich zapytań w jednej chwili.
    now = time.ticks_ms()
    next_at = [time.ticks_add(now, i * 500) for i in range(len(BLOCKS))]

    while True:
        heartbeat("deye")
        await handle_write_requests()

        for i, block in enumerate(BLOCKS):
            now = time.ticks_ms()
            if time.ticks_diff(now, next_at[i]) < 0:
                continue

            count = registers.block_size(block)
            words = await modbusrtu.read_registers(block["start"], count)

            if words is None:
                state["blocks_failed"] += 1
                state["modbus_fail_streak"] += 1
                # Nie czekamy całego okresu bloku - dla energii to 15 minut.
                next_at[i] = time.ticks_add(time.ticks_ms(), BLOCK_RETRY_S * 1000)
            else:
                state["blocks_ok"] += 1
                state["modbus_fail_streak"] = 0
                state["modbus_last_ok"] = uptime_s()
                next_at[i] = time.ticks_add(time.ticks_ms(), block["period"] * 1000)
                try:
                    for field in registers.decode_block(block, words):
                        publish_field(*field)
                except Exception as e:
                    print("blok %d: błąd dekodowania: %s" % (block["start"], e))

            await asyncio.sleep_ms(BLOCK_GAP_MS)

        state["cycles"] += 1
        await asyncio.sleep(POLL_TICK_S)


# ---------- zegar falownika ----------
# Rejestry 62-64 (R/W): sześć bajtów czasu, po dwa na rejestr.
#   62 = rok-2000 | miesiąc,  63 = dzień | godzina,  64 = minuta | sekunda
# Zegar falownika lekko dryfuje, a po nim planowane są okna ładowania - warto
# go korygować, ale rzadko i tylko wtedy, gdy rozjazd faktycznie jest duży.
CLOCK_REG = 62


def _fmt_time(tm):
    return "%04d-%02d-%02d %02d:%02d:%02d" % tuple(tm[:6])


def _decode_clock(words):
    """Rozpakowuje rejestry 62-64. Zwraca (tuple, epoka) albo (None, None)."""
    tm = (2000 + (words[0] >> 8), words[0] & 0xFF,
          words[1] >> 8, words[1] & 0xFF,
          words[2] >> 8, words[2] & 0xFF)
    try:
        # mktime odrzuci bzdury, np. miesiąc 0 albo 37 - wtedy wiemy, że
        # kolejność bajtów w tym firmware jest inna, niż zakładamy
        epoch = time.mktime(tm + (0, 0))
    except Exception:
        return None, None
    if not (1 <= tm[1] <= 12 and 1 <= tm[2] <= 31 and tm[3] <= 23
            and tm[4] <= 59 and tm[5] <= 59):
        return None, None
    return tm, epoch


def _encode_clock(tm):
    return [((tm[0] - 2000) << 8) | tm[1], (tm[2] << 8) | tm[3], (tm[4] << 8) | tm[5]]


async def _read_inverter_clock():
    words = await modbusrtu.read_registers(CLOCK_REG, 3)
    if words is None:
        return None, "falownik nie odpowiedział"
    tm, epoch = _decode_clock(words)
    if tm is None:
        return None, "nieprawidłowa data w rejestrach: %s" % [hex(w) for w in words]
    return (tm, epoch), None


async def _fix_clock(drift, last_write_uptime, result):
    """Decyduje o korekcie zegara i ją wykonuje. Zwraca opis podjętej akcji."""
    if abs(drift) <= CLOCK_MAX_DRIFT_S:
        return "ok"
    if not CLOCK_SYNC_ENABLED:
        return "pominięte: CLOCK_SYNC_ENABLED=False"
    if (last_write_uptime is not None
            and uptime_s() - last_write_uptime < CLOCK_SYNC_INTERVAL_S):
        return "odłożone: zapis nie częściej niż co %d s" % CLOCK_SYNC_INTERVAL_S

    # świeży czas - przy odczycie i porównaniu uciekło kilka sekund
    local = local_time_tuple()
    if local is None:
        return "pominięte: utracono synchronizację NTP"
    if not 2000 <= local[0] <= 2255:
        return "pominięte: rok %d nie zmieści się w rejestrze" % local[0]

    print("zegar falownika rozjechany o %d s, zapisuję %s" % (drift, _fmt_time(local)))
    if not await modbusrtu.write_registers(CLOCK_REG, _encode_clock(local)):
        result["error"] = "falownik nie potwierdził zapisu zegara"
        return "zapis nieudany"

    await asyncio.sleep_ms(500)
    clock, error = await _read_inverter_clock()
    if error:
        result["error"] = "weryfikacja po zapisie: %s" % error
        return "zapisane, niesprawdzone"

    after = local_time_tuple()
    result["inverter_time_after"] = _fmt_time(clock[0])
    if after is not None:
        result["drift_after_s"] = clock[1] - time.mktime(after)
    return "zapisane"


async def clock_task():
    """Monitoruje zegar falownika, a przy włączonym CLOCK_SYNC_ENABLED koryguje go.

    Sam odczyt działa zawsze i jest publikowany, więc poprawność dekodowania
    (kolejność bajtów w rejestrach) można potwierdzić BEZ włączania zapisu.
    """
    last_write_uptime = None

    while True:
        heartbeat("clock")
        result = {"uptime": uptime_s(), "sync_enabled": CLOCK_SYNC_ENABLED}

        try:
            clock, error = await _read_inverter_clock()
            if error:
                result["error"] = error
            else:
                tm, inverter_epoch = clock
                result["inverter_time"] = _fmt_time(tm)
                local = local_time_tuple()

                if local is None:
                    result["note"] = "bez NTP nie ma z czym porównać"
                else:
                    result["local_time"] = _fmt_time(local)
                    result["drift_s"] = inverter_epoch - time.mktime(local)
                    result["action"] = await _fix_clock(
                        result["drift_s"], last_write_uptime, result)
                    if result["action"].startswith("zapisane"):
                        last_write_uptime = uptime_s()
        except Exception as e:
            result["error"] = "%s %s" % (type(e).__name__, e)
            print("clock_task:", result["error"])

        enqueue("%s/clock" % TOPIC_BASE, result)
        await sleep_beating("clock", CLOCK_CHECK_INTERVAL_S)


# ---------- diagnostyka ----------
def _reset_cause_name():
    names = {}
    for attr, label in (("PWRON_RESET", "power_on"), ("HARD_RESET", "hard"),
                        ("WDT_RESET", "watchdog"), ("DEEPSLEEP_RESET", "deepsleep"),
                        ("SOFT_RESET", "soft")):
        code = getattr(machine, attr, None)
        if code is not None:
            names[code] = label
    return names.get(machine.reset_cause(), "unknown_%s" % machine.reset_cause())


def _age_s(mark):
    """Ile sekund temu odnotowano zdarzenie (mark w sekundach uptime)."""
    if mark is None:
        return None
    return uptime_s() - mark


async def diag_task():
    reset_cause = _reset_cause_name()

    while True:
        heartbeat("diag")
        try:
            gc.collect()
            payload = {
                "device": DEVID,
                "uptime": uptime_s(),
                "reset_cause": reset_cause,
                "mem_free": gc.mem_free(),
                "mem_alloc": gc.mem_alloc(),
                "wifi": wlan.isconnected(),
                "mqtt": mqtt_connected,
                "time_synced": time_synced,
                "queue_len": len(publish_queue),
                "queue_dropped": publish_queue.dropped,
                "mqtt_reconnects": state["mqtt_reconnects"],
                "mqtt_last_ok_s": _age_s(state["mqtt_last_ok"]),
                "publish_errors": state["publish_errors"],
                "cycles": state["cycles"],
                "blocks_ok": state["blocks_ok"],
                "blocks_failed": state["blocks_failed"],
                "modbus_fail_streak": state["modbus_fail_streak"],
                "modbus_last_ok_s": _age_s(state["modbus_last_ok"]),
                "modbus": modbusrtu.stats,
                # wiek bicia każdego taska - rosnąca wartość zdradza zawieszony
                # (albo zbyt rzadko bijący) task, zanim watchdog zdąży zresetować
                "beats": {n: time.ticks_diff(time.ticks_ms(), t) // 1000
                          for n, t in _beats.items()},
            }
            try:
                payload["rssi"] = wlan.status("rssi") if wlan.isconnected() else None
            except Exception:
                pass
            enqueue("%s/%s" % (TOPIC_DIAG, DEVID), payload)
        except Exception as e:
            print("diag_task:", type(e).__name__, e)

        await sleep_beating("diag", DIAG_INTERVAL_S)


async def discovery_task():
    while True:
        heartbeat("discovery")
        try:
            ip = wlan.ifconfig()[0] if wlan.isconnected() else None
            enqueue(TOPIC_DISCOVERY,
                    {"name": DEVID, "ip": ip, "uptime": uptime_s(), "ts": unix_time()},
                    retain=False)
        except Exception as e:
            print("discovery_task:", type(e).__name__, e)
        await sleep_beating("discovery", DISCOVERY_INTERVAL_S)


# ---------- zegar ----------
async def ntp_task():
    global time_synced
    if not NTP_ENABLED:
        return

    try:
        import ntptime
    except ImportError:
        print("NTP: brak modułu ntptime, pomijam synchronizację")
        return
    ntptime.host = NTP_HOST

    while True:
        if not wlan.isconnected():
            await asyncio.sleep(5)      # nie marnuj próby, zanim wstanie WiFi
            continue

        try:
            ntptime.settime()
            time_synced = True
            print("NTP: zegar zsynchronizowany")
        except Exception as e:
            print("NTP: nie udało się:", e)

        # po sukcesie wystarczy raz na 12 h, po porażce ponów za 5 minut
        await asyncio.sleep(43_200 if time_synced else 300)


# ---------- dioda ----------
def modbus_led_event(event):
    """Hook z modbusrtu - tylko zapamiętuje zdarzenie, nie blokuje magistrali."""
    global _led_flash
    _led_flash = ((0, 0, 20) if event == "ok" else (25, 0, 0), time.ticks_ms())


async def led_task():
    while True:
        color = None
        if _led_flash and time.ticks_diff(time.ticks_ms(), _led_flash[1]) < 150:
            color = _led_flash[0]
        elif not wlan.isconnected():
            color = (10, 0, 0)      # brak WiFi
        elif not mqtt_connected:
            color = (0, 0, 10)      # WiFi jest, brokera nie ma
        else:
            color = (0, 8, 0)       # wszystko działa

        try:
            RGBLed.set_color(*color)
        except Exception:
            pass
        await asyncio.sleep_ms(100)


# ---------- watchdog ----------
async def wdt_task():
    """Karmi sprzętowy watchdog, ale tylko gdy wszystkie taski dają znak życia.

    Sam feed w osobnym tasku pilnowałby jedynie pętli asyncio: gdyby padło
    odpytywanie falownika, watchdog nadal byłby karmiony i płytka nie wstałaby
    nigdy. Dlatego każdy task odbija się przez heartbeat(), a tutaj tylko
    sprawdzamy, czy żaden nie jest przeterminowany.
    """
    # Kredyt na start, żeby świeżo utworzone taski nie wyglądały na zawieszone.
    for name in ("wifi", "mqtt", "deye", "diag", "discovery", "clock"):
        heartbeat(name)

    await asyncio.sleep(WDT_START_DELAY_S)
    wdt = machine.WDT(timeout=WDT_TIMEOUT_MS)
    print("watchdog: uzbrojony, timeout", WDT_TIMEOUT_MS, "ms")
    alerted = False

    while True:
        now = time.ticks_ms()
        stale = [n for n, t in _beats.items()
                 if time.ticks_diff(now, t) > TASK_DEADLINE_MS]

        if stale:
            print("watchdog: brak bicia z", stale, "- przestaję karmić")
            # Powiedz o tym na MQTT, zanim płytka padnie. Do resetu zostało
            # jeszcze WDT_TIMEOUT_MS, a mqtt_manager kręci się co 0.2 s, więc
            # wiadomość zdąży wyjść - inaczej byłby to cichy restart bez śladu.
            if not alerted:
                alerted = True
                mark_reset_reason("watchdog_stale")
                enqueue("%s/%s/alert" % (TOPIC_DIAG, DEVID),
                        {"device": DEVID, "uptime": uptime_s(), "stale": stale,
                         "deadline_s": TASK_DEADLINE_MS // 1000,
                         "reason": "watchdog przestał karmić, reset za max %d s"
                                   % (WDT_TIMEOUT_MS // 1000)})
        else:
            alerted = False
            wdt.feed()

        # Awaryjne resety na wypadek zawieszonego stosu WiFi lub martwego RS485.
        # Watchdog by tego nie złapał, bo taski formalnie żyją. Uwaga: wiek 0 s
        # jest poprawną wartością, więc None trzeba sprawdzać wprost.
        if REBOOT_AFTER_NO_MQTT_S:
            mqtt_age = _age_s(state["mqtt_last_ok"])
            if mqtt_age is None:
                mqtt_age = uptime_s()
            if mqtt_age > REBOOT_AFTER_NO_MQTT_S:
                print("watchdog: brak kontaktu z brokerem od", mqtt_age, "s - reset")
                request_reset("mqtt_dead")

        if REBOOT_AFTER_NO_MODBUS_S:
            modbus_age = _age_s(state["modbus_last_ok"])
            if modbus_age is None:
                modbus_age = uptime_s()
            if modbus_age > REBOOT_AFTER_NO_MODBUS_S:
                print("watchdog: brak odpowiedzi falownika od", modbus_age, "s - reset")
                request_reset("modbus_dead")

        await asyncio.sleep(5)


# ---------- start ----------
async def main():
    print("deyemqtt start:", DEVID, "| powód restartu:", _reset_cause_name())
    print("bloki rejestrów:", ", ".join(
        "%d x%d/%ds" % (b["start"], registers.block_size(b), b["period"]) for b in BLOCKS))

    modbusrtu.on_event = modbus_led_event
    await asyncio.sleep(2)      # daj czas na inicjalizację sterownika WiFi

    asyncio.create_task(wifi_manager())
    asyncio.create_task(mqtt_manager())
    asyncio.create_task(deye_task())
    asyncio.create_task(diag_task())
    asyncio.create_task(discovery_task())
    asyncio.create_task(led_task())
    asyncio.create_task(ntp_task())
    asyncio.create_task(clock_task())
    asyncio.create_task(reset_counter_task())
    asyncio.create_task(wdt_task())
    if FTP_ENABLED:
        asyncio.create_task(uftpd.ftp())

    while True:
        await asyncio.sleep(60)


try:
    asyncio.run(main())
finally:
    try:
        asyncio.new_event_loop()
    except Exception:
        pass

# end.
