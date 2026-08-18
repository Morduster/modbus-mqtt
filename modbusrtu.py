# modbusrtu.py - minimalny master Modbus RTU na UART (RS485) dla MicroPython
# https://github.com/pradki/deye-modbus-mqtt
#
# Obsługuje funkcje 0x03 (read holding registers), 0x06 (write single register)
# i 0x10 (write multiple registers). Zaprojektowany pod jednego mastera i jeden
# slave na magistrali, bez retransmisji - ponowna próba należy do warstwy wyżej.
#
# Ważne detale, które w praktyce decydują o stabilności łącza:
#   * przed każdą transakcją czyścimy bufor RX - śmieci po poprzednim timeoucie
#     rozjeżdżają następną ramkę i produkują fałszywe błędy CRC,
#   * odpowiedź czytamy do znanej długości ramki, a nie tylko "do ciszy na
#     łączu" - przy kilku taskach asyncio przerwa między bajtami potrafi urosnąć
#     i ramka zostałaby ucięta w połowie,
#   * sprawdzamy adres slave, kod funkcji i licznik bajtów, a ramkę wyjątku
#     (kod funkcji z ustawionym bitem 0x80) raportujemy osobno.

import uasyncio as asyncio
import time
import ustruct
from machine import UART, Pin

from cfg import (
    UART_ID,
    UART_TX_PIN,
    UART_RX_PIN,
    UART_BAUDRATE,
    RS485_DE_PIN,
    MODBUS_SLAVE_ADDR,
    MODBUS_TIMEOUT_MS,
)

FC_READ_HOLDING = 0x03
FC_WRITE_SINGLE = 0x06
FC_WRITE_MULTIPLE = 0x10

# Maksymalna liczba rejestrów w jednym zapytaniu 0x03 (limit protokołu: 125)
MAX_REGS_PER_READ = 125

# Odstęp między nadaniem zapytania a rozpoczęciem nasłuchu. Daje falownikowi
# czas na przełączenie kierunku transmisji i opróżnienie naszego bufora TX.
TURNAROUND_MS = 20

# Cisza na łączu uznawana za koniec ramki. Dla 9600 baud 3.5 znaku to ~4 ms,
# ale przy współbieżnych taskach asyncio taki próg jest zbyt agresywny.
SILENCE_MS = 10

# Licznik zdarzeń - publikowany przez main.py na topiku diagnostycznym.
stats = {
    "ok": 0,          # poprawne odpowiedzi
    "timeout": 0,     # brak odpowiedzi w zadanym czasie
    "crc": 0,         # zła suma kontrolna
    "exception": 0,   # falownik odpowiedział ramką wyjątku
    "malformed": 0,   # zły adres / kod funkcji / długość
}

# Opcjonalny hook do sygnalizacji (np. mrugnięcie diodą). main.py podstawia
# tu własną funkcję. Musi być nieblokująca - jest wołana w ścieżce Modbusa.
on_event = None

uart = UART(UART_ID, baudrate=UART_BAUDRATE, tx=UART_TX_PIN, rx=UART_RX_PIN)

# Magistrala jest jedna, a chętnych kilku (odpytywanie, zapisy z MQTT,
# synchronizacja zegara). Blokada trzyma parę zapytanie-odpowiedź razem.
_bus = asyncio.Lock()

_de = None
if RS485_DE_PIN is not None:
    _de = Pin(RS485_DE_PIN, Pin.OUT, value=0)


def _notify(event):
    if on_event is not None:
        try:
            on_event(event)
        except Exception as e:
            print("modbusrtu: błąd hooka on_event:", e)


def crc16(data):
    """CRC16 Modbus. Zwraca int; w ramce idzie little-endian."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc


def _frame(body):
    """Dokłada CRC do ciała ramki."""
    return bytes(body) + ustruct.pack("<H", crc16(body))


def flush_rx():
    """Wyrzuca wszystko, co zaległo w buforze odbiorczym."""
    dropped = 0
    while uart.any():
        chunk = uart.read()
        if not chunk:
            break
        dropped += len(chunk)
    return dropped


def _write(frame):
    if _de is not None:
        _de.value(1)
    uart.write(frame)
    if _de is not None:
        # trzeba poczekać, aż bajty faktycznie wyjdą, inaczej urwiemy koniec ramki
        try:
            uart.flush()
        except AttributeError:
            time.sleep_ms(1 + (len(frame) * 10_000) // UART_BAUDRATE)
        _de.value(0)


async def _read_frame(expected_len, timeout_ms):
    """Czyta ramkę odpowiedzi. Zwraca bytearray (może być niepełna) albo None."""
    buf = bytearray()
    start = time.ticks_ms()
    last = start

    while True:
        chunk = uart.read()
        now = time.ticks_ms()

        if chunk:
            buf.extend(chunk)
            last = now
            if len(buf) >= expected_len:
                return buf
            # ramka wyjątku jest krótsza od normalnej odpowiedzi
            if len(buf) >= 5 and (buf[1] & 0x80):
                return buf
        elif buf and time.ticks_diff(now, last) > SILENCE_MS:
            return buf

        if time.ticks_diff(now, start) >= timeout_ms:
            return buf if buf else None

        await asyncio.sleep_ms(2)


def _check(frame, slave, fc):
    """Waliduje ramkę odpowiedzi. Zwraca (payload, error_string)."""
    if frame is None:
        return None, "timeout"
    if len(frame) < 5:
        return None, "ramka za krótka (%d B)" % len(frame)

    body = frame[:-2]
    crc_got = ustruct.unpack("<H", frame[-2:])[0]
    crc_exp = crc16(body)
    if crc_got != crc_exp:
        return None, "crc %04X != %04X" % (crc_got, crc_exp)

    if body[0] != slave:
        return None, "adres slave %d, oczekiwano %d" % (body[0], slave)

    if body[1] == (fc | 0x80):
        code = body[2] if len(body) > 2 else 0
        return None, "wyjatek modbus 0x%02X" % code

    if body[1] != fc:
        return None, "kod funkcji 0x%02X, oczekiwano 0x%02X" % (body[1], fc)

    return body[2:], None


async def _transact(slave, fc, body, expected_len, timeout_ms):
    """Jedna transakcja: nadaj, odbierz, zwaliduj. Zwraca payload albo None."""
    async with _bus:
        flush_rx()
        _write(_frame(body))
        await asyncio.sleep_ms(TURNAROUND_MS)
        frame = await _read_frame(expected_len, timeout_ms)

    payload, error = _check(frame, slave, fc)

    if error is None:
        stats["ok"] += 1
        _notify("ok")
        return payload

    if error == "timeout":
        stats["timeout"] += 1
    elif error.startswith("crc"):
        stats["crc"] += 1
    elif error.startswith("wyjatek"):
        stats["exception"] += 1
    else:
        stats["malformed"] += 1

    print("modbus: %s (fc=0x%02X, ramka=%s)" % (error, fc, frame))
    _notify("error")
    return None


async def read_registers(start_reg, num_regs, slave=None, timeout_ms=None):
    """Funkcja 0x03. Zwraca listę surowych u16 albo None przy błędzie."""
    if num_regs < 1 or num_regs > MAX_REGS_PER_READ:
        raise ValueError("num_regs poza zakresem 1..%d" % MAX_REGS_PER_READ)

    slave = MODBUS_SLAVE_ADDR if slave is None else slave
    timeout_ms = MODBUS_TIMEOUT_MS if timeout_ms is None else timeout_ms

    body = bytearray([slave, FC_READ_HOLDING])
    body += ustruct.pack(">HH", start_reg, num_regs)

    payload = await _transact(slave, FC_READ_HOLDING, body, 5 + 2 * num_regs, timeout_ms)
    if payload is None:
        return None

    byte_count = payload[0]
    data = payload[1:]
    if byte_count != 2 * num_regs or len(data) < byte_count:
        stats["malformed"] += 1
        print("modbus: licznik bajtów %d, oczekiwano %d (dane %d B)"
              % (byte_count, 2 * num_regs, len(data)))
        return None

    return [(data[i] << 8) | data[i + 1] for i in range(0, byte_count, 2)]


async def write_register(reg, value, slave=None, timeout_ms=None):
    """Funkcja 0x06. Zwraca True, gdy falownik potwierdził zapis."""
    slave = MODBUS_SLAVE_ADDR if slave is None else slave
    timeout_ms = MODBUS_TIMEOUT_MS if timeout_ms is None else timeout_ms

    raw = value & 0xFFFF
    body = bytearray([slave, FC_WRITE_SINGLE])
    body += ustruct.pack(">HH", reg, raw)

    payload = await _transact(slave, FC_WRITE_SINGLE, body, 8, timeout_ms)
    if payload is None:
        return False

    # poprawna odpowiedź to echo adresu i wartości
    echo_reg, echo_val = ustruct.unpack(">HH", payload[:4])
    if echo_reg != reg or echo_val != raw:
        stats["malformed"] += 1
        print("modbus: echo zapisu nie zgadza się (%d=%d)" % (echo_reg, echo_val))
        return False
    return True


async def write_registers(start_reg, values, slave=None, timeout_ms=None):
    """Funkcja 0x10 - zapis wielu kolejnych rejestrów. Zwraca True przy sukcesie."""
    if not values or len(values) > 123:
        raise ValueError("liczba wartości poza zakresem 1..123")

    slave = MODBUS_SLAVE_ADDR if slave is None else slave
    timeout_ms = MODBUS_TIMEOUT_MS if timeout_ms is None else timeout_ms

    body = bytearray([slave, FC_WRITE_MULTIPLE])
    body += ustruct.pack(">HHB", start_reg, len(values), 2 * len(values))
    for v in values:
        body += ustruct.pack(">H", v & 0xFFFF)

    payload = await _transact(slave, FC_WRITE_MULTIPLE, body, 8, timeout_ms)
    if payload is None:
        return False

    echo_reg, echo_cnt = ustruct.unpack(">HH", payload[:4])
    if echo_reg != start_reg or echo_cnt != len(values):
        stats["malformed"] += 1
        print("modbus: echo zapisu blokowego nie zgadza się (%d x%d)" % (echo_reg, echo_cnt))
        return False
    return True

# end.
