# modbusrtu.py - минимальный мастер Modbus RTU по UART (RS485) для MicroPython
# https://github.com/pradki/deye-modbus-mqtt
#
# Поддерживает функции 0x03 (чтение регистров), 0x06 (запись одного регистра)
# и 0x10 (запись нескольких регистров). Рассчитан на одного мастера и одного
# ведомого на шине, без ретрансляции — повторная попытка относится к верхнему
# слою.
#
# Важные детали, которые на практике определяют устойчивость связи:
#   * перед каждой транзакцией чистим буфер RX — мусор после предыдущего
#     таймаута разъезжается со следующим кадром и даёт ложные ошибки CRC,
#   * ответ читаем до известной длины кадра, а не только «до тишины на
#     линии» — при нескольких задачах asyncio пауза между байтами может
#     вырасти, и кадр оказался бы обрезан на середине,
#   * проверяем адрес ведомого, код функции и счётчик байт, а кадр исключения
#     (код функции с установленным битом 0x80) сообщаем отдельно.

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

# Максимальное число регистров в одном запросе 0x03.
# Протокол Modbus допускает 125, но SW-2200A+ отвечает максимум на 8
# регистров за раз — большие запросы отбрасываются (исключения). Все блоки
# в registers.py укладываются в этот лимит (следит за этим registers._validate).
MAX_REGS_PER_READ = 8

# Интервал между отправкой запроса и началом приёма. Даёт инвертору время
# переключить направление передачи и освободить наш буфер TX.
TURNAROUND_MS = 20

# Тишина на линии, считаемая концом кадра. Для 9600 бод 3.5 символа — это
# ~4 мс, но при параллельных задачах asyncio такой порог слишком агрессивен.
SILENCE_MS = 10

# Счётчик событий — публикуется main.py в диагностическом топике.
stats = {
    "ok": 0,          # правильные ответы
    "timeout": 0,     # нет ответа за отведённое время
    "crc": 0,         # неверная контрольная сумма
    "exception": 0,   # инвертор ответил кадром исключения
    "malformed": 0,   # неверный адрес / код функции / длина
}

# Необязательный хук для сигнализации (например, вывод на экран). main.py
# подставляет сюда свою функцию. Должен быть неблокирующим — вызывается
# в тракте Modbus.
on_event = None

uart = UART(UART_ID, baudrate=UART_BAUDRATE, tx=UART_TX_PIN, rx=UART_RX_PIN)

# Шина одна, а желающих несколько (опрос, записи из MQTT, синхронизация
# часов). Блокировка держит пару запрос-ответ вместе.
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
    """CRC16 Modbus. Возвращает int; в кадре идёт little-endian."""
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
    """Добавляет CRC к телу кадра."""
    return bytes(body) + ustruct.pack("<H", crc16(body))


def flush_rx():
    """Выбрасывает всё, что скопилось в буфере приёма."""
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
        # нужно дождаться реальной отправки байт, иначе обрежем конец кадра
        try:
            uart.flush()
        except AttributeError:
            time.sleep_ms(1 + (len(frame) * 10_000) // UART_BAUDRATE)
        _de.value(0)


async def _read_frame(expected_len, timeout_ms):
    """Читает кадр ответа. Возвращает bytearray (возможно, неполный) или None."""
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
            # кадр исключения короче обычного ответа
            if len(buf) >= 5 and (buf[1] & 0x80):
                return buf
        elif buf and time.ticks_diff(now, last) > SILENCE_MS:
            return buf

        if time.ticks_diff(now, start) >= timeout_ms:
            return buf if buf else None

        await asyncio.sleep_ms(2)


def _check(frame, slave, fc):
    """Проверяет кадр ответа. Возвращает (payload, строка_ошибки)."""
    if frame is None:
        return None, "timeout"
    if len(frame) < 5:
        return None, "кадр слишком короткий (%d Б)" % len(frame)

    body = frame[:-2]
    crc_got = ustruct.unpack("<H", frame[-2:])[0]
    crc_exp = crc16(body)
    if crc_got != crc_exp:
        return None, "crc %04X != %04X" % (crc_got, crc_exp)

    if body[0] != slave:
        return None, "адрес ведомого %d, ожидалось %d" % (body[0], slave)

    if body[1] == (fc | 0x80):
        code = body[2] if len(body) > 2 else 0
        return None, "исключение modbus 0x%02X" % code

    if body[1] != fc:
        return None, "код функции 0x%02X, ожидалось 0x%02X" % (body[1], fc)

    return body[2:], None


async def _transact(slave, fc, body, expected_len, timeout_ms):
    """Одна транзакция: отправить, принять, проверить. Возвращает payload или None."""
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
    elif error.startswith("исключение"):
        stats["exception"] += 1
    else:
        stats["malformed"] += 1

    print("modbus: %s (fc=0x%02X, кадр=%s)" % (error, fc, frame))
    _notify("error")
    return None


async def read_registers(start_reg, num_regs, slave=None, timeout_ms=None):
    """Функция 0x03. Возвращает список сырых u16 или None при ошибке."""
    if num_regs < 1 or num_regs > MAX_REGS_PER_READ:
        raise ValueError("num_regs вне диапазона 1..%d" % MAX_REGS_PER_READ)

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
        print("modbus: счётчик байт %d, ожидалось %d (данных %d Б)"
              % (byte_count, 2 * num_regs, len(data)))
        return None

    return [(data[i] << 8) | data[i + 1] for i in range(0, byte_count, 2)]


async def write_register(reg, value, slave=None, timeout_ms=None):
    """Функция 0x06. Возвращает True, когда инвертор подтвердил запись."""
    slave = MODBUS_SLAVE_ADDR if slave is None else slave
    timeout_ms = MODBUS_TIMEOUT_MS if timeout_ms is None else timeout_ms

    raw = value & 0xFFFF
    body = bytearray([slave, FC_WRITE_SINGLE])
    body += ustruct.pack(">HH", reg, raw)

    payload = await _transact(slave, FC_WRITE_SINGLE, body, 8, timeout_ms)
    if payload is None:
        return False

    # правильный ответ — эхо адреса и значения
    echo_reg, echo_val = ustruct.unpack(">HH", payload[:4])
    if echo_reg != reg or echo_val != raw:
        stats["malformed"] += 1
        print("modbus: эхо записи не совпадает (%d=%d)" % (echo_reg, echo_val))
        return False
    return True


async def write_registers(start_reg, values, slave=None, timeout_ms=None):
    """Функция 0x10 - запись нескольких подряд идущих регистров. Возвращает True при успехе."""
    if not values or len(values) > 123:
        raise ValueError("число значений вне диапазона 1..123")

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
        print("modbus: эхо блочной записи не совпадает (%d x%d)" % (echo_reg, echo_cnt))
        return False
    return True

# Конец файла.
