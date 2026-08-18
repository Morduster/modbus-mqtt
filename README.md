# deyemqtt — Deye inverter → MQTT bridge on ESP32-S3

MicroPython firmware that reads a Deye hybrid inverter over RS485 (Modbus RTU)
and publishes the values to an MQTT broker as JSON. It has been running
unattended on a Deye 12 kW installation as the data source for a home
automation system ([pradki/cave](https://github.com/pradki/cave)).

Everything runs on a single `uasyncio` loop: WiFi and MQTT reconnect on their
own, register blocks are polled on independent schedules, and a hardware
watchdog reboots the board if any task stops making progress. It can also keep
the inverter's own clock from drifting.

```
┌──────────┐   RS485    ┌──────────┐   WiFi    ┌────────────┐
│  Deye    │◄──────────►│ ESP32-S3 │◄─────────►│ MQTT broker│
│ inverter │  Modbus RTU│ deyemqtt │   JSON    │  (Mosquitto)│
└──────────┘   9600 8N1 └──────────┘           └────────────┘
```

## Why block reads (the interesting part)

Several Deye counters are 32-bit values split across two consecutive 16-bit
registers, low word first — `516`/`517` (total battery charge), `518`/`519`
(total discharge), `527`/`528` (total load energy).

Reading them as two separate Modbus requests is legal, but wrong in two ways:

1. **It is not atomic.** If the low word rolls over between the two requests,
   the composed value is short by 65536 — a 6553.6 kWh jump in your energy
   history.
2. **It doubles the traffic.** Deye inverters are known to lock up their Modbus
   port under dense polling, and every extra transaction is another chance to
   collide with something.

So the register map is organised into **blocks of contiguous addresses**, and
each block is fetched with a single `0x03` request. That takes this
installation from 15 transactions per pass down to 5, and each 32-bit value now
comes from one snapshot. Each block also has its own poll period: energy
counters every 15 minutes, live power every 20 seconds — roughly 200
transactions per hour instead of 4300.

If your inverter still locks up, raise the `period` values in `registers.py`
first; that is the knob that matters.

## Hardware

| Part | Notes |
|---|---|
| ESP32-S3 (DevKitC-1 or similar) | tested on `ESP32_GENERIC_S3-SPIRAM_OCT`, MicroPython 1.26 |
| RS485 transceiver | any 3.3 V module; auto-direction ones need no DE pin |
| Deye hybrid inverter | Modbus RTU, 9600 8N1, slave address 1 |

Default wiring (all configurable in `cfg.py`):

| Signal | GPIO |
|---|---|
| UART1 TX → RS485 DI | 9 |
| UART1 RX ← RS485 RO | 8 |
| RS485 DE/RE | unused (`RS485_DE_PIN = None`) |
| onboard RGB LED | 48 |

Check your inverter's manual for which connector exposes Modbus RTU and at what
baud rate — it differs between models and firmware versions.

## Install

```bash
# 1. flash MicroPython (once)
esptool.py --chip esp32s3 write_flash 0 ESP32_GENERIC_S3-SPIRAM_OCT-*.bin

# 2. MQTT client library
mpremote mip install umqtt.simple

# 3. configuration — cfg.py is gitignored, it holds your WiFi password
cp cfg.example.py cfg.py
$EDITOR cfg.py

# 4. upload
mpremote fs cp boot.py main.py cfg.py registers.py modbusrtu.py rgbled.py uftpd.py :
mpremote reset
```

Once running, `uftpd` serves FTP on port 21, so later updates can be uploaded
over WiFi without unplugging the board from the inverter. It has no
authentication — set `FTP_ENABLED = False` on networks you do not control.

## Configuration

All settings live in `cfg.py`; `cfg.example.py` documents every field. The
register map is separate, in `registers.py`, because it contains no secrets and
is meant to be version controlled and extended.

Adding a register means adding a field to a block — or a new block if the
address is not adjacent to an existing one:

```python
{
    "start": 672,
    "period": 20,
    "fields": (
        ("pv1_power", "u16", 1, "W", "PV1 input power"),
        ("pv2_power", "u16", 1, "W", "PV2 input power"),
    ),
},
```

Field types are `u16`, `s16`, `u32`, `s32`; the 32-bit ones consume two
registers and assume Deye's low-word-first order. Use `None` as the name to
skip a register that sits in the middle of a block (register 589 is such a
hole). `scale` multiplies the raw value — `0.1` for Deye's `0.1 kWh` counters.

## MQTT

### Measurements

Published to `<TOPIC_BASE>/<field>` with `retain=True`, so a fresh subscriber
immediately sees the last known state:

```json
{"value": -400, "unit": "W", "reg": 590, "uptime": 8461, "ts": 1787028801}
```

`uptime` is seconds since boot and is always present. `ts` is a Unix timestamp
and appears only after a successful NTP sync — without it, stamp the data with
the receiver's own clock.

| MQTT topic | Modbus reg | Type | Unit | Poll | Description |
|---|---|---|---|---|---|
| `battery_charge_today` | 514 | u16 | kWh | 900 s | Today charge of the battery |
| `battery_discharge_today` | 515 | u16 | kWh | 900 s | Today discharge of the battery |
| `battery_charge_total` | 516+517 | u32 | kWh | 900 s | Total charge of the battery |
| `battery_discharge_total` | 518+519 | u32 | kWh | 900 s | Total discharge of the battery |
| `load_energy_total` | 527+528 | u32 | kWh | 900 s | Total load energy |
| `battery_soc` | 588 | u16 | % | 20 s | Battery capacity (SOC) |
| `battery_power` | 590 | s16 | W | 20 s | Battery output power (positive = discharging) |
| `load_power_l1` | 650 | s16 | W | 20 s | Load phase power A |
| `load_power_l2` | 651 | s16 | W | 20 s | Load phase power B |
| `load_power_l3` | 652 | s16 | W | 20 s | Load phase power C |
| `pv1_power` | 672 | u16 | W | 20 s | PV1 input power |
| `pv2_power` | 673 | u16 | W | 20 s | PV2 input power |

Register numbers and units are taken from Deye's *Modbus protocol* document
(the modbus register table shipped with the inverter). The document is not
redistributed here.

With `LEGACY_REG_TOPICS = True` every single-register field is additionally
published under `<TOPIC_BASE>/regs/<number>` in the older payload shape, so
existing consumers keep working during a migration. Turn it off once they use
the named topics.

### Diagnostics

`<TOPIC_DIAG>/<DEVID>` every 60 seconds. This is what tells you whether the
board is quietly rebooting or the RS485 link is degrading:

```json
{
  "device": "deye_ddeeff", "uptime": 8461, "reset_cause": "power_on",
  "mem_free": 4000000, "mem_alloc": 200000, "wifi": true, "mqtt": true,
  "rssi": -55, "time_synced": true, "queue_len": 0, "queue_dropped": 0,
  "mqtt_reconnects": 1, "mqtt_last_ok_s": 0, "publish_errors": 0,
  "cycles": 423, "blocks_ok": 1690, "blocks_failed": 2,
  "modbus_fail_streak": 0, "modbus_last_ok_s": 3,
  "modbus": {"ok": 1690, "timeout": 2, "crc": 0, "exception": 0, "malformed": 0},
  "beats": {"wifi": 0, "mqtt": 0, "deye": 3, "diag": 0, "discovery": 12, "clock": 7}
}
```

A `reset_cause` of `watchdog` plus a low `uptime` means something is hanging.
A rising `modbus.crc` count points at wiring or termination; rising
`modbus.timeout` at the inverter or the baud rate.

### Reset counters

`<TOPIC_DIAG>/<DEVID>/resets`, retained, refreshed every
`RESET_COUNTER_PUBLISH_S`:

```json
{
  "device": "deye_ddeeff", "total": 58,
  "by_cause": {"power_on": 4, "watchdog": 54},
  "by_reason": {"watchdog_stale": 54},
  "last_cause": "power_on", "last_reason": null,
  "prev_uptime": 302, "max_uptime": 900,
  "since": 1780000000, "uptime": 7
}
```

`by_cause` counts what `machine.reset_cause()` reported. `by_reason` counts the
reboots **this firmware chose itself**, which the reset cause cannot tell you —
`machine.reset()` looks like a plain soft reset whether it came from a dead
broker or from the REPL. The reasons are `mqtt_dead`, `modbus_dead`,
`watchdog_stale` (heartbeat starvation) and `mqtt_command`; the intent is
written to flash *before* rebooting and counted on the next boot.

`prev_uptime` is how long the previous run lasted and `max_uptime` the record —
together they answer "is this thing actually stable?" at a glance. For a
deliberate reboot the value is exact; otherwise it comes from the `uptime` in
the last retained publication of the previous run, so a power cut is covered too,
accurate to `RESET_COUNTER_PUBLISH_S`.

#### Where the counters are stored

By default the retained MQTT message **is** the storage: the board reads it at
boot, adds this run and publishes it back. Nothing is written to flash.

Know what that costs you. Retained messages survive a broker restart only with
`persistence true` (the Debian `mosquitto` package sets it), and the on-disk
dump happens every `autosave_interval`, so an abrupt power loss on the broker's
host can drop the most recent increments. Clear a retained message and the
history is gone. For counters that is usually an acceptable trade.

Set `RESET_COUNTER_PERSIST_FLASH = True` if you would rather not depend on the
broker. A file (`RESET_COUNTER_FILE`) then becomes the source of truth and the
retained message a copy — the counters survive a wiped broker, and if the
board's filesystem is reflashed the retained copy is adopted back when its
`total` is higher. The cost is one flash write per boot and one per recorded
reset reason.

The reset *reason* follows whichever backend is active. With flash off there is
nowhere local to leave a note, so `mark_reset_reason()` publishes the retained
message **immediately, bypassing the publish queue** — after `machine.reset()`
the queue would never get a chance to drain. If the broker is unreachable at
that moment the reason is lost and the reboot is only counted by cause, which is
unavoidable: `mqtt_dead` means the broker was unreachable by definition.

Either way the board waits for the broker connection first
(`RESET_COUNTER_WAIT_MQTT_S`) and only then for the retained message. Counting
that wait from boot instead would let a slow WiFi association eat the window and
silently lose the history.

### Presence

`<TOPIC_DISCOVERY>` every 60 seconds, `retain=False`:

```json
{"name": "deye_ddeeff", "ip": "192.168.63.99", "uptime": 7, "ts": 1787028801}
```

### Commands

Subscribed topics. Both require `name` to match this device's `DEVID`, so one
broker can serve many boards.

| Topic | Payload | Effect |
|---|---|---|
| `cave/cfg/reboot/now` | `{"name": "deye_xxxxxx"}` | `machine.reset()` |
| `cave/cfg/deye/write` | `{"name": "deye_xxxxxx", "reg": 145, "value": 1}` | Modbus `0x06` write |

Writing is **disabled by default**. Enabling it takes two deliberate steps —
`MODBUS_WRITE_ENABLED = True` *and* listing the address in
`MODBUS_WRITE_ALLOWED` — because a write to the wrong register changes how your
installation operates. The result is reported on
`<TOPIC_BASE>/write_result/<reg>`:

```json
{"reg": 145, "value": 1234, "uptime": 12, "ok": true}
```

The bus is guarded by a lock, so reads, MQTT-triggered writes and the clock sync
can never interleave a request with someone else's response.

## Inverter clock

The inverter's real-time clock drifts, and it schedules charge windows by that
clock, so it is worth correcting — carefully. Registers `62`–`64` hold six bytes
of time, two per register:

| Register | High byte | Low byte |
|---|---|---|
| 62 | year − 2000 | month |
| 63 | day | hour |
| 64 | minute | second |

`clock_task` reads them every `CLOCK_CHECK_INTERVAL_S` (default hourly) and
publishes to `<TOPIC_BASE>/clock`:

```json
{
  "uptime": 3600, "sync_enabled": false,
  "inverter_time": "2026-08-18 07:27:52",
  "local_time": "2026-08-18 07:33:01",
  "drift_s": -309, "action": "pominięte: CLOCK_SYNC_ENABLED=False"
}
```

**Reading always works; only the correction is gated.** That is deliberate:
before enabling any write, compare the published `inverter_time` against what
the inverter's own display shows. If they match, the byte order assumed here is
right for your firmware — if they do not, fix `_decode_clock` before letting it
write anything.

To enable correction set `CLOCK_SYNC_ENABLED = True`. A write then happens only
when all of these hold:

- the board's own clock is NTP-synced (otherwise there is nothing to trust),
- `|drift| > CLOCK_MAX_DRIFT_S` (default 60 s),
- at least `CLOCK_SYNC_INTERVAL_S` since the last write (default 30 days),
- the year fits the register's range.

All three registers go out in **one `0x10` transaction**, so the inverter never
sees a half-updated timestamp, and the result is read back and verified —
`drift_after_s` in the payload tells you whether it took.

The inverter wants **local** time. After NTP the board runs on UTC, so
`TZ_OFFSET_S` (standard offset, `3600` for Poland) plus `TZ_DST_EU` (EU daylight
saving rule: last Sunday of March 01:00 UTC to last Sunday of October 01:00 UTC)
are applied before writing. Set `TZ_DST_EU = False` for a fixed offset.

## How the watchdog works

A plain "feed the watchdog in its own task" only proves the asyncio loop is
alive. If the Modbus task dies, the loop keeps feeding and the board never
recovers.

Here every task calls `heartbeat(name)` on each pass, and `wdt_task` feeds the
hardware watchdog **only while no heartbeat is stale** (`TASK_DEADLINE_MS`).
A dead task therefore stops the feeding and the board reboots.

That design has one trap, and it bit this firmware in production: a task that
sleeps longer than the deadline looks exactly like a hung one. `clock_task`
sleeps for an hour, so the board rebooted every ~355 seconds while everything
was perfectly healthy. Long sleeps therefore go through
`sleep_beating(name, seconds)`, which chops the sleep into 30-second slices and
beats after each — the watchdog keeps its short deadline and still detects a
genuine hang.

Two things make this visible instead of mysterious next time:

- the diagnostics payload carries `beats`, the age in seconds of every task's
  last heartbeat — a value climbing toward `TASK_DEADLINE_MS` is the warning,
- before it stops feeding, `wdt_task` publishes to
  `<TOPIC_DIAG>/<DEVID>/alert` naming the stale tasks. The hardware watchdog
  gives it `WDT_TIMEOUT_MS` to get that message out, which is plenty.

Two failure modes a watchdog cannot see are handled separately, because the
tasks are formally alive:

- `REBOOT_AFTER_NO_MQTT_S` — nothing reached the broker for that long, which
  usually means a wedged WiFi stack. Default 30 minutes.
- `REBOOT_AFTER_NO_MODBUS_S` — the inverter stopped answering. Default `0`
  (off) on purpose: rebooting the ESP32 will not un-hang an inverter, and you
  would lose contact with a device that is otherwise fine.

The watchdog is armed `WDT_START_DELAY_S` seconds after boot, so a bad upload
leaves you a window to get in over FTP instead of fighting a reset loop.

## Files

| File | Role |
|---|---|
| `main.py` | tasks: WiFi, MQTT, polling, clock, diagnostics, LED, watchdog |
| `registers.py` | register map — blocks, types, scaling, poll periods |
| `modbusrtu.py` | Modbus RTU master: `0x03`, `0x06`, `0x10` + framing/CRC |
| `cfg.example.py` | configuration template (copy to `cfg.py`) |
| `rgbled.py` | onboard NeoPixel status LED |
| `uftpd.py` | FTP server for over-the-air file updates |
| `boot.py` | empty by default |

Status LED: red = no WiFi, blue = WiFi but no broker, green = running,
short blue/red flash = Modbus transaction succeeded/failed.

## Credits

- `uftpd.py` — FTP server by Robert Hammelrath and contributors, MIT licence:
  [robert-hh/FTP-Server-for-ESP8266-ESP32-and-PYBD](https://github.com/robert-hh/FTP-Server-for-ESP8266-ESP32-and-PYBD)
- `umqtt.simple` — from
  [micropython-lib](https://github.com/micropython/micropython-lib)

Part of [pradki/cave](https://github.com/pradki/cave). Comments in the source
are in Polish.
