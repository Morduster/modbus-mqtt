# registers.py - mapa rejestrów falownika Deye (Modbus RTU, funkcja 0x03)
# https://github.com/pradki/deye-modbus-mqtt
#
# Rejestry są pogrupowane w BLOKI o ciągłych adresach. Jeden blok = jedno
# zapytanie Modbus, co daje trzy rzeczy:
#
#   1. Mniej transakcji na RS485 (15 -> 5), a falownik Deye nie lubi gęstego
#      pollingu - zbyt częste odpytywanie potrafi zawiesić mu port Modbus.
#   2. Atomowość wartości 32-bitowych: oba słowa pochodzą z tej samej migawki.
#      Przy dwóch osobnych odczytach młodsze słowo mogło się przekręcić
#      pomiędzy nimi i wynik byłby zaniżony o 65536 (czyli o 6553.6 kWh).
#   3. Każdy blok ma własny okres odpytywania - energie całkowite nie muszą być
#      czytane częściej niż raz na kwadrans, moce chcemy co ~20 s.
#
# Definicja pola: (name, type, scale, unit, desc)
#
#   name  - nazwa użyta w topiku MQTT (<TOPIC_BASE>/<name>).
#           None = rejestr pomijany, służy do przeskoczenia dziury w bloku.
#   type  - "u16" | "s16" | "u32" | "s32"
#           u32/s32 zajmują DWA kolejne rejestry. Deye trzyma młodsze słowo pod
#           niższym adresem (low word first) - patrz Modbus.Deye.pdf, str. 25-26.
#   scale - mnożnik surowej wartości (0.1 dla rejestrów w jednostkach 0.1 kWh).
#   unit  - jednostka PO przeskalowaniu.
#   desc  - opis z dokumentacji Deye, używany w README i w starych topikach.

# Ile rejestrów zajmuje dany typ
REG_WIDTH = {"u16": 1, "s16": 1, "u32": 2, "s32": 2}

BLOCKS = (
    {
        "start": 514,
        "period": 900,          # energie licznikowe - raz na 15 minut wystarczy
        "fields": (
            ("battery_charge_today",     "u16", 0.1, "kWh", "Today charge of the battery"),
            ("battery_discharge_today",  "u16", 0.1, "kWh", "Today discharge of the battery"),
            ("battery_charge_total",     "u32", 0.1, "kWh", "Total charge of the battery"),
            ("battery_discharge_total",  "u32", 0.1, "kWh", "Total discharge of the battery"),
        ),
    },
    {
        "start": 527,
        "period": 900,
        "fields": (
            ("load_energy_total", "u32", 0.1, "kWh", "Total_Load_Power Wh"),
        ),
    },
    {
        "start": 588,
        "period": 20,
        "fields": (
            ("battery_soc",   "u16", 1, "%", "Battery capacity (SOC)"),
            (None,            "u16", 1, "",  "589 undefined - pomijany"),
            ("battery_power", "s16", 1, "W", "Battery output power (dodatnie = rozładowanie)"),
        ),
    },
    {
        "start": 650,
        "period": 20,
        "fields": (
            ("load_power_l1", "s16", 1, "W", "Load phase power A"),
            ("load_power_l2", "s16", 1, "W", "Load phase power B"),
            ("load_power_l3", "s16", 1, "W", "Load phase power C"),
        ),
    },
    {
        "start": 672,
        "period": 20,
        "fields": (
            ("pv1_power", "u16", 1, "W", "PV1 input power"),
            ("pv2_power", "u16", 1, "W", "PV2 input power"),
        ),
    },
    # Zegar falownika - odkomentuj, jeśli chcesz go publikować. Wartości są
    # pakowane po dwa bajty na rejestr (rok/miesiąc, dzień/godzina,
    # minuta/sekunda), więc surowa liczba nie jest bezpośrednio czytelna.
    # {
    #     "start": 62,
    #     "period": 3600,
    #     "fields": (
    #         ("clock_year_month",    "u16", 1, "raw", "System time: year, month"),
    #         ("clock_day_hour",      "u16", 1, "raw", "System time: day, hour"),
    #         ("clock_minute_second", "u16", 1, "raw", "System time: minute, second"),
    #     ),
    # },
)


def _validate():
    """Sanity check mapy - wywoływany raz przy starcie, łapie literówki w BLOCKS."""
    names = set()
    for block in BLOCKS:
        for name, typ, _scale, _unit, _desc in block["fields"]:
            if typ not in REG_WIDTH:
                raise ValueError("registers.py: nieznany typ %r w bloku %d" % (typ, block["start"]))
            if name is None:
                continue
            if name in names:
                raise ValueError("registers.py: zduplikowana nazwa %r" % name)
            names.add(name)


def block_size(block):
    """Ile rejestrów trzeba przeczytać dla całego bloku."""
    return sum(REG_WIDTH[f[1]] for f in block["fields"])


def decode_block(block, words):
    """Zamienia listę surowych rejestrów (u16) na listę pól.

    Zwraca listę krotek (name, address, value, unit, desc) - pola z name=None
    są pomijane. Rzuca ValueError, jeśli słów jest mniej niż potrzeba.
    """
    need = block_size(block)
    if len(words) < need:
        raise ValueError("blok %d: %d słów, oczekiwano %d" % (block["start"], len(words), need))

    out = []
    offset = 0
    for name, typ, scale, unit, desc in block["fields"]:
        width = REG_WIDTH[typ]
        if name is None:
            offset += width
            continue

        if width == 1:
            raw = words[offset]
            if typ == "s16" and raw & 0x8000:
                raw -= 0x10000
        else:
            # low word first - patrz komentarz na górze pliku
            raw = (words[offset + 1] << 16) | words[offset]
            if typ == "s32" and raw & 0x80000000:
                raw -= 0x100000000

        value = raw if scale == 1 else round(raw * scale, 3)
        out.append((name, block["start"] + offset, value, unit, desc))
        offset += width

    return out


_validate()

# end.
