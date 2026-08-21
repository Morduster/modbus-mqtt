# registers.py - mapa rejestrów falownika Deye (Modbus RTU, funkcja 0x03)
# https://github.com/pradki/deye-modbus-mqtt
#
# Rejestry są pogrupowane w BLOKI o ciągłych adresach. Jeden blok = jedno
# zapytanie Modbus, co daje trzy rzeczy:
#
#   1. Mniej transakcji na RS485, a falownik Deye nie lubi gęstego pollingu -
#      zbyt częste odpytywanie potrafi zawiesić mu port Modbus.
#   2. Atomowość wartości 32-bitowych: oba słowa pochodzą z tej samej migawki.
#      Przy dwóch osobnych odczytach młodsze słowo mogło się przekręcić
#      pomiędzy nimi i wynik byłby zaniżony o 65536 (czyli o 6553.6 kWh).
#   3. Każdy blok ma własny okres odpytywania - liczniki energii nie muszą być
#      czytane częściej niż raz na kwadrans, moce chcemy co ~20 s.
#
# Warto przy rozbudowie sprawdzić, czy nowy rejestr nie leży PRZY takim, który
# już czytamy - wtedy wchodzi do istniejącego bloku i nie kosztuje ani jednej
# dodatkowej transakcji.
#
# Definicja pola: (name, type, scale, unit, desc[, offset])
#
#   name   - nazwa użyta w topiku MQTT (<TOPIC_BASE>/<name>).
#            None = rejestr pomijany, służy do przeskoczenia dziury w bloku.
#   type   - "u16" | "s16" | "u32" | "s32"
#            u32/s32 zajmują DWA kolejne rejestry. Deye trzyma młodsze słowo pod
#            niższym adresem (low word first) - patrz Modbus.Deye.pdf, str. 25-26.
#   scale  - mnożnik surowej wartości.
#   unit   - jednostka PO przeskalowaniu.
#   desc   - opis wzięty z dokumentacji Deye, dosłownie.
#   offset - opcjonalny, odejmowany PRZED skalowaniem: value = (raw - offset) * scale.
#            Potrzebny dla temperatur, które Deye koduje z przesunięciem 1000.
#
# Uwaga o nazwach: dokumentacja Deye nazywa rejestry 520-535 "..._Power", ale
# kolumna jednostki mówi 0.1kWh - to liczniki ENERGII, nie moce. Nazwy topików
# mówią, czym wartość jest; oryginalna nazwa producenta siedzi w "desc", żeby
# dało się wrócić do PDF-a.

# Ile rejestrów zajmuje dany typ
REG_WIDTH = {"u16": 1, "s16": 1, "u32": 2, "s32": 2}

BLOCKS = (
    # ------------------------------------------------------------------ energia
    # 22 rejestry jednym zapytaniem: bateria, sieć, obciążenie, produkcja PV.
    # To z tego liczy się miesięczne podsumowania i porównuje z fakturami.
    {
        "start": 514,
        "period": 900,
        "fields": (
            ("battery_charge_today",    "u16", 0.1, "kWh", "Today charge of the battery"),        # 514
            ("battery_discharge_today", "u16", 0.1, "kWh", "Today discharge of the battery"),     # 515
            ("battery_charge_total",    "u32", 0.1, "kWh", "Total charge of the battery"),        # 516+517
            ("battery_discharge_total", "u32", 0.1, "kWh", "Total discharge of the battery"),     # 518+519
            ("grid_import_today",       "u16", 0.1, "kWh", "Day_GridBuy_Power Wh"),               # 520
            ("grid_export_today",       "u16", 0.1, "kWh", "Day_GridSell_Power Wh"),              # 521
            ("grid_import_total",       "u32", 0.1, "kWh", "Total_GridBuy_Power Wh"),              # 522+523
            ("grid_export_total",       "u32", 0.1, "kWh", "Total_GridSell_Power Wh"),             # 524+525
            ("load_energy_today",       "u16", 0.1, "kWh", "Day_Load_Power Wh"),                  # 526
            ("load_energy_total",       "u32", 0.1, "kWh", "Total_Load_Power Wh"),                # 527+528
            ("pv_energy_today",         "u16", 0.1, "kWh", "Day_PV_Power Wh"),                    # 529
            ("pv1_energy_today",        "u16", 0.1, "kWh", "Day_PV-1_Power Wh"),                  # 530
            ("pv2_energy_today",        "u16", 0.1, "kWh", "Day_PV-2_Power Wh"),                  # 531
            (None,                      "u16", 1,   "",    "532 Day_PV-3 - nie mam trzeciego stringu"),
            (None,                      "u16", 1,   "",    "533 Day_PV-4 - nie mam czwartego stringu"),
            ("pv_energy_total",         "u32", 0.1, "kWh", "Total PV_power Wh"),                  # 534+535
        ),
    },
    # ------------------------------------------------------- temperatury falownika
    # Deye koduje temperatury z offsetem 1000: °C = (raw - 1000) / 10.
    # Zakres [0,3000] daje -100.0 .. +200.0 °C.
    {
        "start": 540,
        "period": 300,
        "fields": (
            ("inverter_dc_temp",       "u16", 0.1, "°C", "DC transformer temperature", 1000),     # 540
            ("inverter_heatsink_temp", "u16", 0.1, "°C", "Heat sink temperature", 1000),          # 541
        ),
    },
    # ------------------------------------------------------- ostrzeżenia i awarie
    # Prawie zawsze zera. Sens jest w tym, żeby były zapisane ZANIM coś się
    # stanie - po fakcie nie ma jak wrócić po powód spadku produkcji.
    # Znaczenie bitów: Modbus.Deye.pdf, str. 28.
    {
        "start": 553,
        "period": 300,
        "fields": (
            ("warning_word_1", "u16", 1, "bits", "Warning message word 1"),      # 553
            ("warning_word_2", "u16", 1, "bits", "Warning message word 2"),      # 554
            ("fault_word_1",   "u16", 1, "bits", "Fault information word 1"),    # 555
            ("fault_word_2",   "u16", 1, "bits", "Fault information word 2"),    # 556
            ("fault_word_3",   "u16", 1, "bits", "Fault information word 3"),    # 557
            ("fault_word_4",   "u16", 1, "bits", "Fault information word 4"),    # 558
        ),
    },
    # ------------------------------------------------------------------- bateria
    # Temperatura, napięcie i prąd wchodzą tu za darmo - leżą obok SOC i mocy,
    # które i tak czytamy.
    {
        "start": 586,
        "period": 20,
        "fields": (
            ("battery_temp",    "u16", 0.1,  "°C", "Battery temperature", 1000),                  # 586
            ("battery_voltage", "u16", 0.01, "V",  "Battery voltage"),                            # 587
            ("battery_soc",     "u16", 1,    "%",  "Battery capacity (SOC)"),                     # 588
            (None,              "u16", 1,    "",   "589 undefined"),
            ("battery_power",   "s16", 1,    "W",  "Battery output power (dodatnie = rozładowanie)"),  # 590
            ("battery_current", "s16", 0.01, "A",  "Battery output current"),                     # 591
        ),
    },
    # -------------------------------------------------------------------- sieć
    # Moc na przyłączu z przekładnika: mówi wprost, czy w tej chwili bierzemy
    # z sieci, czy oddajemy. Znak potwierdź na wyświetlaczu falownika.
    {
        "start": 625,
        "period": 20,
        "fields": (
            ("grid_power", "s16", 1, "W", "Grid side total power"),              # 625
        ),
    },
    # -------------------------------------------------------------- obciążenie
    {
        "start": 650,
        "period": 20,
        "fields": (
            ("load_power_l1", "s16", 1, "W", "Load phase power A"),              # 650
            ("load_power_l2", "s16", 1, "W", "Load phase power B"),              # 651
            ("load_power_l3", "s16", 1, "W", "Load phase power C"),              # 652
            ("load_power",    "s16", 1, "W", "Load totalpower"),                 # 653
        ),
    },
    # --------------------------------------------------------------- moc stringów
    {
        "start": 672,
        "period": 20,
        "fields": (
            ("pv1_power", "u16", 1, "W", "PV1 input power"),                     # 672
            ("pv2_power", "u16", 1, "W", "PV2 input power"),                     # 673
        ),
    },
    # Zegar falownika obsługuje osobny task w main.py (czyta i opcjonalnie
    # koryguje rejestry 62-64), więc nie ma go w tej mapie.
)


def _validate():
    """Sanity check mapy - wywoływany raz przy starcie, łapie literówki w BLOCKS."""
    names = set()
    for block in BLOCKS:
        for field in block["fields"]:
            if len(field) not in (5, 6):
                raise ValueError("registers.py: pole %r ma %d elementów, oczekiwano 5 lub 6"
                                 % (field[0], len(field)))
            name, typ = field[0], field[1]
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
    for field in block["fields"]:
        name, typ, scale, unit, desc = field[:5]
        zero = field[5] if len(field) > 5 else 0
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

        raw -= zero
        value = raw if scale == 1 else round(raw * scale, 3)
        out.append((name, block["start"] + offset, value, unit, desc))
        offset += width

    return out


_validate()

# end.
