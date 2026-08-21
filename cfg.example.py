# cfg.example.py - szablon konfiguracji. Skopiuj do cfg.py i uzupełnij.
#
#     cp cfg.example.py cfg.py
#
# cfg.py jest w .gitignore i NIE trafia do repozytorium - trzymaj tam hasła.
# Mapa rejestrów falownika siedzi osobno, w registers.py (nie ma tam sekretów,
# więc jest wersjonowana).

import machine

# Identyfikator urządzenia - unikalny per płytka, używany w topikach i komendach
DEVID = "deye_" + machine.unique_id().hex()[-6:]

# ---------- WiFi ----------
WIFI_SSID = "twoja-siec"
WIFI_PASS = "twoje-haslo"

# ---------- MQTT ----------
MQTT_BROKER = "192.168.1.10"
MQTT_PORT = 1883
MQTT_KEEPALIVE = 60

# Pomiary lecą na <TOPIC_BASE>/<nazwa_pola>, np. cave/deye/params/battery_soc
TOPIC_BASE = "cave/deye/params"
TOPIC_DIAG = "cave/deye/diag"
TOPIC_DISCOVERY = "cave/discovery"

# Topiki komend (subskrybowane)
TOPIC_CMD_REBOOT = "cave/cfg/reboot/now"    # {"name": "<DEVID>"}
TOPIC_CMD_WRITE = "cave/cfg/deye/write"     # {"name": "<DEVID>", "reg": 145, "value": 1}

# Stare topiki <TOPIC_BASE>/regs/<numer_rejestru> ze starym kształtem payloadu,
# na czas migracji konsumentów na nazwy pól. Lista adresów = publikuj tylko te
# (domyślnie cztery, które realnie ktoś czytał), True = wszystkie pola
# jednorejestrowe, False = żadne. Po przepięciu konsumentów ustaw False.
LEGACY_REG_TOPICS = (588, 590, 672, 673)

# ---------- RS485 / Modbus ----------
UART_ID = 1
UART_TX_PIN = 9
UART_RX_PIN = 8
UART_BAUDRATE = 9600

# Pin kierunku transmisji (DE/RE). None, jeśli konwerter przełącza się sam
# (większość modułów MAX485 z automatyką i wszystkie z układem SP3485).
RS485_DE_PIN = None

MODBUS_SLAVE_ADDR = 1

# Ile czekamy na całą odpowiedź falownika. Deye odpowiada zwykle w 50-200 ms.
MODBUS_TIMEOUT_MS = 500

# Co ile sekund sprawdzamy, którym blokom minął ich okres odpytywania.
# Sam okres jest ustawiany per blok w registers.py.
POLL_TICK_S = 5

# Przerwa między kolejnymi blokami w jednym przebiegu - Deye nie lubi
# transakcji wpychanych plecami jedna w drugą.
BLOCK_GAP_MS = 250

# Po nieudanym odczycie bloku ponawiamy próbę po tylu sekundach, zamiast
# czekać cały okres bloku (który dla energii wynosi 15 minut).
BLOCK_RETRY_S = 60

# ---------- Zapis do falownika (funkcje 0x06 / 0x10) ----------
# UWAGA: zapis w zły rejestr zmienia nastawy pracy instalacji. Domyślnie
# wyłączony, a nawet po włączeniu przechodzą tylko adresy z białej listy.
MODBUS_WRITE_ENABLED = False
MODBUS_WRITE_ALLOWED = ()       # np. (145, 146)

# ---------- Watchdog ----------
# Sprzętowy WDT resetuje płytkę, jeśli pętla asyncio przestanie go karmić.
# Karmimy go tylko wtedy, gdy WSZYSTKIE krytyczne taski zgłaszają aktywność.
WDT_TIMEOUT_MS = 55_000

# Task uznajemy za zawieszony, jeśli nie odbił się od tyle ms.
TASK_DEADLINE_MS = 300_000

# WDT startuje z opóźnieniem, żeby po nieudanym wgraniu kodu zdążyć wejść
# po FTP zamiast walczyć z pętlą resetów.
WDT_START_DELAY_S = 60

# Reset, gdy od tylu sekund nic nie udało się wypchnąć na brokera
# (ratunek na zawieszony stos WiFi). 0 = wyłączone.
REBOOT_AFTER_NO_MQTT_S = 1800

# Reset, gdy falownik nie odpowiada od tylu sekund. 0 = wyłączone i taki jest
# sensowny domyślny wybór: reset ESP32 nie odwiesi falownika, a stracimy
# kontakt z urządzeniem, które poza tym działa poprawnie.
REBOOT_AFTER_NO_MODBUS_S = 0

# ---------- Czas ----------
# Synchronizacja NTP jest opcjonalna. Bez niej payload nie zawiera pola "ts",
# a odbiorca stempluje dane własnym zegarem.
NTP_ENABLED = True
NTP_HOST = "pool.ntp.org"

# ---------- Strefa czasowa i zegar falownika ----------
# Falownik oczekuje czasu LOKALNEGO - po nim planuje okna ładowania.
TZ_OFFSET_S = 3600              # czas standardowy: Polska = UTC+1
TZ_DST_EU = True                # dolicz czas letni wg reguły unijnej

# Jak często czytać i publikować zegar falownika (odczyt jest nieszkodliwy).
CLOCK_CHECK_INTERVAL_S = 3600

# Korekta zegara zapisem 0x10 do rejestrów 62-64. Domyślnie WYŁĄCZONA.
# Zanim włączysz, sprawdź na topiku <TOPIC_BASE>/clock, czy odczytany
# "inverter_time" zgadza się z tym, co pokazuje wyświetlacz falownika - to
# potwierdza, że kolejność bajtów w rejestrach jest taka, jak zakładamy.
CLOCK_SYNC_ENABLED = False

# Nie koryguj, dopóki rozjazd jest mniejszy niż tyle sekund.
CLOCK_MAX_DRIFT_S = 60

# Minimalny odstęp między zapisami zegara (30 dni).
CLOCK_SYNC_INTERVAL_S = 2_592_000

# ---------- Licznik resetów ----------
# Liczniki restartów (z podziałem na przyczynę) trzymane w pliku na flashu
# i publikowane z retain na <TOPIC_DIAG>/<DEVID>/resets.
RESET_COUNTER_ENABLED = True
# Gdzie trzymać liczniki. Domyślnie tylko na brokerze, w wiadomości retained -
# zero zapisów do flasha, a przy tych danych utrata przy wyczyszczeniu brokera
# nikogo nie zaboli.
#
# True = plik na flashu jest źródłem prawdy, broker kopią. Licznik przestaje
# wtedy zależeć od brokera (przeżyje też jego wyczyszczenie i brak
# "persistence true"), kosztem jednego zapisu do flasha na każdy start i na
# każde odłożenie powodu restartu.
RESET_COUNTER_PERSIST_FLASH = False
RESET_COUNTER_FILE = "resets.json"      # używane tylko przy PERSIST_FLASH = True

# Ile czekać na połączenie z brokerem, zanim licznik resetów przestanie liczyć
# na odtworzenie historii i zadowoli się plikiem lokalnym.
RESET_COUNTER_WAIT_MQTT_S = 60

# Ile czekać na retained kopię z brokera przy starcie. Służy tylko do
# odtworzenia historii, gdy plik na flashu zniknął (np. po przeflashowaniu).
RESET_COUNTER_SYNC_S = 10

# Jak często odświeżać retained kopię. Wpisane tam "uptime" pozwala następnemu
# startowi ustalić, jak długo trwał poprzedni bieg - także po zaniku zasilania,
# z dokładnością do tego okresu.
RESET_COUNTER_PUBLISH_S = 300

# ---------- Pozostałe ----------
DIAG_INTERVAL_S = 60
DISCOVERY_INTERVAL_S = 60

# Serwer FTP pozwala podmieniać pliki bez odłączania płytki od falownika.
# W sieci, do której masz ograniczone zaufanie, wyłącz - uftpd nie ma haseł.
FTP_ENABLED = True

# end.
