# https://github.com/pradki/deye-modbus-mqtt

from machine import Pin
from neopixel import NeoPixel

class RGBLed:
    _np = None  # instancja neopixel, współdzielona
    _initialized = False

    @classmethod
    def init(cls, pin_num):
        if not cls._initialized:
            cls._pin = Pin(pin_num, Pin.OUT)
            cls._np = NeoPixel(cls._pin, 1)
            cls._initialized = True
            cls.off()

    @classmethod
    def set_color(cls, r, g, b):
        if cls._np is None:
            raise RuntimeError("RGBLed not initialized. Call RGBLed.init(pin_num) first.")
        cls._np[0] = (r, g, b)
        cls._np.write()

    @classmethod
    def get_color(cls):
        if cls._np is None:
            raise RuntimeError("RGBLed not initialized. Call RGBLed.init(pin_num) first.")
        return cls._np[0]

    @classmethod
    def off(cls):
        cls.set_color(0, 0, 0)

    @classmethod
    def red(cls):
        cls.set_color(255, 0, 0)

    @classmethod
    def green(cls):
        cls.set_color(0, 255, 0)

    @classmethod
    def blue(cls):
        cls.set_color(0, 0, 255)

    @classmethod
    def white(cls):
        cls.set_color(255, 255, 255)

    @classmethod
    def color_hex(cls, hex_string):
        if hex_string.startswith('#'):
            hex_string = hex_string[1:]
        if len(hex_string) != 6:
            raise ValueError("Hex color must be 6 characters")
        r = int(hex_string[0:2], 16)
        g = int(hex_string[2:4], 16)
        b = int(hex_string[4:6], 16)
        cls.set_color(r, g, b)


RGBLed.init(48)
RGBLed.set_color(1, 1, 1)

# end.