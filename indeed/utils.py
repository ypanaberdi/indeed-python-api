from random import getrandbits, randrange
from ipaddress import IPv4Address
from my_fake_useragent import UserAgent

def getRandomIP():
    bits = getrandbits(32) # generates an integer with 32 random bits
    addr = IPv4Address(bits) # instances an IPv4Address object from those bits
    addr_str = str(addr) # get the IPv4Address object's string representation
    return addr_str


def getRandomUserAgent():
    ua = UserAgent()
    return ua.random()

def getRandomSleepTime(min, max):
    return randrange(min, max, 1)
