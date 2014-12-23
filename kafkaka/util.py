import binascii


def crc32(data):
    return binascii.crc32(data) & 0xffffffff

