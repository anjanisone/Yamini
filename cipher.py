from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
import hashlib


def create_key(key: str) -> bytes:
    b = key.encode("utf-8")
    return b if len(b) == 32 else hashlib.sha256(b).digest()


def encrypt(text: str, key: bytes) -> bytes:
    iv = bytes(16)
    aes = AES.new(key, AES.MODE_CBC, iv)
    return aes.encrypt(pad(text.encode("utf-8"), 16))


def decrypt(ciphertext: bytes, key: bytes) -> str:
    iv = bytes(16)
    aes = AES.new(key, AES.MODE_CBC, iv)
    return unpad(aes.decrypt(ciphertext), 16).decode("utf-8")
