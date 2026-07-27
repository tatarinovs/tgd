import os
import random
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


def try_use_cryptography_module():
    class CryptographyEncryptorAdapter:
        __slots__ = ('encryptor', 'decryptor')

        def __init__(self, cipher):
            self.encryptor = cipher.encryptor()
            self.decryptor = cipher.decryptor()

        def encrypt(self, data):
            return self.encryptor.update(data)

        def decrypt(self, data):
            return self.decryptor.update(data)

    def create_aes_ctr(key, iv):
        iv_bytes = int.to_bytes(iv, 16, "big")
        cipher = Cipher(algorithms.AES(key), modes.CTR(
            iv_bytes), default_backend())
        return CryptographyEncryptorAdapter(cipher)

    def create_aes_cbc(key, iv):
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), default_backend())
        return CryptographyEncryptorAdapter(cipher)

    return create_aes_ctr, create_aes_cbc


create_aes_ctr, create_aes_cbc = try_use_cryptography_module()


class MyRandom(random.Random):
    """Источник случайности для FakeTLS-рукопожатия.

    Раньше здесь был AES-CTR поверх Mersenne Twister из `random`: MT
    предсказуем по нескольким выходам, а session_id и x25519-ключ в
    ClientHello — это ровно те данные, по которым DPI-наблюдатель может
    отличить поддельный TLS от настоящего. Теперь всё берётся из os.urandom
    (CSPRNG операционной системы)."""

    def getrandbits(self, k):
        numbytes = (k + 7) // 8
        return int.from_bytes(self.getrandbytes(numbytes), 'big') >> (numbytes * 8 - k)

    def getrandbytes(self, n):
        return os.urandom(n)


myrandom = MyRandom()


def gen_x25519_public_key() -> bytes:
    # generates some number which has square root by modulo P
    P = 2 ** 255 - 19
    n = myrandom.randrange(P)
    return int.to_bytes((n * n) % P, length=32, byteorder="little")
