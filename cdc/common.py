import random
import requests
import string

from config import settings


def send_message(message: str):
    if settings.DISCORD_WEBHOOK_URL is not None:
        requests.post(settings.DISCORD_WEBHOOK_URL, json={'content': message})


def generate_random_string(length: int = 4, alphanum: bool = False): return ''.join(random.choice(string.ascii_letters + string.digits + (r'!@#$%^&*()-=_+[]{};\':",./<>?' if not alphanum else '')) for _ in range(length))
