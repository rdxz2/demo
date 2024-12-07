import dotenv
import os
import requests

dotenv.load_dotenv()

DISCORD_WEBHOOK_URL = os.environ['DISCORD_WEBHOOK_URL']


def send_message(message: str):
    # Send starting message
    requests.post(DISCORD_WEBHOOK_URL, json={'content': message})
