import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('TOKEN')
CONNECTION_URI = os.getenv('CONNTCTION_URI')