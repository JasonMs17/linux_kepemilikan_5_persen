import os
import requests
from dotenv import load_dotenv

load_dotenv()  # load .env

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

def upsertKepemilikan(data):
    url = f"{SUPABASE_URL}/rest/v1/report_kepemilikan_lima_persen"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates"
    }

    response = requests.post(url, headers=headers, json=data)

    try:
        json_response = response.json()
        # print("Response:", json_response)  # Dikomen agar tidak tampil output panjang
    except:
        print("Raw response:", response.text)

    return response
