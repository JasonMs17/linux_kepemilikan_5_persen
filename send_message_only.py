import requests
import os
from dotenv import load_dotenv

load_dotenv()

def send_message_only(title, message):
    try:
        url = "https://lsayljbjdmwmmwzomyyt.supabase.co/functions/v1/send-simple-message"
        payload = {
            "title": title,
            "message": message
        }
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('SERVICE_KEY')}"}   
        print("ENV SUPABASE_KEY =", os.getenv("SERVICE_KEY"))
        print("➡ Mengirim notifikasi ke Supabase Edge Function...")
        print(f"   URL: {url}")
        print(f"   Payload: {payload}")
        print(f"   Headers: Content-Type: {headers['Content-Type']}, Authorization: [HIDDEN]")

        r = requests.post(url, json=payload, headers=headers, timeout=10)

        print("📨 Response received:")
        print(f"   Status Code: {r.status_code}")
        print(f"   Response Text: {r.text}")
        print(f"   Response Headers: {dict(r.headers)}")

    except requests.exceptions.Timeout as e:
        print(f"❌ Timeout error: {e}")
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection error: {e}")
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error: {e}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {type(e).__name__}: {e}")

# Example usage
if __name__ == "__main__":
    send_message_only(
        title="Test",
        message="Test"
    )