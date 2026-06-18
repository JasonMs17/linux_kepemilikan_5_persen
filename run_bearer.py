# get_bearer_and_store_ssm_debug.py
from playwright.sync_api import sync_playwright
import os
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from dotenv import load_dotenv
import requests

load_dotenv()

USER_DATA_DIR = os.path.expanduser("~/.config/BraveSoftware/Brave-Browser/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BRAVE_EXECUTABLE = "/usr/bin/brave-browser"

print("=== CONFIG CHECK ===")
print("USER_DATA_DIR:", USER_DATA_DIR)
print("BRAVE_EXECUTABLE:", BRAVE_EXECUTABLE)
print("====================\n")

def setup_aws_credentials():
    """Setup AWS credentials untuk lokal development"""
    try:
        if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
            print("✅ Menggunakan AWS credentials dari environment variables")
            return boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=AWS_REGION
            )

        profile = os.getenv("AWS_PROFILE", "default")
        try:
            session = boto3.Session(profile_name=profile, region_name=AWS_REGION)
            sts = session.client('sts')
            sts.get_caller_identity()
            print(f"✅ Menggunakan AWS profile: {profile}")
            return session
        except:
            pass

        try:
            session = boto3.Session(region_name=AWS_REGION)
            sts = session.client('sts')
            identity = sts.get_caller_identity()
            print(f"✅ Menggunakan IAM role: {identity['Arn']}")
            return session
        except:
            pass

        raise NoCredentialsError()

    except (NoCredentialsError, PartialCredentialsError) as e:
        print("❌ Tidak ada AWS credentials yang valid ditemukan!")
        raise e


def get_bearer_token():
    print("\n=== PLAYWRIGHT START ===")

    if not os.path.exists(BRAVE_EXECUTABLE):
        print("❌ Executable Brave TIDAK ditemukan:", BRAVE_EXECUTABLE)
        raise SystemExit("Brave tidak ditemukan! Cek path executable!")

    print("✔ Brave executable ditemukan")

    if not os.path.exists(USER_DATA_DIR):
        print("⚠ USER_DATA_DIR tidak ditemukan — browser mungkin fresh login:", USER_DATA_DIR)
    else:
        print("✔ USER_DATA_DIR ditemukan")

    with sync_playwright() as p:
        print("Launching browser...")

        try:
            context = p.chromium.launch_persistent_context(
                user_data_dir=USER_DATA_DIR,
                headless=True,
                executable_path=BRAVE_EXECUTABLE,
                args=["--disable-gpu", "--no-sandbox"]
            )
            print("✅ Browser berhasil diluncurkan")
        except Exception as e:
            print("❌ Gagal launch browser:", e)
            raise

        try:
            page = context.new_page()
            print("➡ Membuka halaman stockbit.com ...")

            try:
                page.goto("https://stockbit.com/", timeout=15000)
                print("✔ Halaman stockbit.com terbuka")
            except Exception as e:
                print("❌ Gagal membuka stockbit:", e)

            bearer_token = None

            def handle_request(request):
                nonlocal bearer_token
                # print(f"[REQUEST] {request.method} → {request.url}")

                if "exodus.stockbit.com/stream/v3/user/StockbitReports" in request.url:
                    print("🎯 DAPAT REQUEST STOCKBIT REPORTS")
                    headers = request.headers

                    if "authorization" in headers:
                        bearer_token = headers["authorization"]
                        print("🔥🔥 BEARER TOKEN FOUND:", bearer_token)

            # Pasang listener request
            print("📡 Pasang listener request...")
            page.on("request", handle_request)

            print("➡ Membuka halaman StockbitReports ...")
            try:
                page.goto("https://stockbit.com/StockbitReports", timeout=15000)
                print("✔ Halaman StockbitReports terbuka")
            except Exception as e:
                print("❌ Gagal membuka StockbitReports:", e)

            print("⏳ Menunggu request berjalan...")
            # Cek berkala agar browser bisa diclose lebih cepat jika token sudah didapat
            for _ in range(50):  # Maksimal 5 detik (50 * 100ms)
                if bearer_token:
                    print("🎯 Bearer token didapat, mempercepat penutupan browser...")
                    break
                page.wait_for_timeout(100)

        finally:
            context.close()
            print("=== PLAYWRIGHT END ===\n")

        return bearer_token


def put_token_to_ssm(token, session):
    print("➡ Mengirim token ke AWS SSM...")

    ssm = session.client("ssm", region_name=AWS_REGION)
    ssm.put_parameter(
        Name="stockbit-bearer-token",
        Value=token,
        Type="SecureString",
        Overwrite=True
    )
    print("✅ Token disimpan ke SSM")


def send_notification(title, message):
    try:
        url = "https://lsayljbjdmwmmwzomyyt.supabase.co/functions/v1/send-simple-message"
        payload = {
            "title": title,
            "message": message
        }
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('SERVICE_KEY')}"}

        print("➡ Mengirim notifikasi ke Supabase Edge Function...")
        r = requests.post(url, json=payload, headers=headers, timeout=10)

        print("📨 Response:", r.status_code, r.text)
    except Exception as e:
        print("❌ Gagal mengirim notifikasi:", e)


def send_telegram_notification(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠ TELEGRAM_BOT_TOKEN atau TELEGRAM_CHAT_ID tidak ditemukan di environment!")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }

    try:
        print("➡ Mengirim notifikasi ke Telegram...")
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code == 200:
            print("📨 Notifikasi Telegram berhasil dikirim")
        else:
            print(f"❌ Gagal mengirim ke Telegram (Status {r.status_code}): {r.text}")
    except Exception as e:
        print("❌ Gagal mengirim notifikasi Telegram:", e)


if __name__ == "__main__":
    try:
        aws_session = setup_aws_credentials()
    except Exception as e:
        send_telegram_notification(f"❌ <b>Update Token GAGAL</b>\nGagal setup AWS credentials: {e}")
        raise SystemExit(f"Gagal setup AWS credentials: {e}")

    token = get_bearer_token()
    print("HASIL TOKEN:", token)

    if not token:
        send_telegram_notification("❌ <b>Update Token GAGAL</b>\nGagal menemukan bearer token — Cek log di server/cron!")
        raise SystemExit("❌ Gagal menemukan bearer token — Cek log di atas!")

    try:
        put_token_to_ssm(token, aws_session)
        send_telegram_notification("✅ <b>Update Token Harian Berhasil!</b>\nToken telah berhasil diperbarui dan disimpan di SSM.")
        # send_notification(
        #     title="Update Token Harian",
        #     message="✅ Berhasil"
        # )
    except Exception as e:
        send_notification(
            title="❌ Update Token GAGAL",
            message=f"{e}"
        )
        send_telegram_notification(f"❌ <b>Update Token GAGAL</b>\nGagal menyimpan token ke SSM: {e}")
        raise SystemExit(f"Gagal menyimpan token ke SSM: {e}")
