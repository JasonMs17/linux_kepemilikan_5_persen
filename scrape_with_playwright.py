import sys
import os
import psutil

# --- 1. Your existing imports ---
import requests
import json
import datetime
import os
import time
from supabase import create_client, Client
from dotenv import load_dotenv
import random
import subprocess
from pathlib import Path
from send_message_only import send_message_only
import csv
from summarize_helper import process_summary

# --- NEW: Playwright ---
from playwright.sync_api import sync_playwright

# --- 2. Configuration ---
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise EnvironmentError("SUPABASE_URL dan SUPABASE_KEY harus diset.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

url = "https://www.idx.co.id/primary/ListedCompany/GetAnnouncement"

today = datetime.date.today().strftime("%Y%m%d")
start_of_day = datetime.date.today().strftime("%Y-%m-%d") + "T00:00:00+00"
end_of_day = datetime.date.today().strftime("%Y-%m-%d") + "T23:59:59+00"

params = {
    "kodeEmiten": "*",
    "emitenType": "*",
    "indexFrom": 0,
    "pageSize": 10,
    "dateFrom": today,
    "dateTo": today,
    "lang": "id",
    "keyword": ""
}

all_data = []
to_insert = []
stop_scraping = False

SUMMARY_KEYWORDS = [
    "volatilitas",
    "rapat",
    "dividen",
    "laporan kepemilikan",
    "perubahan pengurus",
    "kembali"
]

SUMMARY_LOG_FILE = "summary_logs.csv"

lamp1_folder = Path("5_persen/pdf")
lamp1_folder.mkdir(parents=True, exist_ok=True)

print(f"=== {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

# --- 3. Ambil data dari Supabase ---
existing_keys = set()
has_5percent_today = False

try:
    response = (
        supabase.table("idx_keterbukaan_informasi")
        .select("tanggal, judul")
        .gte("tanggal", start_of_day)
        .lte("tanggal", end_of_day)
        .execute()
    )

    for item in response.data:
        db_tanggal = item['tanggal']
        normalized_tanggal = db_tanggal.replace(" ", "T").split("+")[0].split("Z")[0]
        judul = item['judul']
        existing_keys.add((normalized_tanggal, judul))

        if "5%" in judul:
            has_5percent_today = True
            print(f"📌 Pengumuman dengan '5%' sudah ada di database: {judul}")
except Exception as e:
    print(f"Gagal mengambil data dari Supabase: {e}")

# --- 3b. Fungsi PDF Lamp1 (TIDAK DIUBAH) ---
def process_lamp1_pdf(attachments):
    lamp1_attachment = None
    for att in attachments:
        filename = att.get("OriginalFilename", "")
        if "lamp1" in filename.lower():
            lamp1_attachment = att
            break

    if not lamp1_attachment:
        print("⚠ Tidak ada attachment dengan 'Lamp1'")
        return

    try:
        url_pdf = lamp1_attachment.get("FullSavePath")
        filename = lamp1_attachment.get("OriginalFilename")

        print(f"📥 Download: {filename}")
        resp = requests.get(url_pdf, stream=True, timeout=30)
        resp.raise_for_status()

        filepath = lamp1_folder / filename
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)

        print(f"✔ Download selesai: {filepath}")

        print(f"🔄 Menjalankan extract_bounding_box.py...")
        result = subprocess.run(
            ["python", "extract_bounding_box.py", str(filepath)],
            cwd="5_persen",
            capture_output=True,
            text=True,
        )

        today_date = datetime.date.today().strftime("%d-%m-%Y")

        if result.returncode == 0:
            print("✔ Ekstraksi PDF berhasil")
            send_message_only("Data kepemilikan 5%", today_date)
        else:
            print("❌ Ekstraksi PDF gagal")
            send_message_only("Data kepemilikan 5% - GAGAL", today_date)

    except Exception as e:
        send_message_only("Data kepemilikan 5% - ERROR", str(e))

# ===============================
# 🔥 PLAYWRIGHT CONTEXT
# ===============================
with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, channel="chromium")
    context = browser.new_context(
        locale="id-ID",
        timezone_id="Asia/Jakarta",
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    )

    page = context.new_page()

    # WAJIB buka homepage dulu (Cloudflare)
    page.goto("https://www.idx.co.id", wait_until="domcontentloaded")
    time.sleep(5)

    # --- 4. Loop Scraping ---
    while True:
        if stop_scraping:
            break

        print(f"Checking page: {params['indexFrom']}")

        try:
            data = page.evaluate(
                """(params) => {
                    const query = new URLSearchParams(params).toString();
                    const url = "https://www.idx.co.id/primary/ListedCompany/GetAnnouncement?" + query;
                    return fetch(url, { credentials: 'include' }).then(r => r.json());
                }""",
                params
            )
        except Exception as e:
            print(f"Playwright fetch error: {e}")
            break

        replies = data.get("Replies", [])
        print(f"Number of replies: {len(replies)}")

        if not replies:
            print("Full response data:", json.dumps(data, indent=2))
            print("Tidak ada balasan lagi, scraping dihentikan.")
            break

        to_insert_page = []

        for item in replies:
            peng = item["pengumuman"]
            tanggal = peng.get("TglPengumuman")
            judul = peng.get("JudulPengumuman")
            kode_emiten = peng.get("Kode_Emiten", "").strip()

            cleaned_attachments = []
            for att in item.get("attachments", []):
                cleaned_attachments.append({
                    "Id": att.get("Id"),
                    "OriginalFilename": att.get("OriginalFilename"),
                    "FullSavePath": att.get("FullSavePath")
                })

            key = (tanggal, judul)
            if key in existing_keys:
                print(f"Duplikat ditemukan: {tanggal} - {judul}")
                stop_scraping = True
                break
            else:
                print(f"Pengumuman baru: {tanggal} - {judul}")

            summary = None

            if "5%" in judul and not has_5percent_today:
                process_lamp1_pdf(cleaned_attachments)

            should_summarize = any(k.lower() in judul.lower() for k in SUMMARY_KEYWORDS)
            if should_summarize:
                target_pdf_url = None
                for att in cleaned_attachments:
                    if att.get("FullSavePath"):
                        target_pdf_url = att.get("FullSavePath")
                        break

                if target_pdf_url:
                    summary_result = process_summary(
                        target_pdf_url, tanggal, judul, kode_emiten
                    )
                    if summary_result["success"]:
                        summary = summary_result["summary"]

            to_insert_page.append({
                "tanggal": tanggal,
                "judul": judul,
                "kode_emiten": kode_emiten,
                "attachment": cleaned_attachments,
                "summary": summary
            })

            existing_keys.add(key)

            all_data.append({
                "pengumuman": peng,
                "attachments": cleaned_attachments
            })

        if to_insert_page:
            supabase.table("idx_keterbukaan_informasi").insert(to_insert_page).execute()

        params["indexFrom"] += 1

    browser.close()

# --- 6. Simpan ke JSON ---
with open('announcements_today.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)

print(f"Total pengumuman yang diproses: {len(all_data)}")
