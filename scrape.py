import sys
import os
import psutil

# --- 0. Prevent multiple instances ---
# current_pid = os.getpid()
# script_name = "scrape.py"

# for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
#     try:
#         if proc.info['pid'] != current_pid and proc.info['cmdline'] and script_name in ' '.join(proc.info['cmdline']):
#             print("Another instance is already running. Exiting.")
#             sys.exit()
#     except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
#         continue

# --- 1. Your existing imports ---
import requests
import cloudscraper
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

scraper = cloudscraper.create_scraper()
all_data = []
to_insert = []
stop_scraping = False

# Define keywords for summarization
SUMMARY_KEYWORDS = [
    "volatilitas",
    "rapat",
    "dividen",
    "laporan kepemilikan",
    "perubahan pengurus",
    "kembali"
]

SUMMARY_LOG_FILE = "summary_logs.csv"

# --- 2b. Setup folder untuk PDF Lamp1 ---
lamp1_folder = Path("5_persen/pdf")
lamp1_folder.mkdir(parents=True, exist_ok=True)

print(f"=== {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")

# --- 3. Ambil data dari Supabase untuk hari ini ---
# print(f"Mengambil data dari Supabase untuk hari ini ({today})...")
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
    # Normalisasi format tanggal dari database (hapus spasi dan timezone, gunakan format ISO)
    for item in response.data:
        db_tanggal = item['tanggal']
        # Konversi format "2025-12-01 15:03:13+00" ke "2025-12-01T15:03:13"
        normalized_tanggal = db_tanggal.replace(" ", "T").split("+")[0].split("Z")[0]
        judul = item['judul']
        existing_keys.add((normalized_tanggal, judul))
        
        # Cek apakah sudah ada pengumuman dengan "5%" di database hari ini
        if "5%" in judul:
            has_5percent_today = True
            print(f"📌 Pengumuman dengan '5%' sudah ada di database: {judul}")
except Exception as e:
    print(f"Gagal mengambil data dari Supabase: {e}")

# --- 3b. Fungsi untuk download dan extract PDF Lamp1 ---
def process_lamp1_pdf(attachments):
    """
    Download PDF dengan 'Lamp1' di nama file dan ekstrak menggunakan extract_pdf_to_csv.js
    """
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
        resp = scraper.get(url_pdf, stream=True, timeout=30)
        resp.raise_for_status()
        
        filepath = lamp1_folder / filename
        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(8192):
                if chunk:
                    f.write(chunk)
        print(f"✔ Download selesai: {filepath}")
        
        # Panggil extract_bounding_box.py
        print(f"🔄 Menjalankan extract_bounding_box.py...")
        result = subprocess.run(
            ["python", "extract_bounding_box.py", str(filepath)],
            cwd="5_persen",
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            print("✔ Ekstraksi PDF berhasil")
            if result.stdout:
                print(result.stdout)
            # Kirim notifikasi berhasil
            today_date = datetime.date.today().strftime("%d-%m-%Y")
            send_message_only(
                title="Data kepemilikan 5%",
                message=today_date
            )
        else:
            print("❌ Ekstraksi PDF gagal")
            if result.stderr:
                print(result.stderr)
            # Kirim notifikasi gagal
            today_date = datetime.date.today().strftime("%d-%m-%Y")
            send_message_only(
                title="Data kepemilikan 5% - GAGAL",
                message=f"{today_date} - Ekstraksi PDF gagal"
            )
    except Exception as e:
        print(f"❌ Error processing Lamp1: {e}")
        # Kirim notifikasi error
        today_date = datetime.date.today().strftime("%d-%m-%Y")
        send_message_only(
            title="Data kepemilikan 5% - ERROR",
            message=f"{today_date} - {str(e)}"
        )

# --- 4. Loop Scraping ---
while True:
    if stop_scraping:
        # print("Scraping dihentikan karena duplikat ditemukan.")
        break

    print(f"Checking page: {params['indexFrom']}")
    
    # Retry logic untuk handle 403 atau error lainnya
    max_retries = 3
    retry_delay = 30
    success = False
    
    for attempt in range(max_retries):
        try:
            resp = scraper.get(url, params=params, timeout=15)
            resp.raise_for_status()
            success = True
            break
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request gagal (attempt {attempt + 1}/{max_retries}): {e}")
                print(f"Retry dalam {retry_delay} detik...")
                time.sleep(retry_delay)
            else:
                print(f"Request gagal setelah {max_retries} percobaan: {e}")
                break
    
    if not success:
        break

    data = resp.json()
    replies = data.get("Replies", [])
    print(f"Response status: {resp.status_code}")
    print(f"Number of replies: {len(replies)}")
    if not replies:
        print("Full response data:", json.dumps(data, indent=2))
        print("Tidak ada balasan lagi, scraping dihentikan.")
        break

    to_insert_page = []

    for item in replies:
        peng = item["pengumuman"]
        tanggal = peng.get("TglPengumuman")  # misal '2025-12-01T11:56:05'
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
            
            # Initialize summary
            summary = None
            
            # --- Check untuk "5%" di judul ---
            if "5%" in judul and not has_5percent_today:
                print(f"✅ Ditemukan '5%' di judul (belum ada di DB): {judul}")
                process_lamp1_pdf(cleaned_attachments)
            elif "5%" in judul and has_5percent_today:
                print(f"⏭ Pengumuman '5%' sudah pernah diproses hari ini, skip ekstraksi.")

            # --- Check untuk summarization keywords ---
            should_summarize = any(k.lower() in judul.lower() for k in SUMMARY_KEYWORDS)
            if should_summarize:
                print(f"🔍 Judul mengandung keyword penting, melakukan summarization...")
                # Find PDF attachment (prefer Lamp1 or just the first one)
                target_pdf_url = None
                for att in cleaned_attachments:
                    if att.get("FullSavePath"):
                        target_pdf_url = att.get("FullSavePath")
                        # Prefer lampiran over others if multiple? Usually just take the first valid PDF.
                        if "lamp" in att.get("OriginalFilename", "").lower():
                             break
                
                if target_pdf_url:
                    print(f"   Processing PDF: {target_pdf_url}")
                    summary_result = process_summary(target_pdf_url, tanggal, judul, kode_emiten)
                    if summary_result["success"]:
                        summary = summary_result["summary"]
                    
                    print(f"   ✅ Summary logged to {SUMMARY_LOG_FILE}")
                else:
                    print("   ⚠ No PDF attachment found for summarization.")
            
            to_insert_page.append({
                "tanggal": tanggal,
                "judul": judul,
                "kode_emiten": kode_emiten,
                "attachment": cleaned_attachments,
                "summary": summary
            })
            existing_keys.add(key)

        cleaned_item = {
            "pengumuman": {
                "NoPengumuman": peng.get("NoPengumuman"),
                "TglPengumuman": peng.get("TglPengumuman"),
                "JudulPengumuman": peng.get("JudulPengumuman"),
                "JenisPengumuman": peng.get("JenisPengumuman"),
                "Kode_Emiten": kode_emiten
            },
            "attachments": cleaned_attachments
        }
        all_data.append(cleaned_item)

    # --- 5. Insert batch ke Supabase ---
    if to_insert_page:
        print(f"Memasukkan {len(to_insert_page)} pengumuman baru ke database.")
        try:
            supabase.table("idx_keterbukaan_informasi").insert(to_insert_page).execute()
        except Exception as e:
            print(f"Gagal memasukkan data ke Supabase: {e}")

    params["indexFrom"] += 1

print(f"Total pengumuman yang diproses: {len(all_data)}")
for item in all_data[:5]:
    peng = item["pengumuman"]
    print("Tanggal:", peng.get("TglPengumuman"))
    print("Judul:", peng.get("JudulPengumuman"))
    print("Kode:", peng.get("Kode_Emiten").strip())
    print("-------------------------")
