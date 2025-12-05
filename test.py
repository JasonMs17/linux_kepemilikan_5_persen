import requests
import cloudscraper
import json
import datetime
import os
from supabase import create_client, Client
from dotenv import load_dotenv

# --- 1. Konfigurasi Awal ---
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
    "kodeEmiten": "",
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

# --- Ambil data dari Supabase untuk hari ini ---
print(f"Mengambil data dari Supabase untuk hari ini ({today})...")
try:
    response = (
        supabase.table("idx_keterbukaan_informasi")
        .select("tanggal, judul")
        .gte("tanggal", start_of_day)
        .lte("tanggal", end_of_day)
        .execute()
    )
    existing_keys = set((item['tanggal'][:10], item['judul']) for item in response.data)
    print(f"Ditemukan {len(existing_keys)} data di database.")
except Exception as e:
    print(f"Gagal mengambil data dari Supabase: {e}")
    existing_keys = set()  # jika gagal, asumsikan kosong

# --- Scraping dan cek duplikat ---
stop_scraping = False

while True:
    if stop_scraping:
        print("Scraping dihentikan karena duplikat ditemukan.")
        break

    print(f"Checking page: {params['indexFrom']}")
    resp = scraper.get(url, params=params)
    resp.raise_for_status()

    data = resp.json()
    replies = data.get("Replies", [])
    if not replies:
        print("Tidak ada balasan lagi, scraping dihentikan.")
        break

    for item in replies:
        peng = item["pengumuman"]
        tanggal = peng.get("TglPengumuman")  # misal '2025-12-01T11:56:05'
        judul = peng.get("JudulPengumuman")
        kode_emiten = peng.get("Kode_Emiten", "").strip()

        # Ambil hanya tanggal YYYY-MM-DD untuk cek duplikat
        key = (tanggal[:10], judul)

        if key in existing_keys:
            print(f"Duplikat ditemukan: {tanggal} - {judul}")
            stop_scraping = True
            break
        else:
            print(f"Pengumuman baru: {tanggal} - {judul}")
            existing_keys.add(key)  # update set lokal agar tidak dicek lagi

        # Simpan semua data untuk JSON
        cleaned_peng = {
            "NoPengumuman": peng.get("NoPengumuman"),
            "TglPengumuman": tanggal,
            "JudulPengumuman": judul,
            "JenisPengumuman": peng.get("JenisPengumuman"),
            "Kode_Emiten": kode_emiten
        }
        cleaned_attachments = [
            {"Id": att.get("Id"),
             "OriginalFilename": att.get("OriginalFilename"),
             "FullSavePath": att.get("FullSavePath")}
            for att in item.get("attachments", [])
        ]
        all_data.append({
            "pengumuman": cleaned_peng,
            "attachments": cleaned_attachments
        })

    params["indexFrom"] += 1

# --- Save ke JSON ---
with open('announcements_today.json', 'w', encoding='utf-8') as f:
    json.dump(all_data, f, indent=2, ensure_ascii=False)

print(f"Total pengumuman yang diproses: {len(all_data)}")
for item in all_data[:5]:  # Print first 5
    peng = item["pengumuman"]
    print("Tanggal:", peng.get("TglPengumuman"))
    print("Judul:", peng.get("JudulPengumuman"))
    print("Kode:", peng.get("Kode_Emiten"))
    print("-------------------------")
