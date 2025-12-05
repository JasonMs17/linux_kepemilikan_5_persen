import cloudscraper
import datetime
import subprocess
from pathlib import Path

# --- Setup folder untuk PDF Lamp1 ---
lamp1_folder = Path("5_persen/pdf")
lamp1_folder.mkdir(parents=True, exist_ok=True)

# --- Setup cloudscraper ---
scraper = cloudscraper.create_scraper()

# --- Fungsi untuk download dan extract PDF Lamp1 ---
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
        
        # Panggil extract_pdf_to_csv.js
        print(f"🔄 Menjalankan extract_pdf_to_csv.js...")
        result = subprocess.run(
            ["node", "extract_pdf_to_csv.js"],
            cwd="5_persen",
            capture_output=True,
            text=True,
        )
        
        if result.returncode == 0:
            print("✔ Ekstraksi PDF berhasil")
            if result.stdout:
                print(result.stdout)
        else:
            print("❌ Ekstraksi PDF gagal")
            if result.stderr:
                print(result.stderr)
    except Exception as e:
        print(f"❌ Error processing Lamp1: {e}")

# --- Main untuk testing ---
if __name__ == "__main__":
    print(f"=== Test Download PDF - {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===\n")
    
    # Contoh format attachments dari scrape.py
    attachments = [
        {
            "Id": 0,
            "FullSavePath": "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202512/309e1b54cb_f64e2be829.pdf",
            "OriginalFilename": "20251201_Semua Emiten Saham_Pengumuman Bursa_31998692.pdf"
        },
        {
            "Id": 0,
            "FullSavePath": "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_EREP/202512/91da4fc53f_47ed4dc276.pdf",
            "OriginalFilename": "20251201_Semua Emiten Saham_Pengumuman Bursa_31998692_lamp1.pdf"
        }
    ]
    
    print("Testing dengan attachments default...")
    print(f"Total attachments: {len(attachments)}\n")
    process_lamp1_pdf(attachments)
    
    print("\n" + "="*60)
    print("Untuk test dengan URL berbeda, ubah variable 'attachments' di atas")
    print("="*60)
