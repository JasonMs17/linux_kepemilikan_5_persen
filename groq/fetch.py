# simple_cloudscraper_download.py
from pathlib import Path
import cloudscraper

def download_pdf(url, out_folder="pdf", filename=None):
    """Download PDF menggunakan cloudscraper"""
    Path(out_folder).mkdir(parents=True, exist_ok=True)
    if not filename:
        filename = url.split("/")[-1] or "file.pdf"
    out_path = Path(out_folder) / filename

    scraper = cloudscraper.create_scraper(
        browser={'custom': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'}
    )

    try:
        with scraper.get(url, stream=True, timeout=60) as r:
            print(f"Downloading {url} -> {out_path} (status {r.status_code})")
            r.raise_for_status()
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        print("Saved:", out_path)
        return True
    except Exception as e:
        print("Download error:", e)
        return False

def main(url, filename=None):
    download_pdf(url, filename=filename)

if __name__ == "__main__":
    # ganti dengan link PDF yang mau di-download
    url = "https://www.idx.co.id/path/to/lampiran.pdf"
    main(url)
