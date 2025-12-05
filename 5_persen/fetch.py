# fetch_and_download_idx.py
# pip install cloudscraper
import json
from urllib.parse import urlencode
from pathlib import Path
import cloudscraper
import time
import datetime

BASE = "https://www.idx.co.id"
SEARCH_API = BASE + "/primary/Search/GetSearch"

def format_date(date_str):
    dt = datetime.datetime.strptime(date_str, "%Y%m%d")
    return dt.strftime("%d%m%y")

def fetch_lamp1_pdfs(scraper, keyword, index_from=0, page_size=10, date_from=None, date_to=None):
    params = {
        "keyword": keyword,
        "indexFrom": index_from,
        "pageSize": page_size,
        "DateFrom": date_from or "",
        "DateTo": date_to or "",
        "SortBy": "date",
        "SortOrder": "desc"
    }
    url = f"{SEARCH_API}?{urlencode(params)}"
    # headers mirip browser + X-Requested-With
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": BASE + "/"
    }
    resp = scraper.get(url, headers=headers, timeout=30)
    print("Search API status:", resp.status_code)
    if resp.status_code != 200:
        print("Response snippet:", resp.text[:800])
        return []
    try:
        data = resp.json()
    except Exception as e:
        print("Failed to parse JSON:", e)
        print("Response snippet:", resp.text[:800])
        return []
    items = data.get("items") or []
    lamp1 = [it for it in items if ("_lamp1" in (it.get("htmlSnippet","") or "") or "_lamp1" in (it.get("link","") or ""))]
    links = [it.get("link") for it in lamp1 if it.get("link")]
    return links

def download_pdf(scraper, url, out_folder="pdf", date_from=""):
    Path(out_folder).mkdir(parents=True, exist_ok=True)
    filename = f"{format_date(date_from)}.pdf"
    out_path = Path(out_folder) / filename
    try:
        # gunakan stream untuk file besar
        with scraper.get(url, stream=True, timeout=60) as r:
            print(f"Downloading {url} -> {out_path} (status {r.status_code})")
            if r.status_code != 200:
                print("Failed to download:", r.status_code, r.text[:400])
                return False
            with open(out_path, "wb") as f:
                for chunk in r.iter_content(8192):
                    if chunk:
                        f.write(chunk)
        print("Saved:", out_path)
        return True
    except Exception as e:
        print("Download error:", e)
        return False

def main():
    # params
    keyword = "5%"
    # Auto get today's date and tomorrow's date
    today = datetime.datetime.now()
    tomorrow = today + datetime.timedelta(days=1)
    date_from = today.strftime("%Y%m%d") 
    date_to = tomorrow.strftime("%Y%m%d")
    index_from = 0
    page_size = 10

    # create cloudscraper session
    scraper = cloudscraper.create_scraper(
        browser={'custom': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'}
    )

    # step 0: visit homepage to get cookies / possible JS challenge solved
    try:
        h = scraper.get(BASE + "/", timeout=30)
        print("Homepage status:", h.status_code)
        # small wait to mimic human behavior
        time.sleep(0.8)
    except Exception as e:
        print("Homepage request failed:", e)

    # fetch lamp1 pdf links
    links = fetch_lamp1_pdfs(scraper, keyword, index_from, page_size, date_from, date_to)
    print("Found links:", links)

    # save json
    with open("lamp1_pdfs.json", "w", encoding="utf-8") as fh:
        json.dump(links, fh, indent=2, ensure_ascii=False)

    # download only the first PDF if available
    if links:
        link = links[0]
        # some links might be relative — make absolute
        if link.startswith("/"):
            link = BASE + link
        download_pdf(scraper, link, date_from=date_from)
    else:
        print("No Lamp1 links found to download.")

if __name__ == "__main__":
    main()
