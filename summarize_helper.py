from io import BytesIO
import cloudscraper
import pdfplumber
from dotenv import load_dotenv
import os
import requests
from groq import Groq
import csv
import datetime
from pdf2image import convert_from_bytes
from PIL import Image
import time
from prompts.default_prompt import default_system_prompt
from prompts.volatilitas_prompt import volatilitas_system_prompt

# Load API key
load_dotenv()
api_key_groq = os.getenv("GROQ_API_KEY")
api_key_ocr = os.getenv("OCR_SPACE_API_KEY")
client = Groq(api_key=api_key_groq)

def fetch_pdf_in_memory(url):
    """Download PDF ke memory tanpa simpan di disk"""
    scraper = cloudscraper.create_scraper(
        browser={'custom': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'}
    )
    try:
        resp = scraper.get(url, stream=True, timeout=60)
        resp.raise_for_status()
        return BytesIO(resp.content)
    except Exception as e:
        print("Error fetching PDF:", e)
        return None

def extract_text_from_pdf(pdf_bytes, debug=False):
    """Ekstrak teks PDF menggunakan pdfplumber, berhenti jika menemukan 'Go to Indonesian Page'"""
    text = ""
    try:
        pdf_bytes.seek(0)  # reset pointer ke awal
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if debug: print(f"Page {pdf.pages.index(page)} text length: {len(page_text or '')}")
                if page_text:
                    # Hentikan ekstraksi saat menemukan kata "Go to Indonesian Page"
                    if "Go to Indonesian Page" in page_text:
                        page_text = page_text.split("Go to Indonesian Page")[0]
                        text += page_text + "\n"
                        break  # stop setelah bagian Indonesia
                    text += page_text + "\n"
    except Exception as e:
        print(f"pdfplumber error: {e}")
    if debug: print(f"Total pdfplumber text length: {len(text)}")
    return text.strip()


def extract_text_with_ocr_space(pdf_bytes, max_pages=5, debug=False):
    """Fallback OCR menggunakan OCR.space API dengan PDF diubah ke image dulu"""
    try:
        pdf_bytes.seek(0)
        images = convert_from_bytes(pdf_bytes.read())
        text = ""
        for i, img in enumerate(images[:max_pages]):
            # simpan image sementara di memory
            img_bytes = BytesIO()
            img.save(img_bytes, format='PNG')
            img_bytes.seek(0)

            files = {'file': ('page.png', img_bytes)}
            data = {
                'apikey': api_key_ocr,
                'OCREngine': 2,
                'language': 'eng',
                'isTable': True,
                'scale': True
            }

            r = requests.post("https://api.ocr.space/parse/image", files=files, data=data)
            result = r.json()
            if debug: print(f"OCR result for page {i}: {result}")

            if result.get("ParsedResults"):
                page_text = result["ParsedResults"][0].get("ParsedText", "")
                if debug: print(f"Page {i} OCR text length: {len(page_text)}")
                # Hentikan jika menemukan 'Go to Indonesian Page'
                if "Go to Indonesian Page" in page_text:
                    page_text = page_text.split("Go to Indonesian Page")[0]
                    text += page_text + "\n"
                    break
                text += page_text + "\n"

        if debug: print(f"Total OCR text length: {len(text)}")
        return text.strip()
    except Exception as e:
        print("OCR.space error:", e)
        return ""

def summarize_text(text, judul=None):
    """Gunakan Groq API untuk merangkum teks dengan prompt yang sesuai"""
    # Tentukan prompt berdasarkan keyword di judul
    prompt = load_prompt(judul)
    
    try:
        completion = client.chat.completions.create(
            model="meta-llama/llama-4-scout-17b-16e-instruct",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": text}
            ],
            temperature=0.2,
            max_completion_tokens=512,
            top_p=1,
            stream=False
        )
        return completion.choices[0].message.content
    except Exception as e:
        return f"Error summarizing: {e}"

def load_prompt(judul=None):
    """Load prompt berdasarkan keyword di judul"""
    if judul:
        judul_lower = judul.lower()
        
        # Check volatilitas
        if "volatilitas" in judul_lower:
            return volatilitas_system_prompt
    
    # Default
    return default_system_prompt

def process_summary(url, tanggal=None, judul=None, kode_emiten=None, debug=False):
    result = {"success": False, "method": "none", "summary": "", "error": "", "extracted_text": ""}
    
    if debug: print("Starting PDF fetch...")
    start_time = time.time()
    pdf_bytes = fetch_pdf_in_memory(url)
    fetch_time = time.time() - start_time
    if debug: print(f"Fetch completed in {fetch_time:.2f}s")
    
    if not pdf_bytes:
        result["error"] = "Failed to download PDF"
        if debug: print("Fetch failed, logging...")
        log_to_csv(tanggal, judul, kode_emiten, url, "Failed", "none", "", result["error"])
        return result

    # ===== DEFAULT: ALWAYS USE OCR =====
    if debug: print("Starting text extraction with OCR.space...")
    start_time = time.time()
    text = extract_text_with_ocr_space(pdf_bytes, debug=debug)
    ocr_time = time.time() - start_time
    if debug: print(f"OCR completed in {ocr_time:.2f}s, text length: {len(text)}")
    method = "ocr_space"
    result["extracted_text"] = text
    
    if not text:
        result["method"] = "failed"
        result["error"] = "No text extracted"
        if debug: print("OCR failed, logging...")
        log_to_csv(tanggal, judul, kode_emiten, url, "Failed", "failed", "", result["error"])
        return result

    # ===== FALLBACK (pdfplumber jika OCR < 50 chars) - COMMENTED OUT =====
    # if len(text) < 50:
    #     if debug: print("OCR text too short, trying pdfplumber as fallback...")
    #     start_time = time.time()
    #     text = extract_text_from_pdf(pdf_bytes, debug=debug)
    #     extract_time = time.time() - start_time
    #     if debug: print(f"pdfplumber extraction completed in {extract_time:.2f}s, text length: {len(text)}")
    #     method = "pdfplumber"
    #     result["extracted_text"] = text
    #     if not text:
    #         result["method"] = "failed"
    #         result["error"] = "No text extracted from both methods"
    #         if debug: print("Both methods failed, logging...")
    #         log_to_csv(tanggal, judul, kode_emiten, url, "Failed", "failed", "", result["error"])
    #         return result

    if debug: print("Starting summarization with Groq...")
    start_time = time.time()
    summary = summarize_text(text, judul)
    summary_time = time.time() - start_time
    if debug: print(f"Summarization completed in {summary_time:.2f}s")
    
    result["success"] = True
    result["method"] = method
    result["summary"] = summary
    if debug: print("Logging success...")
    log_to_csv(tanggal, judul, kode_emiten, url, "Success", method, text, summary)
    return result

def log_to_csv(tanggal, judul, kode_emiten, url, status, method, extracted_text, summary_or_error):
    SUMMARY_LOG_FILE = "summary_logs.csv"
    if not os.path.exists(SUMMARY_LOG_FILE):
        with open(SUMMARY_LOG_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Tanggal Pengumuman", "Judul", "Kode Emiten", "PDF Link", "Fetch Status", "Extraction Method", "Extracted Text", "Summary"])
    
    with open(SUMMARY_LOG_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            tanggal or "",
            judul or "",
            kode_emiten or "",
            url,
            status,
            method,
            extracted_text,
            summary_or_error
        ])

if __name__ == "__main__":
    test_url = "https://19january2021snapshot.epa.gov/sites/static/files/2016-02/documents/epa_sample_letter_sent_to_commissioners_dated_february_29_2015.pdf"
    result = process_summary(test_url, "Test Tanggal", "Test Judul", "TEST", debug=True)
    print("Result:", result)
    