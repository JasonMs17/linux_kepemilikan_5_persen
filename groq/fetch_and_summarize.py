from io import BytesIO
import cloudscraper
import pdfplumber
from dotenv import load_dotenv
import os
from groq import Groq
import requests
import base64

# Load API key
load_dotenv()
api_key = os.getenv("GROQ_API_KEY")
client = Groq(api_key=api_key)

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

def extract_text_from_pdf(pdf_bytes):
    """Ekstrak teks PDF menggunakan pdfplumber, berhenti jika menemukan 'Go To English Page'"""
    text = ""
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                # Hentikan ekstraksi saat menemukan kata "Go To English Page"
                if "Go To English Page" in page_text:
                    page_text = page_text.split("Go To English Page")[0]
                    text += page_text + "\n"
                    break  # stop setelah bagian Indonesia
                text += page_text + "\n"
    return text.strip()

def extract_text_with_puter(pdf_bytes):
    """Fallback OCR menggunakan Puter.js cloud OCR, max 5 halaman"""
    text = ""
    try:
        with pdfplumber.open(pdf_bytes) as pdf:
            num_pages = min(5, len(pdf.pages))
            for i in range(num_pages):
                page = pdf.pages[i]
                img = page.to_image(resolution=200)
                img_bytes_io = BytesIO()
                img.original.save(img_bytes_io, format="PNG")
                img_bytes = img_bytes_io.getvalue()

                # Encode image ke base64
                base64_img = base64.b64encode(img_bytes).decode("utf-8")

                # Request ke Puter.js OCR API
                resp = requests.post(
                    "https://api.puter.com/v2/img2txt",
                    json={"image": f"data:image/png;base64,{base64_img}"}
                )
                resp.raise_for_status()
                result = resp.json()
                page_text = result.get("text", "")
                text += page_text + "\n"
    except Exception as e:
        print("Puter.js OCR error:", e)
    return text.strip()

def summarize_text(text):
    """Gunakan Groq API untuk merangkum teks"""
    completion = client.chat.completions.create(
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        messages=[
            {"role": "system", "content": """You are a financial assistant that reads company stock reports or shareholder announcements.
First, identify the **main purpose or essence of the document** in simple terms, such as "monthly report of shareholding", "changes in ownership", or "announcement of treasury shares".
Then, extract all **key numeric details** that are relevant for investment decisions.
Include percentages and changes where available. 
Ignore administrative or cosmetic details like signatures, addresses, letter numbers, or dates unless they affect ownership information.
Present the summary clearly, starting with the **main purpose**, followed by structured numeric data.
Be concise but capture all data relevant for evaluating stock ownership and investment decisions."""},
            {"role": "user", "content": text}
        ],
        temperature=0.2,
        max_completion_tokens=512,
        top_p=1,
        stream=False
    )
    return completion.choices[0].message.content

def main(url):
    pdf_bytes = fetch_pdf_in_memory(url)
    if not pdf_bytes:
        print("Failed to download PDF.")
        return

    text = extract_text_from_pdf(pdf_bytes)

    # Fallback OCR jika teks kosong atau terlalu sedikit
    if len(text) < 50:
        print("Text extraction failed or too short, using Puter.js OCR...")
        text = extract_text_with_puter(pdf_bytes)
        if not text:
            print("No text could be extracted from PDF, even with Puter.js OCR.")
            return

    summary = summarize_text(text)
    print(summary)

if __name__ == "__main__":
    # Ganti URL PDF di sini
    url = "https://www.idx.co.id/StaticData/NewsAndAnnouncement/ANNOUNCEMENTSTOCK/From_KSEI/LK-10122025-8427-00.pdf-0.pdf"
    main(url)
