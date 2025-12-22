import pdfplumber
import os
import re
import csv
from datetime import datetime
from conn import upsertKepemilikan

PDF_FOLDER = "pdf"

def clean_cell(text):
    return str(text).strip() if text else ""

def clean_row(row):
    return [clean_cell(cell) for cell in row]

def is_repeated_header(row):
    if not row:
        return False
    s = " ".join(str(x or "").strip() for x in row)
    return (
        row[0] == "No"
        and "Kode Efek" in s
        and "Nama Emiten" in s
    )

def is_investor_subheader(row):
    if not row:
        return False
    s = " ".join(str(x or "").strip() for x in row)
    return (
        "Jumlah Saham" in s
        and "Saham Gabungan Per Investor" in s
        and "Persentase Kepemilikan Per Investor" in s
    )

def normalize_rows(rows, target_len=18):
    normalized = []
    for r in rows:
        r = r or []
        r = [str(x or "") for x in r]
        if len(r) < target_len:
            r.extend([""] * (target_len - len(r)))
        elif len(r) > target_len:
            r = r[:target_len]
        normalized.append(r)
    return normalized

kode_regex = re.compile(r"^[A-Z]{3,4}$")

headers = [
    'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
    'alamat','alamat_lanjutan','bangsa','domisili','status',
    'jmlh_sblm','gabungan_before','percent_before',
    'jmlh_after','gabungan_after','percent_after','perubahan','percent_difference'
]

for file in os.listdir(PDF_FOLDER):
    if not file.lower().endswith(".pdf"):
        continue

    file_path = os.path.join(PDF_FOLDER, file)
    print(f"📄 Processing {file}")

    all_rows = []
    page_index = 0

    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_index += 1
            table = page.extract_table({
    "vertical_strategy": "lines",
    "horizontal_strategy": "lines",
})
            if not table:
                continue

            rows = [clean_row(r) for r in table]
            all_rows.extend(rows)

    all_rows = [r for r in all_rows if not is_repeated_header(r)]
    all_rows = [r for r in all_rows if not is_investor_subheader(r)]

    all_rows = normalize_rows(all_rows, target_len=len(headers))

    filtered = [r for r in all_rows if r and len(r) > 1 and kode_regex.match(r[1])]

    result = []
    for r in filtered:
        obj = {}
        for i, h in enumerate(headers):
            obj[h] = r[i] if i < len(r) else ""
        result.append(obj)

    for obj in result:
        try:
            pa = float(obj.get("percent_after") or 0)
            pb = float(obj.get("percent_before") or 0)
            obj["percent_difference"] = pa - pb
        except ValueError:
            obj["percent_difference"] = ""

    print(f"✔ Extracted rows: {len(result)}")

    csv_name = file.replace('.pdf', '.csv')
    csv_path = os.path.join('csv', csv_name)

    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(result)

    print(f"💾 CSV saved: {csv_path}")

    match = re.search(r"(\d{4})(\d{2})(\d{2})", file)
    if not match:
        print("❌ Tidak ada tanggal di nama file")
        continue

    tanggal = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    payload = { "tanggal": tanggal, "data": result }

    print("📤 Upsert to Supabase...")
    # upsertKepemilikan(payload)
    print(f"✔ Selesai upsert {tanggal}")
    # ---- Detect missing No & recover via extract_words() ----
    numbers = [int(r[0]) for r in filtered if r[0].isdigit()]
    missing = [x for x in range(numbers[0], numbers[-1]) if x not in numbers]

    print("Missing numbers:", missing)

    if missing:
        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                words = page.extract_words()

                for miss in missing:
                    # cari posisi Y baris sebelum & sesudah
                    prevY = next((w['top'] for w in words if w['text'] == str(miss-1)), None)
                    nextY = next((w['top'] for w in words if w['text'] == str(miss+1)), None)

                    if prevY and nextY:
                        merged_words = [
                            w['text'] for w in words
                            if w['top'] > prevY and w['top'] < nextY
                        ]
                        print(f"Recovered merged row for {miss} :", merged_words)

                        # Bangun row kosong lalu isi dari merged text
                        new_row = [""] * len(headers)
                        new_row[0] = str(miss)      # isi No
                        new_row[1] = merged_words[0] if merged_words else ""  # kode efek
                        # Sisanya custom mapping berdasarkan posisi
                        filtered.insert(miss-1, new_row)

