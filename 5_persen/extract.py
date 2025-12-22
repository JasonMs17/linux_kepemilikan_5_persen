import pdfplumber 
import os
import re
import csv
from datetime import datetime
from conn import upsertKepemilikan

PDF_FOLDER = "pdf"

# cleanup cell minimal (strip only)
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

def replace_dashes_with_zero(rows):
    for row in rows:
        for i in range(len(row)):
            if row[i] == "-":
                row[i] = "0"
    return rows

def fill_group_headers(rows):
    current_no = None
    fill_values = {}
    for row in rows:
        no = row[0].strip()
        if no:
            current_no = no
            if no not in fill_values:
                fill_values[no] = {
                    'emiten': row[2],
                    'pemegang_rek': row[3],
                    'pemegang_saham': row[4],
                    'nama_rek': row[5],
                    'alamat': row[6],
                    'alamat_lanjutan': row[7],
                    'bangsa': row[8],
                    'domisili': row[9],
                    'status': row[10],
                    'gabungan_before': row[12],
                    'percent_before': row[13],
                    'gabungan_after': row[15],
                    'percent_after': row[16]
                }
        if current_no and current_no in fill_values:
            fv = fill_values[current_no]
            row[2] = fv['emiten'] if not row[2] else row[2]
            row[3] = fv['pemegang_rek'] if not row[3] else row[3]
            row[4] = fv['pemegang_saham'] if not row[4] else row[4]
            row[5] = fv['nama_rek'] if not row[5] else row[5]
            row[6] = fv['alamat'] if not row[6] else row[6]
            row[7] = fv['alamat_lanjutan'] if not row[7] else row[7]
            row[8] = fv['bangsa'] if not row[8] else row[8]
            row[9] = fv['domisili'] if not row[9] else row[9]
            row[10] = fv['status'] if not row[10] else row[10]
            row[12] = fv['gabungan_before'] if not row[12] else row[12]
            row[13] = fv['percent_before'] if not row[13] else row[13]
            row[15] = fv['gabungan_after'] if not row[15] else row[15]
            row[16] = fv['percent_after'] if not row[16] else row[16]
    return rows

kode_regex = re.compile(r"^[A-Z]{3,4}$")

headers = [
    'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
    'alamat','alamat_lanjutan','bangsa','domisili','status',
    'jmlh_sblm','gabungan_before','percent_before',
    'jmlh_after','gabungan_after','percent_after','perubahan','percent_difference'
]

# ========== MAIN PROCESSING ==========
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
            table = page.extract_table()
            if not table:
                continue

            rows = [clean_row(r) for r in table] if page_index >= 3 else table
            all_rows.extend(rows)

    all_rows = [r for r in all_rows if not is_repeated_header(r)]
    all_rows = [r for r in all_rows if not is_investor_subheader(r)]

    all_rows = normalize_rows(all_rows, target_len=len(headers))
    all_rows = fill_group_headers(all_rows)

    filtered = [r for r in all_rows if r and len(r) > 1 and kode_regex.match(r[1])]

    result = []
    for r in filtered:
        obj = {}
        for i, h in enumerate(headers):
            obj[h] = r[i] if i < len(r) else ""
        result.append(obj)

    # Calculate percent_difference
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
    upsertKepemilikan(payload)
    print(f"✔ Selesai upsert {tanggal}")
