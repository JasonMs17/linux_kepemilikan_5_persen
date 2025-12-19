import fitz
import numpy as np
import csv
import os
import sys
from conn import upsertKepemilikan
from dotenv import load_dotenv

load_dotenv()

PDF_PATH = sys.argv[1] if len(sys.argv) > 1 else "pdf/20251217.pdf"
COLUMN_PAGE_INDEX = 1   # halaman khusus untuk baca kolom dari pixel merah

# Headers for CSV
headers = [
    'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
    'alamat','alamat_lanjutan','bangsa','domisili','status',
    'jmlh_sblm','gabungan_before','percent_before',
    'jmlh_after','gabungan_after','percent_after','perubahan'
]

# Fungsi untuk upsert data ke Supabase
def upsertToSupabase(all_rows, filtered_rows, fileName):
    print(f"\n📤 Upsert ke Supabase...")
    
    if not filtered_rows:
        print("ℹ Tidak ada data untuk di-upsert.")
        return

    print(f"ℹ Total baris untuk upsert: {len(filtered_rows)}")
    
    # Extract tanggal dari nama file (format: yyyymmdd.pdf)
    # Konversi ke ISO format YYYY-MM-DD
    import re
    match = re.search(r'(\d{8})', fileName)
    tanggal = None
    if match:
        dateStr = match.group(1)
        year = dateStr[:4]
        month = dateStr[4:6]
        day = dateStr[6:8]
        tanggal = f"{year}-{month}-{day}"

    if not tanggal:
        print("❌ Tidak bisa ekstrak tanggal dari nama file:", fileName)
        return

    # Simpan semua rows ke CSV file
    csvFileName = fileName.replace('.pdf', '.csv')
    csvFilePath = os.path.join("csv", csvFileName)
    os.makedirs("csv", exist_ok=True)
    
    try:
        with open(csvFilePath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for r in all_rows:
                writer.writerow(r)
        print(f"💾 CSV tersimpan: {csvFilePath}")
    except Exception as csvErr:
        print("❌ Error menyimpan CSV:", str(csvErr))

    # Format payload sesuai struktur tabel
    payload = {
        "tanggal": tanggal,
        "data": filtered_rows
    }

    try:
        response = upsertKepemilikan(payload)
        print(f"✔ Berhasil upsert {len(filtered_rows)} baris untuk tanggal {tanggal}")
        
        # Print missing numbers dari filtered_rows
        nos = set()
        for row in filtered_rows:
            no_val = row.get('No')
            if no_val:
                try:
                    num = int(no_val)
                    nos.add(num)
                except ValueError:
                    pass
        if nos:
            maxNo = max(nos)
            missing = [i for i in range(1, maxNo + 1) if i not in nos]
            if missing:
                print(f"Missing numbers: {', '.join(map(str, missing))}")
            else:
                print("No missing numbers")
        else:
            print('No numbers found')
    except Exception as err:
        print("Error upsert ke Supabase:", str(err))

# =======================================================
# 1. DETEKSI KOLOM DARI PIXEL MERAH PALING BAWAH
# =======================================================
def detect_columns_from_red_pixel(pdf_path, page_index):
    pdf = fitz.open(pdf_path)
    page = pdf[page_index]
    pix = page.get_pixmap()
    img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
    R, G, B = img[..., 0], img[..., 1], img[..., 2]
    # target warna merah #880000
    R0, G0, B0 = 136, 0, 0
    tol = 20
    mask = (
        (R >= R0 - tol) & (R <= R0 + tol) &
        (G >= G0 - tol) & (G <= G0 + tol) &
        (B >= B0 - tol) & (B <= B0 + tol)
    )
    h, w = mask.shape
    # cari 3 baris paling bawah yang mengandung pixel merah
    red_rows = []
    for y in range(h - 1, -1, -1):
        if mask[y].any():
            red_rows.append(y)
            if len(red_rows) >= 5:
                break
    
    if len(red_rows) < 5:
        raise ValueError(f"Hanya ditemukan {len(red_rows)} baris pixel merah, minimal 5 baris diperlukan.")
    
    # ambil semua x dari 3 baris paling bawah
    all_xs = set()
    for y in red_rows:
        xs = np.where(mask[y])[0]
        all_xs.update(xs.tolist())
    
    xs = sorted(list(all_xs))
    
    # buat blok kolom dengan minimal width 2
    blocks = []
    start = xs[0]
    for i in range(1, len(xs)):
        if xs[i] != xs[i - 1] + 1:
            width = xs[i - 1] - start + 1
            if width >= 2:  # hanya simpan blok dengan minimal width 2
                blocks.append([start, xs[i - 1]])
            start = xs[i]
    # tambahkan blok terakhir
    width = xs[-1] - start + 1
    if width >= 1:
        blocks.append([start, xs[-1]])
    
    # Adjust end boundaries for specific columns (indices 12,13,15,16,17)
    adjust_indices = [11, 12, 13, 15, 16, 17]
    for idx in adjust_indices:
        if idx < len(blocks):
            blocks[idx][1] += 1
    
    print("\n=== COLUMN DETECTED FROM PIXEL (PAGE 2) ===")
    for i, (x1, x2) in enumerate(blocks, 1):
        print(f"Kolom {i}: x={x1} → {x2}, width={x2-x1+1}")
    return blocks
# =======================================================
# 2. EXTRACT WORDS PER HALAMAN
# =======================================================
def extract_words_by_rows(page):
    words = page.get_text("words")  # x0, y0, x1, y1, text
    rows = []
    for w in words:
        x0, y0, x1, y1, text, *_ = w
        placed = False
        for row in rows:
            # check if y overlaps (toleransi ±1px)
            if abs(row["y"] - y0) < 3:
                row["words"].append((x0, text))
                placed = True
                break
        if not placed:
            rows.append({"y": y0, "words": [(x0, text)]})
    # sort rows berdasarkan Y (atas → bawah)
    rows.sort(key=lambda r: r["y"])
    # sort words di setiap row berdasarkan X
    for r in rows:
        r["words"].sort(key=lambda x: x[0])
    return rows

# =======================================================
# 3. MASUKKAN KE KOLOM SESUAI BLOK X
# =======================================================
def assign_words_to_columns(rows, column_blocks):
    assigned = []
    for row in rows:
        cols = [""] * len(column_blocks)
        for x, text in row["words"]:
            for idx, (x1, x2) in enumerate(column_blocks):
                if x1 <= x <= x2:
                    if cols[idx] == "":
                        cols[idx] = text
                    else:
                        cols[idx] += " " + text
                    break
        # Skip header rows
        combined = " ".join(cols).lower()
        if cols[0].strip() == "No" or "kepemilikan per" in combined or "kepemilikan efek diatas" in combined or "berdasarkan sid" in combined or "keterangan" in combined or "font" in combined or "hitam tidak ada perubahan" in combined or "biru ada perubahan" in combined:
            continue
        assigned.append(cols)
    return assigned

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    pdf = fitz.open(PDF_PATH)
    # 1) ambil kolom dari pixel merah (hanya halaman index 2)
    col_blocks = detect_columns_from_red_pixel(PDF_PATH, COLUMN_PAGE_INDEX)
    all_rows = []
    # 2) proses semua halaman dari 2 sampai terakhir
    for page_index in range(1, len(pdf)):
        page = pdf[page_index]
        print(f"Processing page {page_index + 1} ...")
        rows = extract_words_by_rows(page)
        table_rows = assign_words_to_columns(rows, col_blocks)
        all_rows.extend(table_rows)
    
    # Filter rows yang memiliki kolom required
    required_indices = [0, 1, 2, 3, 12, 13, 15, 16]  # No, kode, emiten, pemegang_rek, gabungan_before, percent_before, gabungan_after, percent_after
    filtered_rows = []
    for row in all_rows:
        if all(row[i].strip() for i in required_indices if i < len(row)):
            obj = {}
            for idx, header in enumerate(headers):
                value = row[idx] if idx < len(row) else None
                obj[header] = value
            
            # Tambah percent_difference jika ada perbedaan
            try:
                pa_str = obj.get('percent_after', '').replace(',', '').replace(' ', '').strip()
                pb_str = obj.get('percent_before', '').replace(',', '').replace(' ', '').strip()
                if pb_str == '-':
                    obj['percent_before'] = '0'
                    pb = 0
                else:
                    pb = float(pb_str) if pb_str else None
                pa = float(pa_str) if pa_str else None
                if pa is not None and pb is not None:
                    obj['percent_difference'] = pa - pb
            except ValueError:
                pass
            
            filtered_rows.append(obj)
    
    print(f"✔ Ekstraksi selesai: {len(filtered_rows)} baris")
    
    # Upsert ke Supabase
    fileName = os.path.basename(PDF_PATH)
    upsertToSupabase(all_rows, filtered_rows, fileName)
