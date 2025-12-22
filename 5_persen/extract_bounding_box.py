import fitz
import numpy as np
import csv
import os
import sys
from conn import upsertKepemilikan
from dotenv import load_dotenv

load_dotenv()

PDF_PATH = sys.argv[1] if len(sys.argv) > 1 else "pdf/20251216.pdf"
COLUMN_PAGE_INDEX = 1   # halaman khusus untuk baca kolom dari pixel merah

# Headers for CSV
headers = [
    'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
    'alamat','alamat_lanjutan','bangsa','domisili','status',
    'jmlh_sblm','gabungan_before','percent_before',
    'jmlh_after','gabungan_after','percent_after','perubahan'
]

# Month mapping
MONTH_MAP = {
    'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04',
    'MAY': '05', 'JUN': '06', 'JUL': '07', 'AUG': '08',
    'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'
}

# Fungsi untuk extract tanggal dari halaman pertama PDF
def extract_date_from_pdf(pdf_path):
    """Extract tanggal dengan format DD-MMM-YYYY dari halaman pertama PDF"""
    try:
        pdf = fitz.open(pdf_path)
        first_page = pdf[0]
        text = first_page.get_text()
        pdf.close()
        
        print("\n🔍 Mencari tanggal dari halaman pertama PDF...")
        
        # Pattern untuk DD-MMM-YYYY (misal: 16-DEC-2025)
        import re
        pattern = r'(\d{1,2})-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-(\d{4})'
        matches = re.finditer(pattern, text, re.IGNORECASE)
        
        found_dates = []
        for match in matches:
            day = match.group(1).zfill(2)
            month_str = match.group(2).upper()
            year = match.group(3)
            month = MONTH_MAP.get(month_str)
            if month:
                iso_date = f"{year}-{month}-{day}"
                found_dates.append(iso_date)
                print(f"✓ Tanggal ditemukan: {match.group(0)} → {iso_date}")
        
        if found_dates:
            # Gunakan tanggal pertama yang ditemukan
            selected_date = found_dates[0]
            print(f"✔ Menggunakan tanggal: {selected_date}")
            return selected_date
        else:
            print("❌ Tidak menemukan tanggal dengan format DD-MMM-YYYY di halaman pertama")
            return None
    
    except Exception as e:
        print(f"❌ Error saat extract tanggal dari PDF: {str(e)}")
        return None

# Fungsi untuk upsert data ke Supabase
def upsertToSupabase(all_rows, filtered_rows, fileName, tanggal=None):
    print(f"\n📤 Upsert ke Supabase...")
    
    if not filtered_rows:
        print("ℹ Tidak ada data untuk di-upsert.")
        return

    print(f"ℹ Total baris untuk upsert: {len(filtered_rows)}")
    
    # Tanggal sudah diekstrak dari PDF, jika tidak ada maka fail
    if not tanggal:
        print("❌ Tanggal tidak ditemukan dari PDF. Upsert dibatalkan.")
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
    full_data_count = count_unique_kodes_from_list(all_rows)
    # Jika ingin debug full_data_count, uncomment baris berikut
    # print("Full data count:", full_data_count)
    payload = {
        "tanggal": tanggal,
        "data": filtered_rows,
        "full_data_count": full_data_count
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

# Fungsi untuk menghitung total unique kode (4 huruf kapital) dari list of list (all_rows)
def count_unique_kodes_from_list(rows):
    kode_data = {}
    seen_entries = set()  # Track (kode, pemegang_saham, cleaned_percent_after) tuples
    duplicate_count = 0
    
    for row in rows:
        kode = row[1].strip() if len(row) > 1 else ''
        if len(kode) == 4 and kode.isupper() and kode.isalpha():
            pemegang_rek = row[3].strip() if len(row) > 3 else ''
            pemegang_saham = row[4].strip() if len(row) > 4 else ''
            percent_after = row[16].strip() if len(row) > 16 else ''
            
            # Clean percent_after: remove commas, spaces, and dots
            percent_after_clean = percent_after.replace(',', '').replace(' ', '').replace('.', '')
            
            if pemegang_saham:  # Hanya count jika pemegang_saham tidak kosong
                # Create a unique key for this entry
                entry_key = (kode, pemegang_saham, percent_after_clean)
                
                # Only process if we haven't seen this exact combination before
                if entry_key not in seen_entries:
                    seen_entries.add(entry_key)
                    
                    if kode not in kode_data:
                        kode_data[kode] = {
                            "count": 0,
                            "columns": []
                        }
                    kode_data[kode]["count"] += 1
                    kode_data[kode]["columns"].append({
                        "kode": kode,
                        "pemegang_saham": pemegang_saham,
                        "percent_after": percent_after  # Store original version, not cleaned
                    })
                else:
                    duplicate_count += 1
                    print(f"⚠ Duplicate removed: {kode} | {pemegang_saham} | {percent_after} (original) → {percent_after_clean} (clean)")
    
    if duplicate_count > 0:
        print(f"\n📊 Total duplikat yang dihapus: {duplicate_count}")
    
    return kode_data

# Fungsi untuk menghitung total unique kode (4 huruf kapital) dari list of dict (filtered_rows)
def count_unique_kodes(filtered_rows):
    kode_counts = {}
    for row in filtered_rows:
        kode = row.get('kode', '').strip()
        if len(kode) == 4 and kode.isupper() and kode.isalpha():
            kode_counts[kode] = kode_counts.get(kode, 0) + 1
    return kode_counts

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

# Fungsi untuk fill missing kode berdasarkan required columns
def fill_missing_kode(all_rows, required_indices):
    # Iterate tanpa sort, asumsikan urutan sudah berdasarkan No
    for i in range(len(all_rows)):
        # Check jika kolom kode (index 1) kosong
        if not all_rows[i][1].strip():
            # Check apakah baris ini memiliki semua required columns (kecuali kode)
            has_required = True
            for idx in required_indices:
                if idx != 1:  # Skip kode index
                    if idx >= len(all_rows[i]) or not all_rows[i][idx].strip():
                        has_required = False
                        break
            
            # Jika punya required columns lain, coba fill kode
            if has_required:
                current_no = all_rows[i][0].strip()
                
                # Strategy 1: Cek baris langsung atas dan bawah (original logic)
                kode_atas = all_rows[i-1][1].strip() if i > 0 else ''
                kode_bawah = all_rows[i+1][1].strip() if i < len(all_rows)-1 else ''
                
                # Strategy 2: Jika nomor baris saat ini ada, cari grup dengan nomor yang sama
                # Cari kode di atas grup nomor yang sama dan di bawah grup
                if current_no:
                    # Cari kode sebelum grup nomor ini
                    kode_before_group = ''
                    for j in range(i - 1, -1, -1):
                        if all_rows[j][0].strip() != current_no:
                            kode_before_group = all_rows[j][1].strip()
                            break
                    
                    # Cari kode setelah grup nomor ini
                    kode_after_group = ''
                    for j in range(i + 1, len(all_rows)):
                        if all_rows[j][0].strip() != current_no:
                            kode_after_group = all_rows[j][1].strip()
                            break
                    
                    # Gunakan kode dari grup logic jika tersedia dan sama
                    if kode_before_group and kode_after_group and kode_before_group == kode_after_group:
                        kode_atas = kode_before_group
                        kode_bawah = kode_after_group
                
                if kode_atas and kode_bawah and kode_atas == kode_bawah:
                    all_rows[i][1] = kode_atas
                    no_val = current_no if current_no else "unknown"
                    print(f"✓ Filled missing kode for No {no_val} with {kode_atas}")

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    pdf = fitz.open(PDF_PATH)
    fileName = os.path.basename(PDF_PATH)  # Define fileName here
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
    
    # Simpan raw CSV sebelum fill (urutan asli)
    rawCsvFileName = fileName.replace('.pdf', '_raw.csv')
    rawCsvFilePath = os.path.join("csv", rawCsvFileName)
    os.makedirs("csv", exist_ok=True)
    try:
        with open(rawCsvFilePath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for r in all_rows:
                writer.writerow(r)
        print(f"💾 Raw CSV tersimpan: {rawCsvFilePath}")
    except Exception as rawCsvErr:
        print("❌ Error menyimpan raw CSV:", str(rawCsvErr))
    
    # Filter rows yang memiliki kolom required
    required_indices = [0, 1, 2, 4, 12, 13, 15, 16]
    
    # Fill missing kode untuk baris yang punya required columns
    fill_missing_kode(all_rows, required_indices)
    
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
    
    # Extract tanggal dari halaman pertama PDF
    tanggal = extract_date_from_pdf(PDF_PATH)
    
    # Upsert ke Supabase
    fileName = os.path.basename(PDF_PATH)
    upsertToSupabase(all_rows, filtered_rows, fileName, tanggal)
