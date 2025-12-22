import pandas as pd

# Load CSV
df = pd.read_csv("csv/20251205.csv")

# Fungsi preprocess angka
def parse_number(x):
    if pd.isna(x) or x in ["-", ""]:
        return 0.0
    s = str(x).strip()
    # hapus semua titik ribuan
    s = s.replace('.', '')
    # jika ada koma sebagai desimal, append jadi 3 digit
    if ',' in s:
        parts = s.split(',')
        if len(parts) == 2:
            integer, decimal = parts
            decimal = decimal.ljust(3, '0')  # pastikan 3 digit
            s = integer + decimal             # gabungkan jadi integer
        else:
            s = s.replace(',', '')
    try:
        return float(s)
    except:
        return 0.0


# Terapkan preprocessing
df["jmlh_sblm"] = df["jmlh_sblm"].apply(parse_number)
df["gabungan_before"] = df["gabungan_before"].apply(parse_number)

# Tambah kolom parent_no
df["parent_no"] = None

# Logic merge menggunakan cumulative sum
parent_index = None
parent_total = 0
child_sum = 0

for idx, row in df.iterrows():
    if pd.notna(row["No"]):  # parent row
        parent_index = idx
        parent_total = row["gabungan_before"]
        child_sum = 0
        df.at[idx, "parent_no"] = row["No"]
    else:
        if parent_index is not None:
            df.at[idx, "parent_no"] = df.at[parent_index, "No"]
            child_sum += row["jmlh_sblm"]
            # reset parent_index hanya jika cumulative anak >= parent_total
            if child_sum >= parent_total:
                parent_index = None
                parent_total = 0
                child_sum = 0

# Simpan hasil merge
df.to_csv("csv/merged.csv", index=False)
print("✔ merged.csv berhasil dibuat")
