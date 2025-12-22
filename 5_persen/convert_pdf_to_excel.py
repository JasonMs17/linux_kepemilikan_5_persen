import pdfplumber
import pandas as pd
import os

pdf_folder = "pdf"
excel_folder = "excel"
os.makedirs(excel_folder, exist_ok=True)

for f in os.listdir(pdf_folder):
    if not f.lower().endswith(".pdf"):
        continue
    pdf_path = os.path.join(pdf_folder, f)
    excel_path = os.path.join(excel_folder, f.replace(".pdf", ".xlsx"))
    
    all_tables = []
    first_columns = None
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if len(table) < 2:  # abaikan tabel kosong
                    continue
                df = pd.DataFrame(table[1:], columns=table[0])
                if first_columns is None:
                    first_columns = df.columns
                else:
                    # samakan kolom dengan header pertama, tambahkan kolom kosong kalau perlu
                    for col in first_columns:
                        if col not in df.columns:
                            df[col] = None
                    df = df[first_columns]  # urutkan kolom
                all_tables.append(df)
    
    if all_tables:
        final_df = pd.concat(all_tables, ignore_index=True)
        final_df.to_excel(excel_path, index=False)
        print(f"Berhasil convert: {f}")
    else:
        print(f"Tidak ada tabel di PDF: {f}")
