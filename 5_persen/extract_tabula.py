import camelot
import os
import re
import csv

PDF_FOLDER = "pdf"

headers = [
    'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
    'alamat','alamat_lanjutan','bangsa','domisili','status',
    'jmlh_sblm','gabungan_before','percent_before',
    'jmlh_after','gabungan_after','percent_after','perubahan','percent_difference'
]

kode_regex = re.compile(r"^[A-Z]{3,4}$")

for file in os.listdir(PDF_FOLDER):
    if not file.lower().endswith(".pdf"):
        continue

    file_path = os.path.join(PDF_FOLDER, file)
    print(f"📄 Processing {file}")

    # extract tables from every page using STREAM mode
    tables = camelot.read_pdf(
        file_path,
        flavor="stream",
        pages="all",
        strip_text="\n",
    )

    print(f"🔍 Total tables detected: {tables.n}")

    all_rows = []
    for t in tables:
        df = t.df
        rows = df.values.tolist()
        all_rows.extend(rows)

    # filter rows with kode saham
    filtered = [r for r in all_rows if len(r) > 1 and kode_regex.match(str(r[1]).strip())]

    # normalize rows
    for idx, r in enumerate(filtered):
        if len(r) < len(headers):
            r.extend([""] * (len(headers) - len(r)))
        elif len(r) > len(headers):
            filtered[idx] = r[:len(headers)]

    # convert rows to dict
    result = [dict(zip(headers, row)) for row in filtered]

    # compute percent difference
    for obj in result:
        try:
            pa = float(obj.get("percent_after") or 0)
            pb = float(obj.get("percent_before") or 0)
            obj["percent_difference"] = pa - pb
        except:
            obj["percent_difference"] = ""

    print(f"✔ Extracted rows: {len(result)}")

    # save CSV
    csv_name = file.replace(".pdf", ".csv")
    csv_path = os.path.join("csv", csv_name)

    with open(csv_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=headers)
        writer.writeheader()
        writer.writerows(result)

    print(f"💾 Saved: {csv_path}")
