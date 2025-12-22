import fitz
import csv

pdf_path = "pdf/20251204_Semua Emiten Saham_Pengumuman Bursa_31999960_lamp1.pdf"
output_csv = "filtered_x_61.csv"

TARGET_X = 61.2
TOLERANCE = 1   # boleh diganti

doc = fitz.open(pdf_path)

filtered = []

for page_num in range(0, len(doc)):
    page = doc[page_num]
    words = page.get_text("words")
    
    for w in words:
        x0, y0, x1, y1, text, _, _, _ = w
        if abs(x0 - TARGET_X) <= TOLERANCE and len(text) == 4 and text.isupper() and text.isalpha():
            filtered.append([page_num, text, round(x0, 1), round(y0, 1)])

# sort by page number, then by Y (atas ke bawah)
filtered.sort(key=lambda r: (r[0], r[3]))

# save
with open(output_csv, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["page", "text", "x", "y"])
    writer.writerows(filtered)

print("Saved to:", output_csv)
