import fitz  # PyMuPDF

doc = fitz.open("pdf/20251205.pdf")
page = doc[2]

words = page.get_text("words")  
# format: (x0, y0, x1, y1, text, block_no, line_no, word_no)

rows = {}
for w in words:
    x0, y0, x1, y1, text, _, _, _ = w
    y = round(y0, 1)  # group berdasarkan garis horizontal
    rows.setdefault(y, []).append((x0, y0, text))

# sorting vertical & horizontal
with open("txt/20251205_coords.txt", "w") as f:
    for y, items in sorted(rows.items()):
        items.sort(key=lambda i: i[0])  # sort by x0
        # print lengkap x,y,teks
        for x0, y0, text in items:
            f.write(f"x={x0:.1f}, y={y0:.1f} -> {text}\n")
        f.write("--------\n")
