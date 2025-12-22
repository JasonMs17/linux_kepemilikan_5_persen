from aspose.pdf import Document, ExcelSaveOptions

input_pdf = "pdf/20251205.pdf"
output_excel = "result.xlsx"

# Load PDF
doc = Document(input_pdf)

# Pilih opsi konversi ke excel
options = ExcelSaveOptions()
options.MinimizeTheNumberOfWorksheets = True  # semua tabel di satu sheet
options.InsertBlankColumnAtFirst = False
options.ConvertNonTabularDataToSpreadsheet = True

# Save to Excel
doc.save(output_excel, options)

print("Converted to Excel:", output_excel)
