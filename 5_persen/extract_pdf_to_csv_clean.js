import fs from "fs";
import path from "path";
import "./pdfjs-canvas-patch.js";
import pdfTableExtractor from "pdf-table-extractor";
import "dotenv/config";
import { stringify } from "csv-stringify/sync";
import { upsertKepemilikan } from "./connection.js";

// Folder input & output
const lamp1_pdfs = path.join(process.cwd(), "pdf");
const extracted = path.join(process.cwd(), "csv");

// Buat folder csv jika belum ada
if (!fs.existsSync(extracted)) {
    fs.mkdirSync(extracted);
}

// ----- LIST TEXT YANG HARUS DI-CLEAR -----
const corruptPatterns = [
    "ADARO ANDALAN INDONESIA Tbk, PT",
    "ADARO STRATEGIC INVESTMENTS",
    "ADARO STRATEGIC INVESTMENT",
    "3200142830",
    "3,200,142,830",
    "41.10"
];

// Convert ke regex global
const corruptRegexList = corruptPatterns.map(str =>
    new RegExp(str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), "g")
);

// Clean function
function cleanCell(text) {
    if (!text) return "";

    let cleaned = text;
    corruptRegexList.forEach(regex => cleaned = cleaned.replace(regex, ""));
    return cleaned.trim();
}

// Clean satu row
function cleanRow(row) {
    return row.map(cell => cleanCell(String(cell || "")));
}

// Regex untuk kode efek
const kodeEfekRegex = /^[A-Z]{3,4}$/;

// Forward-fill
function forwardFill(rows) {
    for (let r = 1; r < rows.length; r++) {
        for (let c = 0; c < rows[r].length; c++) {
            if (!rows[r][c]) {
                rows[r][c] = rows[r - 1][c];
            }
        }
    }
    return rows;
}

// Deteksi header sampah
function isRepeatedHeader(row) {
    if (!row) return false;
    return (
        row[0] === "No" &&
        row.join(" ").includes("Kode Efek") &&
        row.join(" ").includes("Nama Emiten")
    );
}

function isInvestorSubheader(row) {
    if (!row) return false;
    const s = row.join(" ");
    return (
        s.includes("Jumlah Saham") &&
        s.includes("Saham Gabungan Per Investor") &&
        s.includes("Persentase Kepemilikan Per Investor")
    );
}

// Fungsi untuk upsert data ke Supabase
async function upsertToSupabase(rows, fileName) {
    console.log(`\n📤 Upsert ke Supabase...`);
    
    if (rows.length === 0) {
        console.log("ℹ Tidak ada data untuk di-upsert.");
        return;
    }

    console.log(`ℹ Total baris: ${rows.length}`);
    
    // Extract tanggal dari nama file (format: yyyymmdd.pdf)
    // Konversi ke ISO format YYYY-MM-DD
    const match = fileName.match(/(\d{8})/);
    let tanggal = null;
    if (match) {
        const dateStr = match[1];
        const year = dateStr.substring(0, 4);
        const month = dateStr.substring(4, 6);
        const day = dateStr.substring(6, 8);
        tanggal = `${year}-${month}-${day}`;
    }

    if (!tanggal) {
        console.error("❌ Tidak bisa ekstrak tanggal dari nama file:", fileName);
        return;
    }

    // Simpan ke CSV file
    const csvFileName = fileName.replace('.pdf', '.csv');
    const csvFilePath = path.join(extracted, csvFileName);
    
    try {
        const csvContent = stringify(rows, { 
            header: true, 
            columns: [
                'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
                'alamat','alamat_lanjutan','bangsa','domisili','status',
                'jmlh_sblm','gabungan_before','percent_before',
                'jmlh_after','gabungan_after','percent_after','perubahan'
            ]
        });
        fs.writeFileSync(csvFilePath, csvContent);
        console.log(`💾 CSV tersimpan: ${csvFilePath}`);
    } catch (csvErr) {
        console.error("❌ Error menyimpan CSV:", csvErr.message);
    }

    // Format payload sesuai struktur tabel
    const payload = {
        tanggal: tanggal,
        data: rows
    };

    try {
        await upsertKepemilikan(payload);
        console.log(`✔ Berhasil upsert ${rows.length} baris untuk tanggal ${tanggal}`);
    } catch (err) {
        console.error("Error upsert ke Supabase:", err.message);
    }
}

// Proses PDF
fs.readdirSync(lamp1_pdfs)
    .filter(file => file.endsWith(".pdf"))
    .forEach(file => {
        const filePath = path.join(lamp1_pdfs, file);
        console.log(`Processing ${file}...`);

        pdfTableExtractor(filePath, result => {
            let allRows = [];
            let pageIndex = 0;

            // PROSES PER HALAMAN
            result.pageTables.forEach(pt => {
                pageIndex++;

                let rows = pt.tables;

                // Hanya halaman ≥3 yang di-clean
                if (pageIndex >= 3) {
                    rows = rows.map(row => cleanRow(row));
                }

                allRows.push(...rows);
            });

            // Filter header2
            allRows = allRows.filter(row => !isRepeatedHeader(row));
            allRows = allRows.filter(row => !isInvestorSubheader(row));

            // Forward fill
            allRows = forwardFill(allRows);

            // Filter kode efek
            const filtered = allRows.filter(row => {
                if (!row || !row[1]) return false;
                return kodeEfekRegex.test(String(row[1]).trim());
            });

            // Map ke object dengan headers sebagai keys
            const headers = [
                'No','kode','emiten','pemegang_rek','pemegang_saham','nama_rek',
                'alamat','alamat_lanjutan','bangsa','domisili','status',
                'jmlh_sblm','gabungan_before','percent_before',
                'jmlh_after','gabungan_after','percent_after','perubahan'
            ];

            const dataToUpsert = filtered.map(row => {
                const obj = {};
                headers.forEach((header, index) => {
                    let value = row[index] || null;
                    
                    // Konversi ke number untuk kolom tertentu
                    const numericColumns = ['No', 'jmlh_sblm', 'perubahan', 'jmlh_after', 'gabungan_after', 'gabungan_before', 'percent_after', 'percent_before'];
                    if (numericColumns.includes(header) && value !== null) {
                        // Hapus semua separator ribuan (koma) dan titik desimal, biar parseFloat bisa handle angka besar
                        const cleaned = String(value)
                            .replace(/[^\d,.-]/g, '')  // hapus semua selain digit, koma, titik, minus
                            .replace(/,/g, '');        // hapus semua koma ribuan
                        
                        const num = parseFloat(cleaned);
                        if (header === 'percent_before') {
                            // Untuk percent_before, jika bukan angka ubah jadi 0
                            value = isNaN(num) ? 0 : num;
                        } else {
                            value = isNaN(num) ? null : num;
                        }
                    }
                    
                    obj[header] = value;
                });
                
                // Tambah kolom percent_difference hanya jika ada perbedaan
                const percentAfter = obj.percent_after;
                const percentBefore = obj.percent_before;
                
                if (percentAfter !== null && percentBefore !== null && percentAfter !== percentBefore) {
                    obj.percent_difference = percentAfter - percentBefore;
                }
                // Jika tidak ada perbedaan, tidak tambahkan kolom percent_difference
                
                return obj;
            });

            console.log(`✔ Ekstraksi selesai: ${dataToUpsert.length} baris`);
            
            // Upsert ke Supabase
            upsertToSupabase(dataToUpsert, file);
        },
        err => console.error("Error ekstraksi:", err));
    });
