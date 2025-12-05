import "dotenv/config";

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_ANON_KEY;

export async function upsertKepemilikan(data) {
    const url = `${SUPABASE_URL}/rest/v1/report_kepemilikan_lima_persen`;

    const res = await fetch(url, {
        method: "POST",
        headers: {
            "apikey": SUPABASE_KEY,
            "Authorization": `Bearer ${SUPABASE_KEY}`,
            "Content-Type": "application/json",
            "Prefer": "return=representation,resolution=merge-duplicates"
        },
        body: JSON.stringify(data)
    });

    const json = await res.json();
    console.log("Response:", json);
}

