import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm

# ================== KONFIGURASI ==================
API_URL = "http://api.weatherapi.com/v1/current.json"
API_KEY = "b9a12af935d8444489240401251609"

INPUT_XLSX = r"C:\Adn\Adn Belajar python\kecamatan_jawa_timur_wilayahid.xlsx"      # ganti sesuai file kamu
SHEET_NAME = 0                                 # atau "Sheet1"
KOLOM_KECAMATAN = "Kecamatan"                  # nama kolom di Excel
OUTPUT_XLSX = "weather_kecamatan_jatim_threads.xlsx"

MAX_WORKERS = 10                               # jumlah thread
TIMEOUT = 10.0                                 # detik per request
# =================================================

# Session per-thread (aman untuk multithreading)
_thread_local = threading.local()

def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        _thread_local.session = make_session()
    return _thread_local.session

def read_kecamatan(path: str, sheet=0, kolom="Kecamatan") -> list[str]:
    df = pd.read_excel(path, sheet_name=sheet)
    if kolom not in df.columns:
        kolom = df.columns[0]
    names = (
        df[kolom]
        .dropna()
        .astype(str)
        .map(str.strip)
        .replace("", pd.NA)
        .dropna()
        .unique()
        .tolist()
    )
    return names

def fetch_one(kecamatan_name: str) -> dict:
    session = get_session()
    query = f"{kecamatan_name}, Jawa Timur"
    params = {"key": API_KEY, "q": query}

    try:
        r = session.get(API_URL, params=params, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        loc = data.get("location", {})
        cur = data.get("current", {})
        cond = (cur.get("condition") or {})

        return {
            "Kecamatan": kecamatan_name,
            "Query": query,
            "Lokasi_Teridentifikasi": f'{loc.get("name", "")}, {loc.get("region", "")}',
            "Negara": loc.get("country", ""),
            "Last_Update": cur.get("last_updated", ""),
            "Suhu_C": cur.get("temp_c", None),
            "Kelembapan": cur.get("humidity", None),
            "Kondisi_Cuaca": cond.get("text", ""),
            "Kecepatan_Angin_kph": cur.get("wind_kph", None),
            "Arah_Angin": cur.get("wind_dir", ""),
            "Sinar_UV": cur.get("uv", None),
            "Error": ""
        }
    except requests.RequestException as e:
        return {
            "Kecamatan": kecamatan_name,
            "Query": query,
            "Lokasi_Teridentifikasi": "",
            "Negara": "",
            "Last_Update": "",
            "Suhu_C": None,
            "Kelembapan": None,
            "Kondisi_Cuaca": "",
            "Kecepatan_Angin_kph": None,
            "Arah_Angin": "",
            "Sinar_UV": None,
            "Error": str(e)
        }


def main():
    # 1) Baca daftar kecamatan
    kecamatan_list = read_kecamatan(INPUT_XLSX, sheet=SHEET_NAME, kolom=KOLOM_KECAMATAN)
    # opsional: unik + sort
    kecamatan_list = sorted(set(kecamatan_list))

    results = []
    errors = 0

    # 2) Paralel request + progress bar
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_one, name): name for name in kecamatan_list}
        with tqdm(total=len(futures), desc="Mengambil cuaca (threads)", unit="lokasi") as pbar:
            for fut in as_completed(futures):
                row = fut.result()
                results.append(row)
                if row.get("Error"):
                    errors += 1
                    # tampilkan ringkas error pada progress bar
                    pbar.set_postfix(err=errors)
                pbar.update(1)

    # 3) Simpan ke Excel
    out_df = pd.DataFrame(results)
    out_df.to_excel(OUTPUT_XLSX, index=False)

    print(f"Selesai. {len(out_df)} baris disimpan ke: {OUTPUT_XLSX}")
    if errors:
        print(f"Peringatan: {errors} lokasi gagal diambil. Lihat kolom 'Error' di file output.")

if __name__ == "__main__":
    main()
