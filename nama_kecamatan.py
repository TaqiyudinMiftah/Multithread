import requests
import pandas as pd

def get_json(url):
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.json()

def main():
    # 1) Ambil semua provinsi
    resp = get_json("https://wilayah.id/api/provinces.json")
    provinces = resp["data"] 
    
    # 2) Cari provinsi Jawa Timur
    jatim = next(p for p in provinces if p["name"].upper() == "JAWA TIMUR")
    prov_code = jatim["code"]
    
    # 3) Ambil semua kabupaten/kota di Jawa Timur
    resp2 = get_json(f"https://wilayah.id/api/regencies/{prov_code}.json")
    regencies = resp2["data"]
    
    # 4) Ambil kecamatan dari tiap kabupaten/kota
    district_names = []
    for reg in regencies:
        reg_code = reg["code"]
        r3 = get_json(f"https://wilayah.id/api/districts/{reg_code}.json")
        districts = r3["data"]
        for d in districts:
            district_names.append(d["name"])
    
    # Unik dan sort
    district_names = sorted(set(district_names))
    
    # Simpan ke Excel
    df = pd.DataFrame({"Kecamatan": district_names})
    output_path = "kecamatan_jawa_timur_wilayahid.xlsx"
    df.to_excel(output_path, index=False)
    
    print(f"Berhasil menyimpan {len(df)} kecamatan ke file: {output_path}")

if __name__ == "__main__":
    main()
