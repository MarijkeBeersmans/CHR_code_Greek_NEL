import os, requests, zipfile, tqdm

base_url = "https://zenodo.org/records/17463358/files"
#models = ["kurz_gold_silver.zip","kurz_gold.zip", "voll_gold.zip", "voll_gold_silver.zip"]
models = ["kurz_gold_silver.zip"]

os.makedirs("models", exist_ok=True)

for m in models:
    url = f"{base_url}/{m}"
    print(f"Downloading {m}...")
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(f'models/{m}', "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    with zipfile.ZipFile(f"models/{m}") as z:
        z.extractall("models/")

print("✅ All models downloaded to ./models/")
