import os, requests, zipfile

base_url = "https://zenodo.org/records/17463358/files"
models = ["kurz_gold.zip", "kurz_gold_silver.zip", "voll_gold.zip", "voll_gold_silver.zip"]

os.makedirs("models", exist_ok=True)

for m in models:
    url = f"{base_url}/{m}"
    print(f"Downloading {m}...")
    r = requests.get(url)
    r.raise_for_status()
    with open(f"models/{m}", "wb") as f:
        f.write(r.content)
    with zipfile.ZipFile(f"models/{m}") as z:
        z.extractall("models/")

print("✅ All models downloaded to ./models/")
