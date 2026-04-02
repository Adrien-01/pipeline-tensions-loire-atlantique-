# scripts/01_ingestion.py

import os
import requests
import pandas as pd
import gzip
import zipfile

# -------------------- CHEMINS -------------------- #

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw")
RAW_DATA_PATH_FILTER = os.path.join(RAW_DATA_PATH, "DONNEES_FILTREES")
os.makedirs(RAW_DATA_PATH, exist_ok=True)
print(f"[INFO] RAW_DATA_PATH = {RAW_DATA_PATH}")

# URLs
DVF_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/2023/full.csv.gz"
INSEE_ZIP_URL = "https://www.insee.fr/fr/statistiques/fichier/8290591/ensemble.zip"
BPE_URL = "https://www.insee.fr/fr/statistiques/fichier/8217527/DS_BPE_CSV_FR.zip"

# -------------------- FONCTIONS -------------------- #

def create_directories():
    os.makedirs(RAW_DATA_PATH, exist_ok=True)
    os.makedirs(RAW_DATA_PATH_FILTER, exist_ok=True)
    print(f"[INFO] Répertoires créés : {RAW_DATA_PATH}")
    print(f"[INFO] Répertoires créés (filtres) : {RAW_DATA_PATH_FILTER}")

def download_file(url, filename):
    filepath = os.path.join(RAW_DATA_PATH, filename)
    if os.path.exists(filepath):
        print(f"[SKIP] {filename} déjà présent")
        return filepath
    print(f"[DOWNLOAD] {filename}")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"[OK] Téléchargé : {filepath}")
    return filepath

# -------------------- DVF -------------------- #

def filter_dvf_loire_atlantique(input_path, output_filename="dvf_44.csv"):
    output_path = os.path.join(RAW_DATA_PATH_FILTER, output_filename)
    print("[PROCESS] Filtrage DVF Loire-Atlantique...")

    chunksize = 100_000
    first_chunk = True
    with gzip.open(input_path, 'rt', encoding='utf-8') as f:
        for chunk in pd.read_csv(f, chunksize=chunksize, low_memory=False):
            filtered = chunk[chunk["code_departement"].astype(str).str.strip() == "44"]
            if not filtered.empty:
                if first_chunk:
                    filtered.to_csv(output_path, index=False)
                    first_chunk = False
                else:
                    filtered.to_csv(output_path, mode='a', header=False, index=False)

    print(f"[OK] DVF filtré : {output_path}")
    return output_path


# -------------------- INSEE -------------------- #

def extract_insee_zip(zip_path, target_filename="donnees_communes.csv"):
    """
    Extrait uniquement le fichier CSV voulu depuis le ZIP INSEE.
    """
    print(f"[PROCESS] Extraction ZIP INSEE : {zip_path}")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        if target_filename not in zip_ref.namelist():
            raise FileNotFoundError(f"{target_filename} non trouvé dans le ZIP")
        zip_ref.extract(target_filename, RAW_DATA_PATH)
    csv_path = os.path.join(RAW_DATA_PATH, target_filename)
    print(f"[OK] Fichier INSEE extrait : {csv_path}")
    return csv_path

def filter_insee_loire_atlantique(input_path, output_filename="insee_44.csv"):
    """
    Filtre le fichier INSEE sur le département 44 (Loire-Atlantique)
    """
    output_path = os.path.join(RAW_DATA_PATH_FILTER, output_filename)
    print("[PROCESS] Filtrage INSEE Loire-Atlantique...")

    # Lecture CSV
    df = pd.read_csv(input_path, sep=";", encoding="utf-8")
    df["code_departement"] = df["DEP"].astype(str).str[:2]

    # Filtrer sur 44
    df_44 = df[df["code_departement"] == "44"]

    df_44.to_csv(output_path, index=False)
    print(f"[OK] INSEE filtré : {output_path}, {len(df_44)} lignes")
    return output_path

# -------------------- BPE -------------------- #
def extract_bpe_zip(zip_path, target_filename="DS_BPE_2024_data.csv"):
    print(f"[PROCESS] Extraction du fichier {target_filename} depuis le ZIP")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()

        # Debug (très utile)
        print("[DEBUG] Fichiers dans le ZIP :", file_list)

        # Vérifie que le fichier existe
        if target_filename not in file_list:
            raise FileNotFoundError(f"{target_filename} non trouvé dans le ZIP")

        # Extraction uniquement du bon fichier
        zip_ref.extract(target_filename, RAW_DATA_PATH)

    csv_path = os.path.join(RAW_DATA_PATH, target_filename)
    print(f"[OK] Fichier extrait : {csv_path}")
    return csv_path

def filter_bpe_loire_atlantique(input_path, output_filename="bpe_medecins_44.csv"):
    output_path = os.path.join(RAW_DATA_PATH_FILTER, output_filename)
    print("[PROCESS] Filtrage BPE Loire-Atlantique (communes + médecins)...")

    df = pd.read_csv(input_path, sep=";", encoding="utf-8", quotechar='"')

    # On s'assure que GEO est bien une string
    df["GEO"] = df["GEO"].astype(str)

    # Filtrage :
    df_filtered = df[
        (df["GEO_OBJECT"] == "COM") &          # niveau commune
        (df["GEO"].str.startswith("44")) &     # département 44
        (df["FACILITY_TYPE"] == "D265")        # médecins généralistes
    ]

    df_filtered.to_csv(output_path, index=False)

    print(f"[OK] BPE filtré : {output_path}, {len(df_filtered)} lignes")
    return output_path

# -------------------- PIPELINE -------------------- #

def run_ingestion():
    create_directories()

    # --- DVF ---
    dvf_path = download_file(DVF_URL, "dvf_full.csv.gz")
    dvf_output = os.path.join(RAW_DATA_PATH, "dvf_44.csv")
    if not os.path.exists(dvf_output):
        filter_dvf_loire_atlantique(dvf_path)
    else:
        print(f"[SKIP] DVF déjà filtré : {dvf_output}")

    # --- INSEE ---
    insee_zip_path = download_file(INSEE_ZIP_URL, "insee_ensemble.zip")
    insee_csv_path = extract_insee_zip(insee_zip_path, target_filename="donnees_communes.csv")
    insee_output = os.path.join(RAW_DATA_PATH, "insee_44.csv")
    
    if not os.path.exists(insee_output):
      filter_insee_loire_atlantique(insee_csv_path)
    else:
     print(f"[SKIP] INSEE déjà filtré : {insee_output}")

    # --- BPE ---
    # Téléchargement
    bpe_zip_path = download_file(BPE_URL, "DS_BPE_CSV_FR.zip")
    # Extraction
    bpe_csv_path = extract_bpe_zip(bpe_zip_path, target_filename="DS_BPE_2024_data.csv")
    # Filtrage
    bpe_output = os.path.join(RAW_DATA_PATH, "bpe_44.csv")
    if not os.path.exists(bpe_output):
        filter_bpe_loire_atlantique(bpe_csv_path)
    else:
        print(f"[SKIP] BPE déjà filtré : {bpe_output}")

# -------------------- EXECUTION -------------------- #

if __name__ == "__main__":
    run_ingestion()