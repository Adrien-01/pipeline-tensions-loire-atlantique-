import pandas as pd
import os


# ==============================
# CONFIGURATION DES CHEMINS
# ==============================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_MEDECINS = os.path.join(PROJECT_ROOT, "data", "cleaned", "nb_medecins.csv")
INPUT_POPULATION = os.path.join(PROJECT_ROOT, "data", "cleaned", "population.csv")
INPUT_PRIX = os.path.join(PROJECT_ROOT, "data", "cleaned", "prix_moyen.csv")

OUTPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "dataset_final.csv")


# ==============================
# FONCTIONS
# ==============================

def load_data():
    print("[LOAD] Chargement des fichiers...")

    medecins = pd.read_csv(INPUT_MEDECINS)
    population = pd.read_csv(INPUT_POPULATION)
    prix = pd.read_csv(INPUT_PRIX)

    return medecins, population, prix


def preprocess(df_list):
    print("[PROCESS] Harmonisation des types...")

    for df in df_list:
        df["code_commune"] = df["code_commune"].astype(str)

    return df_list


def merge_data(medecins, population, prix):
    print("[MERGE] Fusion des datasets...")

    df = population.merge(medecins, on="code_commune", how="left")
    df = df.merge(prix, on="code_commune", how="left")

    df["nb_medecins"] = df["nb_medecins"].fillna(0)

    return df


def create_indicators(df):
    print("[FEATURE] Création des indicateurs...")

    # Eviter division par zéro
    df["nb_medecins"] = df["nb_medecins"].replace(0, pd.NA)

    # Densité médicale (pour 1000 habitants)
    df["densite_medecins"] = (df["nb_medecins"] / df["population"]) * 1000

    # Tension médicale (nb habitants par médecin)
    df["tension_medicale"] = df["population"] / df["nb_medecins"]

    # Prix par médecin (indicatif)
    df["prix_par_medecin"] = df["prix_moyen"] / df["nb_medecins"]

    # Score de tension (normalisation)
    df["score_tension"] = (
        (df["tension_medicale"] - df["tension_medicale"].mean()) /
        df["tension_medicale"].std()
    )

    return df


def categorize(df):
    print("[FEATURE] Catégorisation des communes...")

    def categorie(tension):
        if pd.isna(tension):
            return "inconnu"
        elif tension > 3000:
            return "désert médical"
        elif tension > 1500:
            return "tension"
        else:
            return "correct"

    df["categorie"] = df["tension_medicale"].apply(categorie)

    return df


def clean_final(df):
    print("[CLEAN] Nettoyage final...")

    # Option : supprimer les lignes sans population
    df = df.dropna(subset=["population"])

    return df


def save_data(df):
    print("[SAVE] Sauvegarde du dataset final...")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"[SUCCESS] Dataset enregistré ici : {OUTPUT_PATH}")


# ==============================
# MAIN
# ==============================

def main():
    medecins, population, prix = load_data()
    medecins, population, prix = preprocess([medecins, population, prix])

    df = merge_data(medecins, population, prix)
    df = create_indicators(df)
    df = categorize(df)
    df = clean_final(df)

    save_data(df)


if __name__ == "__main__":
    main()