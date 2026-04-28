import pandas as pd
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

INPUT_BPE = os.path.join(PROJECT_ROOT, "data", "raw", "DONNEES_FILTREES", "bpe_medecins_44.csv")
INPUT_DVF = os.path.join(PROJECT_ROOT, "data", "raw", "DONNEES_FILTREES", "dvf_44.csv")
INPUT_INSEE = os.path.join(PROJECT_ROOT, "data", "raw", "DONNEES_FILTREES", "insee_44.csv")

OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "cleaned")


def clean_dataset(path, geo_col, value_col, output_name, agg_func="sum"):
    df = pd.read_csv(path, sep=",", dtype={geo_col: str})

    # Nettoyage noms colonnes
    df.columns = df.columns.str.strip().str.upper()

    geo_col = geo_col.upper()
    value_col = value_col.upper()

    print(f"\n📂 Traitement : {output_name}")
    print("Colonnes disponibles :", df.columns.tolist())

    # Sélection
    df = df[[geo_col, value_col]]

    # Nettoyage
    df = df.dropna(subset=[geo_col, value_col])
    df[geo_col] = df[geo_col].astype(str).str.zfill(5)
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    # Renommage
    df = df.rename(columns={
        geo_col: "code_commune",
        value_col: output_name
    })

    # Agrégation
    if agg_func == "sum":
        df = df.groupby("code_commune", as_index=False)[output_name].sum()
    elif agg_func == "mean":
        df = df.groupby("code_commune", as_index=False)[output_name].mean()

    # Sauvegarde
    output_path = os.path.join(OUTPUT_DIR, f"{output_name}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.to_csv(output_path, index=False)

    print(f"✅ Fichier créé : {output_path}")


def main():
    #  1. Médecins (BPE)
    clean_dataset(
        INPUT_BPE,
        geo_col="GEO",
        value_col="OBS_VALUE",
        output_name="nb_medecins",
        agg_func="sum"
    )

    # 2. DVF (prix immobilier)
  
    clean_dataset(
        INPUT_DVF,
        geo_col="CODE_COMMUNE",
        value_col="VALEUR_FONCIERE",
        output_name="prix_moyen",
        agg_func="mean"
    )

    # 3. INSEE (population)

    clean_dataset(
        INPUT_INSEE,
        geo_col="COM",
        value_col="PTOT",
        output_name="population",
        agg_func="sum"
    )


if __name__ == "__main__":
    main()