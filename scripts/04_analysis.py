import pandas as pd
import os
import matplotlib.pyplot as plt


# ==============================
# CONFIGURATION
# ==============================

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(PROJECT_ROOT, "data", "processed", "dataset_final.csv")


# ==============================
# LOAD
# ==============================

def load_data():
    print("[LOAD] Chargement du dataset final...")
    df = pd.read_csv(INPUT_PATH)
    return df


# ==============================
# ANALYSE DESCRIPTIVE
# ==============================

def describe_data(df):
    print("\n[INFO] Aperçu du dataset :")
    print(df.head())

    print("\n[STATS] Statistiques générales :")
    print(df.describe())


# ==============================
# TOP COMMUNES
# ==============================

def top_tension(df):
    print("\n[TOP] Communes les plus en tension médicale :")
    top = df.sort_values("tension_medicale", ascending=False).head(10)
    print(top[["code_commune", "tension_medicale", "population", "nb_medecins"]])


def top_densite(df):
    print("\n[TOP] Communes avec la meilleure densité médicale :")
    top = df.sort_values("densite_medecins", ascending=False).head(10)
    print(top[["code_commune", "densite_medecins", "population", "nb_medecins"]])


# ==============================
# CORRÉLATION
# ==============================

def correlation(df):
    print("\n[CORR] Corrélation :")
    corr = df[["prix_moyen", "densite_medecins", "tension_medicale"]].corr()
    print(corr)


# ==============================
# VISUALISATIONS
# ==============================

def plot_distribution(df):
    print("\n[PLOT] Distribution de la tension médicale...")

    plt.figure()
    df["tension_medicale"].hist(bins=30)
    plt.title("Distribution de la tension médicale")
    plt.xlabel("Habitants par médecin")
    plt.ylabel("Nombre de communes")
    plt.show()


def plot_scatter(df):
    print("\n[PLOT] Relation prix vs densité médicale...")

    plt.figure()
    plt.scatter(df["prix_moyen"], df["densite_medecins"])
    plt.title("Prix immobilier vs densité médicale")
    plt.xlabel("Prix moyen")
    plt.ylabel("Densité médicale")
    plt.show()


# ==============================
# CATEGORIES
# ==============================

def analyse_categories(df):
    print("\n[CATEGORIE] Répartition des communes :")
    print(df["categorie"].value_counts())


# ==============================
# MAIN
# ==============================

def main():
    df = load_data()

    describe_data(df)
    top_tension(df)
    top_densite(df)
    correlation(df)
    analyse_categories(df)

    plot_distribution(df)
    plot_scatter(df)


if __name__ == "__main__":
    main()