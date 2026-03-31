# Pipeline d’analyse des tensions territoriales en Loire-Atlantique

![Python](https://img.shields.io/badge/python-3.11-blue)
![License](https://img.shields.io/badge/license-MIT-green)

## 🎯 Objectif
Analyser les déséquilibres territoriaux en Loire-Atlantique en croisant :
- population,
- logement,
- accès aux soins (médecins).

Le projet construit un **pipeline automatisé** pour ingestion, nettoyage, structuration et analyse des données.

---

## 🗂️ Structure du dépôt

- `data/` : fichiers bruts (`raw`), nettoyés (`staging`) et consolidés (`processed`)  
- `scripts/` : scripts Python pour chaque étape du pipeline  
- `notebooks/` : analyse et visualisation  
- `sql/` : scripts SQL pour création et manipulation des tables  
- `output/` : graphiques, cartes et exports finaux  

---

## 🧭 Méthodologie

1. **Ingestion** : téléchargement automatique des données depuis INSEE et data.gouv.fr  
2. **Nettoyage** : uniformisation, filtrage Loire-Atlantique, préparation staging  
3. **Transformation** : fusion population-logement-santé, calcul de ratios  
4. **Analyse** : identification des zones sous tension et visualisation  

---

## 🔧 Comment exécuter le pipeline

```bash
# Cloner le dépôt
git clone <URL_DU_REPO>
cd pipeline-tensions-loire-atlantique

# Lancer les scripts étape par étape
python scripts/01_ingestion.py
python scripts/02_cleaning.py
python scripts/03_transform.py
python scripts/04_analysis.py
