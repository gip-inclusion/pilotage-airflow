# Journal des modifications

## 2025-04-05

### Ajouté

- [[benef/sorties] Import du fichier fourni par l'ASP dans le cadre du TB cabinet](https://github.com/gip-inclusion/pilotage-airflow/pull/428)

### Modifié

- [Ajout libellés aux tables accompagnement](https://github.com/gip-inclusion/pilotage-airflow/pull/432)
- [Correction import données ASP cab](https://github.com/gip-inclusion/pilotage-airflow/pull/430)
- [Modifications sur la table potentiel + traduction en python](https://github.com/gip-inclusion/pilotage-airflow/pull/431)
- [Retrait des doublons de la table structures de l'ASP](https://github.com/gip-inclusion/pilotage-airflow/pull/429)
- [table contrats_parent: utilisation du type de dispositif issu du seed ref_me…](https://github.com/gip-inclusion/pilotage-airflow/pull/433)

## 2025-03-29

### Ajouté

- [[accompagnement] creation table accompagnement_pro](https://github.com/gip-inclusion/pilotage-airflow/pull/425)
- [[parcours] creation table parcours_salarie](https://github.com/gip-inclusion/pilotage-airflow/pull/411)

### Tech

- [DAG : Amélioration des performances mémoires pour `populate_metabase_fluxiae`](https://github.com/gip-inclusion/pilotage-airflow/pull/427)
- [Mettre à niveau Python à 3.11](https://github.com/gip-inclusion/pilotage-airflow/pull/426)

## 2025-03-22

### Ajouté

- [[accompagnement ]creation table accompagnement_freins](https://github.com/gip-inclusion/pilotage-airflow/pull/424)
- [[Sorties, recrutement, benef] Ajout de la zone d'emploi et de la tranche d'age + renommage bRSA](https://github.com/gip-inclusion/pilotage-airflow/pull/423)
- [Ajout d'informations sur les candidatures des candidats de la file active pour permettre le filtrage sur les TB privés](https://github.com/gip-inclusion/pilotage-airflow/pull/422)

### Tech

- [[monrecap] Amelioration technico technique](https://github.com/gip-inclusion/pilotage-airflow/pull/421)

## 2025-03-15

### Ajouté

- [DAG : Prise en compte de `RefCotisationEiti` pour le flux IAE](https://github.com/gip-inclusion/pilotage-airflow/pull/419)
- [Importation des statistiques sur les demandeurs d'emploi de l'API France Travail](https://github.com/gip-inclusion/pilotage-airflow/pull/409)

### Modifié

- [[Monrecap] Modification type colonne barometre](https://github.com/gip-inclusion/pilotage-airflow/pull/418)
- [Conversion dates monrecap + ajout colonne](https://github.com/gip-inclusion/pilotage-airflow/pull/420)
- [Resolution d'une erreur de doublons dans la table stg_contrats (merci Giulia !)](https://github.com/gip-inclusion/pilotage-airflow/pull/417)
- [Resolution de l'erreur data consistency sur les modèles candidats_recherche_active](https://github.com/gip-inclusion/pilotage-airflow/pull/416)

## 2025-03-08

### Ajouté

- [add fluxIAE_Accompagnement to dag](https://github.com/gip-inclusion/pilotage-airflow/pull/413)
- [Ajout le nom du prescripteur aux candidatures des candidats en recherche active](https://github.com/gip-inclusion/pilotage-airflow/pull/415)

### Modifié

- [[Sorties] Retrait des sorties non comptabilisées par l'ASP](https://github.com/gip-inclusion/pilotage-airflow/pull/414)

## 2025-03-01

### Tech

- [Modification du pg_restore afin de prendre en compte que les tables nécessaires au dbt run](https://github.com/gip-inclusion/pilotage-airflow/pull/389)
- [Profiter de l'arrivée de Guilia pour faire du ménage (merci!) 🧹](https://github.com/gip-inclusion/pilotage-airflow/pull/410)

## 2025-02-22

### Ajouté

- [Création d'une table bénéficiaires](https://github.com/gip-inclusion/pilotage-airflow/pull/407)

### Supprimé

- [[monrecap] retrait contacts commandeurs de la table non commandeurs](https://github.com/gip-inclusion/pilotage-airflow/pull/408)

## 2025-02-15

### Ajouté

- [Ajout du genre et du statut brsa dans la table sorties](https://github.com/gip-inclusion/pilotage-airflow/pull/405)
- [Create DAG populate_metabase_fluxiae](https://github.com/gip-inclusion/pilotage-airflow/pull/368)

### Modifié

- [Renommer les dernières mentions à "PE" en "FT"](https://github.com/gip-inclusion/pilotage-airflow/pull/404)

## 2025-02-08

### Ajouté

- [Ajout du nom prénom du conseiller aux candidats file active](https://github.com/gip-inclusion/pilotage-airflow/pull/401)
- [Rétention à deux mois](https://github.com/gip-inclusion/pilotage-airflow/pull/399)

### Modifié

- [Changement nom colonne mon recap](https://github.com/gip-inclusion/pilotage-airflow/pull/403)
- [Correction sur la table rétention](https://github.com/gip-inclusion/pilotage-airflow/pull/402)

## 2025-02-01
