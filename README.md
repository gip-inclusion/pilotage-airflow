
# Airflow & DBT du Pilotage de l'Inclusion

Ce dépôt a pour objectif de gérer l'intégralité (ou presque) des flux de données intervenant pour le Pilotage de l'Inclusion.

Il regroupe les scripts SQL et Python utilisés par les data analysts, ainsi que les DAGs [Airflow](https://airflow.apache.org/)
et modèles [DBT](https://docs.getdbt.com/) maintenus par l'équipe.

## Environnement & Installation

### Votre environnement virtuel Python 3.10

    python -m venv venv
    source venv/bin/activate
    pip install -r requirements-ci.txt


> **Note**
>
> Pour l'instant nous devons utiliser `requirements-ci.txt` pour des raisons d'incompatibilité
> entre dépendances, cf. `generate-requirements.sh`


### Votre serveur de base de données PostgreSQL pour Airflow & DBT

    sudo apt install postgresql  # sous Linux
    brew install postgresql  # sous MacOS
    


### Le logiciel [direnv](https://direnv.net)

Sans oublier d'installer les [hooks](https://direnv.net/docs/hook.html) ni de
lancer `direnv allow` ensuite dans ce répertoire.
[Tuto](https://stackoverflow.com/questions/49083789/how-to-add-new-line-in-bashrc-file-in-ubuntu) pour ajouter une nouvelle ligne sur le fichier bashrc (linux) ou .bash_profile (mac OS)

### Votre fichier ``.env``

Le fichier `env.example` contient des valeurs par défaut directement utilisables,
sauf pour le chargement des dumps du Pilotage en production.

Il sert de modèle pour la création de votre propre fichier `.env`; n'hésitez pas
à le modifier si votre configuration locale (adresse, nom, port de votre base de
données...) est différente.

    cp env.example .env
    # modifiez le ficher `.env` si besoin
    # Une fois les variables d'environnement chargées vérifiez que vous disposez bien de `psql`, `pg_dump` et `pg_restore`

Pour définir ```PGPASSWORD=password``` vous pouvez utiliser la commande suivante 

    sudo -u postgres psql
    > ALTER USER postgres PASSWORD 'password';

Notez que le fichier commité `.env-base` contient des variables d'environnement
bonnes quel que soit votre environnement de développement et n'est pas censé
être "personnalisé".


## DBT et base de données Pilotage

Pour vérifier que DBT est bien configuré:

    dbt debug

Pour avoir une configuration "prête à l'emploi", il vous faut lancer les commandes suivantes:

    make load_dump
    dbt deps
    dbt seed

Si tout va bien, vous pourrez ensuite utiliser DBT pour pour toutes vos opérations.

    dbt run --select heures_etp_salaries

### Charger un dump local pilotage

Une commande Makefile est proposée, pourvu que vous ayiez rempli les variables d'environnement nécessaires
pour accéder à la base de données PostgreSQL de production.

    make load_dump

## Airflow

Créez une base de données pour Airflow si elle n'existe pas.

    # attention, cette commande remettra également à zéro votre base de données Airflow
    # lors du premier lancement de cette commande, le message d'erreur 'Variable PGHOST does not exist' s'affichera : c'est normal et il est à ignorer.
    make airflow_initdb
    

En particulier cette étape:

- crée la base de données `airflow`
- lui applique des migrations
- importe les variables "par défaut" `dag-variables.json`
- ajoute un utilsateur `admin/password`

Pour lancer airflow localement, ouvrez **DEUX** terminaux et:

    airflow webserver
    airflow scheduler

Vous pouvez ensuite accéder à l'interface Web sur http://127.0.0.1:8080 et lancer les DAGs.

L'utilisateur local de base a pour credentials "admin" / "password".

> **Note**
>
> Il faut absolument éviter d'utiliser des variables d'environnement dans vos DAGs, et ser
> cantonner à l'usage de variables Airflow. En effet ces dernières sont en général utilisées
> par le métier pour agir rapidement et efficacement sur l'exécution des DAGs.
