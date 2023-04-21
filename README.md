
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

    sudo apt install postgresql
    brew install postgresql
    # vérifiez que vous disposez bien de `psql`, `pg_dump` et `pg_restore`


### Le logiciel [direnv](https://direnv.net)

Sans oublier d'installer les [hooks](https://direnv.net/docs/hook.html)

### Votre fichier ``.env``

    cp env.example .env
    # modifiez le ficher `.env` pour y faire apparaître les bonnes valeurs

## Charger un dump local

Une commande Makefile est proposée, pourvu que vous ayiez rempli les variables d'environnement nécessaires
pour accéder à la base de données PostgreSQL de production.

    make load_dump

## DBT

Pour vérifier que DBT est bien configuré:

    cd airflow_src/
    dbt debug
    dbt seed
    dbt run

Si tout va bien, vous pourrez ensuite utiliser DBT pour pour toutes vos opérations.

## Airflow

Créez une base de données pour Airflow si elle n'existe pas.

    createdb airflow
    make airflow_init

Pour lancer airflow localement, ouvrez deux terminaux et:

    cd airflow_src/
    ./start-webserver.sh
    ./start-scheduler.sh

Vous pouvez ensuite accéder à l'interface Web sur http://127.0.0.1:8080 et lancer les DAGs.

L'utilisateur local de base a pour credentials "admin" / "password".

Les DAGs Airflow nécessitent quelques variables à configurer dans Airflow pour fonctionner;
voir ``variables.json`` pour ces dernières. Vous pouvez importer le fichier dans Admin/Variables.


> **Note**
>
> Il faut absolument éviter d'utiliser des variables d'environnement dans vos DAGs, et ser
> cantonner à l'usage de variables Airflow. En effet ces dernières sont en général utilisées
> par le métier pour agir rapidement et efficacement sur l'exécution des DAGs.
