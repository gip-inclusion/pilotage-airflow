# pilotage-airflow 

Ce dépôt a pour but de regrouper les scripts .sql et .py utilisés par les data analyst du pilotage de l'inclusion pour construire, versionner et documenter les tables de données via dbt et les deployer via airflow.

## airflow & dbt

### DBT

Configurez les variables d'environnement `PGDATABASE`, `PGHOST` , `PGPASSWORD`, `PGPORT` pour pouvoir utiliser dbt.

Pour vérifier que dbt est bien configuré, se rendre dans le répertoire `airflow_src` et lancer `dbt debug`.

### Airflow

Mettez en place votre environnement virtuel, base de données Postgres pour Airflow et fichier ``.env``.

.. code::

    pip install -r requirements-dev.txt
    
    createdb airflow

    vim .env
    
    [FILL WITH SOME OF THOSE]
    AIRFLOW_DATABASE_URL=postgresql://postgres:password@172.17.0.1:5432/airflow
    SECRET_KEY="local-dev-secret-key"
    AIRFLOW_SUPERUSER_PASSWORD="password"


Pour lancer airflow localement, ouvrez deux terminaux et:

.. code::

    cd airflow_src
    ./start-webserver.sh
    ./start-scheduler.sh

Vous pouvez ensuite accéder à l'interface Web sur http://127.0.0.1:8080 et lancer les DAGs.

L'utilisateur local de base a pour credentials "admin" / "password".

Les DAGs Airflow nécessitent quelques variables à configurer dans Airflow pour fonctionner;
voir ``variables.json`` pour ces dernières. Vous pouvez importer le fichier dans Admin/Variables.

