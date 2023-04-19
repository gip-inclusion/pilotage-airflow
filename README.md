# pilotage-airflow

Ce dépôt a pour but de regrouper les scripts .sql et .py utilisés par les data analyst du pilotage de l'inclusion pour construire, versionner et documenter les tables de données via dbt et les deployer via airflow.

## airflow & dbt

### DBT

Pour lancer DBT en local:
- Assurez vous d'avoir un serveur de base de données local PostgreSQL et les outils client `psql`, `pg_dump`, `pg_restore`.
- Mettez en place votre environnement virtuel (https://direnv.net/) (pensez à bien activer le hook pour le déclenchement automatique de `direnv` dans le shell (https://direnv.net/docs/hook.html))
- Configurez votre fichier `.env`

Pour vérifier que DBT est bien configuré, se rendre dans le répertoire `airflow_src` et lancer `dbt debug`.

Installez les requirements et créez la base de données locale:

.. code::

    pip install -r requirements-dev.txt

    createdb airflow


### Airflow

Pour lancer airflow localement, ouvrez deux terminaux et:

.. code::

    cd airflow_src
    ./start-webserver.sh
    ./start-scheduler.sh

Vous pouvez ensuite accéder à l'interface Web sur http://127.0.0.1:8080 et lancer les DAGs.

L'utilisateur local de base a pour credentials "admin" / "password".

Les DAGs Airflow nécessitent quelques variables à configurer dans Airflow pour fonctionner;
voir ``variables.json`` pour ces dernières. Vous pouvez importer le fichier dans Admin/Variables.

