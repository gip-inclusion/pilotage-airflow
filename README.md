
# Airflow & DBT du Pilotage de l'Inclusion

Ce dépôt a pour objectif de gérer l'intégralité (ou presque) des flux de données intervenant pour le Pilotage de l'Inclusion.

Il regroupe les scripts SQL et Python utilisés par les data analysts, ainsi que les DAGs [Airflow](https://airflow.apache.org/)
et modèles [DBT](https://docs.getdbt.com/) maintenus par l'équipe.

## Environnement & Installation

### Votre environnement virtuel Python

    make venv
    source venv/bin/activate


### Votre serveur de base de données PostgreSQL pour Airflow & DBT

    sudo apt install postgresql  # sous Linux
    brew install postgresql  # sous MacOS



### Le logiciel [direnv](https://direnv.net)

Sans oublier d'installer les [hooks](https://direnv.net/docs/hook.html) ni de
lancer `direnv allow` ensuite dans ce répertoire.
[Tuto](https://stackoverflow.com/questions/49083789/how-to-add-new-line-in-bashrc-file-in-ubuntu) pour ajouter une nouvelle ligne sur le fichier `bashrc` (linux) ou `.bash_profile` (mac OS)

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

Notez que le fichier commité `.env-base` contient les bonnes variables d'environnement
quel que soit votre environnement de développement et n'est pas censé être "personnalisé".


## DBT et base de données Pilotage

Pour vérifier que DBT est bien configuré :

    dbt debug

Si votre base de données n'existe pas vérifiez les bases existantes avec :

    psql -l

Si elle n'apparait pas, créez la avec :

    createdb pilotage


Pour avoir une configuration "prête à l'emploi", il vous faut lancer les commandes suivantes :

    make load_dump
    dbt deps
    dbt seed

Si tout va bien, vous pourrez ensuite utiliser DBT pour toutes vos opérations.

    dbt run

### Charger un dump local pilotage

Une commande Makefile est proposée, pourvu que vous ayez rempli les variables d'environnement nécessaires
pour accéder à la base de données PostgreSQL de production.

    make load_dump

### Ajout de nouvelles colonnes dans les seeds

L'ajout de nouvelles colonnes dans les seeds ne fonctionnera pas avec la colonne

    dbt seed

Afin d'ajouter les nouvelles colonnes utiliser la fonction

    dbt seed --full-refresh

## Airflow

Créez une base de données pour Airflow si elle n'existe pas.

    docker compose up

Vous pouvez ensuite accéder à l'interface Web sur http://127.0.0.1:8080 et lancer les DAGs.

L'utilisateur local de base a pour credentials "admin" / "password".

## Linter & en forme de vos scripts

Si les scripts ne respectent pas la mise en forme des différents linters ils ne passeront pas les tests de la CI.
Afin d'effectuer une mise en forme automatique la commande ci-dessous peut être lancée.
Elle analysera tous les fichiers et les mettra en forme automatiquement.

    make fix

> **Note**
>
> Il faut absolument éviter d'utiliser des variables d'environnement dans vos DAGs pour se cantonner à l'usage de variables Airflow.
> Ces dernières sont en général utilisées par le métier pour agir rapidement et efficacement sur l'exécution des DAGs.
