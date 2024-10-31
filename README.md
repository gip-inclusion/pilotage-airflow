
# Airflow & DBT du Pilotage de l'Inclusion

Ce dépôt a pour objectif de gérer l'intégralité (ou presque) des flux de données intervenant pour le Pilotage de l'Inclusion.

Il regroupe les scripts SQL et Python utilisés par les data analysts, ainsi que les DAGs [Airflow](https://airflow.apache.org/)
et modèles [DBT](https://docs.getdbt.com/) maintenus par l'équipe.

## Environnement & Installation

### Exigences du système

Pour lancer ce projet sur votre machine locale vous devez [installer Docker](https://docs.docker.com/engine/install/).

Vous devrez réserver temporairement environ 110 Go d'espace pour l'installation des données en direct sur votre machine (voir ce README). Une fois l'installation terminée, cette taille sera réduite à ~50 Go. Si vous utilisez Docker Desktop, l'espace maximum disponible pour le conteneur est inférieur par défaut, vous devrez donc augmenter l'espace réservé à Docker dans Settings > Resources > Virtual Disk Limit (Paramètres > Ressources > Limite de disque virtuel).

### Votre environnement virtuel Python

    make venv
    source .venv/bin/activate

### Le logiciel [direnv](https://direnv.net)

Sans oublier d'installer les [hooks](https://direnv.net/docs/hook.html) ni de
lancer `direnv allow` ensuite dans ce répertoire.
[Tuto](https://stackoverflow.com/questions/49083789/how-to-add-new-line-in-bashrc-file-in-ubuntu) pour ajouter une nouvelle ligne sur le fichier `bashrc` (linux) ou `.bash_profile` (mac OS)

Créer un fichier `.envrc.local` contenant au minimum le chemin vers le .venv : `echo "source .venv/bin/activate" >> .envrc.local`

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

## Docker

La base de données (PostgreSQL), le serveur S3 (MinIO) et Airflow peuvent tous être lancés ensemble avec

    docker compose up --build

## DBT et base de données Pilotage

Pour avoir une configuration "prête à l'emploi", il vous faut configurer la connexion au DB du prod
(e.g. PROD_PGHOST dans votre `.env`) et lancer les commandes suivantes :

    make load_dump
    dbt deps
    dbt seed

Après `load_dump`, si vous trouvez des erreurs liées à les `EXTENSIONS` non-existantes (comme `postgis` par exemple), veuillez les ignorer ;
elles sont causées par la différence entre votre environmment local et notre environmment de déploiement web.

Pour vérifier que DBT est bien configuré :

    dbt debug

Si tout va bien, vous pourrez ensuite utiliser DBT pour toutes vos opérations.

    dbt run

### Ajout de nouvelles colonnes dans les seeds

L'ajout de nouvelles colonnes dans les seeds ne fonctionnera pas avec la colonne

    dbt seed

Afin d'ajouter les nouvelles colonnes utiliser la fonction

    dbt seed --full-refresh

## Airflow

Démarrer Airflow avec Docker

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
