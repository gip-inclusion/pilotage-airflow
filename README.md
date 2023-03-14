# data-gip-inclusion

Ce dépôt a pour but de regrouper les scripts .sql et .py utilisés de façon très (très) ponctuelle par les data analyst du GIP de la plateforme de l'inclusion.  
Ces derniers pourront ainsi sauvegarder, versionner et bénéficier d'une relecture de leur scripts.

## airflow & dbt

Mettez en place votre environnement virtuel et fichier ``.env``. Voir ``variables.json`` pour les variables airflow.

Utiliser:

.. code::

    make start

pour tester vos DAGs.
