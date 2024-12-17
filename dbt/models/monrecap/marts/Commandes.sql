select {{ pilo_star(source('monrecap','Commandes_v0')) }} from {{ source('monrecap', 'Commandes_v0') }}
