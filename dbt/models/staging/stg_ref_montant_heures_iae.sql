select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_RefMontantIae')) }}
from {{ source('fluxIAE', 'fluxIAE_RefMontantIae') }}
where
    -- here we isolate the number of hours per year and per structure type neeeded to reach 1 annual ETP
    rmi_libelle = 'Nombre d''heures annuelles théoriques pour un salarié à taux plein'
