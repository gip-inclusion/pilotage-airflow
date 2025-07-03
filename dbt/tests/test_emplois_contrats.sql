""" Normalement les contrat_id_ctr devrait être unique
    or ce n'est pas le cas car certaines fois on a plusieurs
    pass iae associables au même contrat_id_ctr """
with nb_pass_contrats as (
    select
        "hash_numéro_pass_iae",
        contrat_id_ctr,
        count(*) as occurrences
    from {{ ref("les_emplois_contrats") }}
    group by
        "hash_numéro_pass_iae",
        contrat_id_ctr
)

select *
from nb_pass_contrats
where occurrences > 1