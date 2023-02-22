drop table if exists ref_mesure_dispositif_asp;
create table ref_mesure_dispositif_asp as 
select 
    distinct(af.af_mesure_dispositif_code),
    case 
        when af.af_mesure_dispositif_code = 'AI_DC' then 'AI Droit commun'
        when af.af_mesure_dispositif_code = 'ACIPA_DC' then 'ACIPA Droit commun'
        when af.af_mesure_dispositif_code = 'ACI_DC' then 'ACI Droit commun'
        when af.af_mesure_dispositif_code = 'EI_DC' then 'EI Droit commun'
        when af.af_mesure_dispositif_code = 'FDI_DC' then 'FDI Droit commun'
        when af.af_mesure_dispositif_code = 'EI_MP' then 'EI Milieu pénitentiaire'
        when af.af_mesure_dispositif_code = 'EITI_DC' then 'EITI Droit commun'
        when af.af_mesure_dispositif_code = 'EIPA_DC' then 'EIPA Droit commun'
        when af.af_mesure_dispositif_code = 'ETTI_DC' then 'ETTI Droit commun'
        when af.af_mesure_dispositif_code = 'ACI_MP' then 'ACI Milieu pénitentiaire'
    end type_structure
from "fluxIAE_AnnexeFinanciere_v2" as af;