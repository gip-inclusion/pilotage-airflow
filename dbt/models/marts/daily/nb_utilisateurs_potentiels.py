import pandas as pd


def model(dbt, session):
    organisations = dbt.ref("organisations")
    institutions = dbt.source("emplois", "institutions")
    structures = dbt.source("emplois", "structures")
    depts = [d for d in organisations["dept_org"].unique() if d is not None]
    regions = [r for r in organisations["région_org"].unique() if r is not None]

    potentiel_records = []

    for dept in depts:
        num_dept = dept.split(" ")[0]
        for reg in regions:
            # organisations
            for orga_type in organisations["type"].unique():
                cur_pot = len(
                    organisations[
                        (organisations["type"] == orga_type)
                        & (organisations["dept_org"] == dept)
                        & (organisations["région_org"] == reg)
                    ]
                )
                potentiel_records.append([reg, num_dept, dept, "prescripteur", orga_type, cur_pot])

            # institutions
            for inst_type in institutions["type"].unique():
                cur_pot = len(
                    institutions[
                        (institutions["type"] == inst_type)
                        & (institutions["nom_département"] == dept)
                        & (institutions["région"] == reg)
                    ]
                )
                potentiel_records.append([reg, num_dept, dept, "institution", inst_type, cur_pot])

            # structures
            structures = structures[structures["active"] == 1]
            for struct_type in structures["type"].unique():
                cur_pot = len(
                    structures[
                        (structures["type"] == struct_type)
                        & (structures["nom_département"] == dept)
                        & (structures["région"] == reg)
                    ]
                )
                potentiel_records.append([reg, num_dept, dept, "siae", struct_type, cur_pot])

    return pd.DataFrame.from_records(
        potentiel_records,
        columns=["région", "département_num", "département", "type_utilisateur", "profil", "potentiel"],
    )
