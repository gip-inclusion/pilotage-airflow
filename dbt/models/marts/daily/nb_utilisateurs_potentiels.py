import pandas as pd


def model(dbt, session):
    organisations = dbt.ref("organisations")
    institutions = dbt.source("emplois", "institutions")
    structures = dbt.source("emplois", "structures")
    depts = [d for d in organisations["dept_org"].unique() if d is not None]
    regions = [r for r in organisations["région_org"].unique() if r is not None]

    potential_records = []

    for dept in depts:
        num_dept = dept.split(" ")[0]
        for region in regions:
            # organisations
            for orga_type in organisations["type"].unique():
                potential = len(
                    organisations[
                        (organisations["type"] == orga_type)
                        & (organisations["dept_org"] == dept)
                        & (organisations["région_org"] == region)
                    ]
                )
                potential_records.append([region, num_dept, dept, "prescripteur", orga_type, potential])

            # institutions
            for inst_type in institutions["type"].unique():
                potential = len(
                    institutions[
                        (institutions["type"] == inst_type)
                        & (institutions["nom_département"] == dept)
                        & (institutions["région"] == region)
                    ]
                )
                potential_records.append([region, num_dept, dept, "institution", inst_type, potential])

            # structures
            structures = structures[structures["active"] == 1]
            for struct_type in structures["type"].unique():
                potential = len(
                    structures[
                        (structures["type"] == struct_type)
                        & (structures["nom_département"] == dept)
                        & (structures["région"] == region)
                    ]
                )
                potential_records.append([region, num_dept, dept, "siae", struct_type, potential])

    return pd.DataFrame.from_records(
        potential_records,
        columns=["région", "département_num", "département", "type_utilisateur", "profil", "potential"],
    )
