import pandas as pd


def subdf_by_value_if_exists(df, column, value):
    if value not in set(column):
        return pd.DataFrame()
    return df.groupby(column).get_group(value)


def model(dbt, session):
    organisations = dbt.ref("organisations")
    institutions = dbt.source("emplois", "institutions")
    structures = dbt.source("emplois", "structures")
    structures = structures[structures["active"] == 1]

    regions = filter(None, institutions["région"].unique())

    potential_records = []

    for region in regions:
        region_inst = institutions.groupby(institutions.région).get_group(region)
        region_struct = subdf_by_value_if_exists(structures, structures.région, region)
        region_orga = subdf_by_value_if_exists(organisations, organisations.région_org, region)

        depts = [d for d in region_inst["nom_département"].unique() if d is not None]

        for dept in depts:
            num_dept = dept.split(" ")[0]
            # institutions
            dept_inst = region_inst.groupby(region_inst.nom_département).get_group(dept)
            for inst_type in dept_inst["type"].unique():
                potential = len(dept_inst[dept_inst.type == inst_type])
                potential_records.append([region, num_dept, dept, "institution", inst_type, potential])

            # organisations
            if not region_orga.empty:
                dept_orga = subdf_by_value_if_exists(region_orga, region_orga.dept_org, dept)
                for orga_type in dept_orga["type"].unique():
                    potential = len(dept_orga[dept_orga.type == orga_type])
                    potential_records.append([region, num_dept, dept, "prescripteur", orga_type, potential])

            # structures
            if not region_struct.empty:
                dept_struct = subdf_by_value_if_exists(region_struct, region_struct.nom_département, dept)
                for struct_type in dept_struct["type"].unique():
                    potential = len(dept_struct[dept_struct.type == struct_type])
                    potential_records.append([region, num_dept, dept, "siae", struct_type, potential])

    return pd.DataFrame.from_records(
        potential_records,
        columns=["région", "département_num", "département", "type_utilisateur", "profil", "potentiel"],
    )
