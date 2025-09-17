#!/usr/bin/env python3

import csv
import sys
from collections import Counter
from math import ceil


FILE_BASE_COLUMNS = ["code_ligne", "code_struct", "nir", "nom", "prenom", "date_naissance", "siren", "siret", "extra"]

FILE_EXTRA_COLUMNS = [
    "mois_sortie",
    "type_structure_emplois",
    "ZE2020",
    "donnee_partenaire_4",
    "donnee_partenaire_5",
    "donnee_partenaire_6",
    "donnee_partenaire_7",
    "donnee_partenaire_8",
    "donnee_partenaire_9",
    "donnee_partenaire_10",
    "donnee_partenaire_11",
    "donnee_partenaire_12",
    "donnee_partenaire_13",
    "donnee_partenaire_14",
    "donnee_partenaire_15",
    "donnee_partenaire_16",
    "donnee_partenaire_17",
    "donnee_partenaire_18",
    "donnee_partenaire_19",
    "donnee_partenaire_20",
]


def main():
    counter = Counter()

    with open(sys.argv[1]) as f:
        next(f)  # skip header
        for row in csv.DictReader(f, fieldnames=FILE_BASE_COLUMNS, delimiter="|"):
            row.update(zip(FILE_EXTRA_COLUMNS, row["extra"].split(";"), strict=True))
            key = (
                str(row["nir"])[0],  # /!\ Not really accurate but we don't have anything better
                str(row["type_structure_emplois"]),
                str(row["ZE2020"]),
                str(row["mois_sortie"][4:8] + "Q" + str(ceil(int(row["mois_sortie"][2:4], base=10) / 3))),
            )
            counter.update([key])

    too_small_cohorts = {key for key, count in counter.items() if count < 5}
    print(f"Total cohorts count is {len(counter)} with {len(too_small_cohorts)} too small for {counter.total()} rows")

    for key in too_small_cohorts:
        print(f"Cohort {key!r} has only {counter[key]} rows")


if __name__ == "__main__":
    main()
