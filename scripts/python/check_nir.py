#!/usr/bin/env python3
import string
import sys
import re

# https://fr.wikipedia.org/wiki/Num%C3%A9ro_de_s%C3%A9curit%C3%A9_sociale_en_France#Signification_des_chiffres_du_NIR
NIR_RE = re.compile(
    r"""
    ^
    (?P<sex>[1-478])
    (?P<birth_year>\d{2})
    (?P<birth_month>0[1-9]|1[0-2]|[2-3]\d|4[0-2]|[5-9]\d)
    (?:
      (?:
        (?P<birth_department_continental>0[1-9]|[1-8]\d|9[0-6]|2[AB])
        (?P<birth_city_continental>00[1-9]|0[1-9]\d|[1-9]\d{2})
      ) | (?:
        (?P<birth_department_ultramarine>9[78]\d)
        (?P<birth_city_ultramarine>0[1-9]|[1-9]\d)
      ) | (?:
        99
        (?P<birth_country>00[1-9]|0[1-9]\d|[1-9]\d{2})
      )
    )
    (?P<birth_order>00[1-9]|0[1-9]\d|[1-9]\d{2})
    (?P<key>0[1-9]|[1-8]\d|9[0-7])
    $""",
    re.IGNORECASE | re.VERBOSE,
    )


def main():
    with open(sys.argv[1]) as f:
        for line in f:
            nir = line.removesuffix("\n")
            if NIR_RE.match(nir):
                if int(nir[-2:]) == (97 - int(nir[:13]) % 97):
                    print(f"{nir}: OK")
                else:
                    print(f"{nir}: Wrong control key")
            else:
                print(f"{nir}: Wrong format")
                if any([char in nir for char in string.whitespace]):
                    print(f" > Value contain at least one whitespace character")



if __name__ == '__main__':
    main()
