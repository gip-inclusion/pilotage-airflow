import datetime
import enum
import hashlib
import os
import re

from unidecode import unidecode


class NormalizationKind(enum.Enum):
    NAME = enum.auto()
    DATE = enum.auto()


def hash_content(content: str) -> str:
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()


def normalize_sensible_data(*args: tuple[str | datetime.date, NormalizationKind]) -> str:
    normalized_args = []
    for datum, normalization_kind in args:
        match normalization_kind:
            case NormalizationKind.NAME:
                normalized_datum = re.sub(r"[^a-z]+", "", unidecode(datum).lower())
            case NormalizationKind.DATE:
                # Truncate to 10 characters in case a datetime.datetime() is given
                normalized_datum = datum.isoformat()[:10]
            case _:
                raise ValueError(f"Unknown normalization kind: {normalization_kind}")
        normalized_args.append(normalized_datum)

    return "|".join(normalized_args)
