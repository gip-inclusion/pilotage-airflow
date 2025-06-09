import hashlib
import os
import re
import unicodedata


def hash_content(content: str) -> str:
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()


def normalize_sensible_data(*args) -> str:
    normalized_args = []
    for arg in args:
        try:
            normalized_content = arg.isoformat()
        except AttributeError:
            normalized_content = unicodedata.normalize("NFKD", str(arg))
            normalized_content = re.sub((r"[^a-z]+"), "", normalized_content.lower())

        normalized_args.append(normalized_content)

    normalized_data = "|".join(normalized_args)

    return normalized_data
