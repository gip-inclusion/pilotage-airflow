import hashlib
import os
import re
import unicodedata


def hash_content(content: str) -> str:
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()


def normalize_sensible_data(*args: str) -> str:
    normalized_args = []
    for arg in args:
        if not isinstance(arg, str):
            raise TypeError(f"Expected str, got {type(arg).__name__}")

        normalized_content = unicodedata.normalize("NFKD", arg)
        normalized_content = re.sub((r"[^a-z0-9]+"), "", normalized_content.lower())

        normalized_args.append(normalized_content)

    normalized_data = "-".join(normalized_args)

    return normalized_data
