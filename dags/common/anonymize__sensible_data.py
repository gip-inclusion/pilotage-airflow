import hashlib
import os
import re
import unicodedata
def hash_content(content: str) -> str:
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()


def normalize_sensible_data(first_name, last_name, birth_date):
    content = f"{first_name}{last_name}{birth_date.strftime('%d%m%Y')}"

    normalized_content = unicodedata.normalize("NFKD", content)

    normalized_content = re.sub((r"[^a-zA-Z0-9]+"), "", normalized_content).lower()

    return normalized_content
