from pathlib import Path
from typing import Union
from urllib.request import urlretrieve

from ibsg import _LOCAL
from ibsg import _DATA_DIR


def fetch(url: str) -> Union[str, Path]:
    if _LOCAL:
        filename = url.split("/")[-1]
        filepath = _DATA_DIR / filename
        if not filepath.exists():
            urlretrieve(url, filepath)
    else:
        filepath = url
    return filepath
