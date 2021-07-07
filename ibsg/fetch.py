from pathlib import Path
from typing import Union
from urllib.request import urlretrieve


def fetch(url: str, local: bool, data_dir: Path) -> Union[str, Path]:
    if local:
        filename = url.split("/")[-1]
        filepath = data_dir / filename
        if not filepath.exists():
            urlretrieve(url, filepath)
    else:
        filepath = url
    return filepath
