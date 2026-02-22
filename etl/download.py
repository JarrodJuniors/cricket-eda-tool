"""ETL Download — fetches Cricsheet bulk ZIP files."""

import zipfile
from pathlib import Path

import httpx
from tqdm import tqdm

# Cricsheet JSON bulk download URLs
CRICSHEET_URLS = {
    "ipl":   "https://cricsheet.org/downloads/ipl_json.zip",
    "t20i":  "https://cricsheet.org/downloads/t20s_json.zip",
    "odi":   "https://cricsheet.org/downloads/odis_json.zip",
    "test":  "https://cricsheet.org/downloads/tests_json.zip",
    "people": "https://cricsheet.org/downloads/people.csv",
}


def download_competition(
    competition: str,
    raw_dir: Path,
    force: bool = False,
) -> Path:
    """Download and extract a Cricsheet ZIP for a given competition.

    Args:
        competition: One of "ipl", "t20i", "odi", "test".
        raw_dir: Base raw data directory (data/raw/).
        force: Re-download even if target directory exists.

    Returns:
        Path to the extracted directory.
    """
    if competition not in CRICSHEET_URLS:
        raise ValueError(f"Unknown competition '{competition}'. Choose from: {list(CRICSHEET_URLS)}")

    url = CRICSHEET_URLS[competition]
    target_dir = raw_dir / competition
    zip_path = raw_dir / f"{competition}.zip"

    if target_dir.exists() and not force:
        print(f"  ✅ {competition}: already downloaded at {target_dir}")
        return target_dir

    target_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"  ⬇️  Downloading {competition} from {url} ...")
    with httpx.stream("GET", url, follow_redirects=True, timeout=120) as response:
        response.raise_for_status()
        total = int(response.headers.get("content-length", 0))
        with open(zip_path, "wb") as f:
            with tqdm(total=total, unit="B", unit_scale=True, desc=competition) as pbar:
                for chunk in response.iter_bytes(chunk_size=65536):
                    f.write(chunk)
                    pbar.update(len(chunk))

    print(f"  📦 Extracting to {target_dir} ...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(target_dir)

    zip_path.unlink()  # remove ZIP after extract
    print(f"  ✅ {competition}: {len(list(target_dir.glob('*.json')))} match files extracted")
    return target_dir


def download_people(raw_dir: Path) -> Path:
    """Download the Cricsheet people.csv (player registry)."""
    raw_dir.mkdir(parents=True, exist_ok=True)
    dest = raw_dir / "people.csv"
    print("  ⬇️  Downloading people.csv ...")
    response = httpx.get(CRICSHEET_URLS["people"], follow_redirects=True, timeout=30)
    response.raise_for_status()
    dest.write_bytes(response.content)
    print(f"  ✅ people.csv saved ({dest.stat().st_size // 1024} KB)")
    return dest
