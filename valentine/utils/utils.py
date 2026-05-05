from pathlib import Path

import nltk


def ensure_nltk_data() -> None:
    """Download NLTK corpora to ~/nltk_data and register the path.

    Downloads to the user's home directory rather than AppData/Roaming to avoid
    Windows MSIX app-virtualization path aliasing, which causes NLTK's path
    security check to reject files whose resolved path differs from the stored root.
    """

    download_dir = str(Path.home() / "nltk_data")
    if download_dir not in nltk.data.path:
        nltk.data.path.insert(0, download_dir)

    resources = ["punkt_tab", "omw-1.4", "stopwords", "wordnet"]
    failed = [r for r in resources if not nltk.download(r, quiet=True, download_dir=download_dir)]
    if failed:
        raise LookupError(
            f"Failed to download required NLTK resource(s): {failed}. "
            "Check your network connection or run "
            "`python -m nltk.downloader " + " ".join(failed) + "` manually."
        )


def convert_data_type(string: str):
    try:
        f = float(string)
        if f.is_integer():
            return int(f)
    except ValueError:
        return string
    else:
        return f
