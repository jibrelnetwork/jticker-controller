from pathlib import Path

from jticker_core import register


@register(singleton=True)
def version() -> str:
    try:
        return Path(__file__).parent.parent.joinpath("version.txt").read_text().strip()
    except Exception:
        return "dev"
