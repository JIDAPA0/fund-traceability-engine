"""Launcher for the Streamlit-based local search web UI."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys


def main() -> int:
    try:
        import streamlit  # noqa: F401
    except Exception:
        print('run_local_search_web_ui failed: streamlit is not installed. Install with `pip install -e ".[ui]"`.')
        return 1

    app_path = Path(__file__).resolve().parents[1] / "apps" / "local_search_web_ui.py"
    cmd = [sys.executable, "-m", "streamlit", "run", str(app_path)]
    cmd.extend(sys.argv[1:])

    subprocess.run(cmd, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
