import sys
from pathlib import Path

import pytest

BUNDLE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BUNDLE_DIR / "src"))
sys.path.insert(0, str(BUNDLE_DIR / "scripts"))


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    import local_run

    session = local_run.spark_session(tmp_path_factory.mktemp("spark"))
    yield session
