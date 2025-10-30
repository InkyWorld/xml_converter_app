from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR: Path | None = Path(__file__).resolve().parent.parent
INTIMO_VALIDATION_SCHEMA_FILE: Path = BASE_DIR / "src" / "schemas" / "validate_intimo.xsd"
KASTA_VALIDATION_SCHEMA_FILE: Path = BASE_DIR / "src" / "schemas" / "validate_kasta.xsd"
DATA_DIR: Path = BASE_DIR / "data"
INPUT_DIR: Path = BASE_DIR / "input"
OUTPUT_DIR: Path = BASE_DIR / "output"

INTERTOP_APLICATION_KEY: str | None = os.getenv("INTERTOP_APLICATION_KEY")
INTERTOP_APLICATION_SECRET: str | None = os.getenv("INTERTOP_APLICATION_SECRET")
BASE_LINK_INTERTOP: str | None = os.getenv("BASE_LINK_INTERTOP")