from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
INTIMO_VALIDATION_SCHEMA_FILE = BASE_DIR / "schemas" / "validate_intimo.xsd"
KASTA_VALIDATION_SCHEMA_FILE = BASE_DIR / "schemas" / "validate_kasta.xsd"
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
