from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
INTIMO_VALIDATION_SCHEMA_FILE = BASE_DIR / "src" / "schemas" / "validate_intimo.xsd"
KASTA_VALIDATION_SCHEMA_FILE = BASE_DIR / "src" / "schemas" / "validate_kasta.xsd"
DATA_DIR = BASE_DIR / "data"
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INTERTOP_APLICATION_KEY = os.getenv("INTERTOP_APLICATION_KEY")
INTERTOP_APLICATION_SECRET = os.getenv("INTERTOP_APLICATION_SECRET")
BASE_LINK_INTERTOP = os.getenv("BASE_LINK_INTERTOP")