import logging

from .config import BASE_DIR

app_logger = logging.getLogger("xml_converter_app")
app_logger.setLevel(logging.DEBUG)
if not BASE_DIR:
    raise ValueError("BASE_DIR is not set in config.py")
log_dir = BASE_DIR.joinpath("logs")
log_dir.mkdir(exist_ok=True)
log_file_path = log_dir / "logfile.log"

file_handler = logging.FileHandler(log_file_path, mode="a", encoding="utf-8")
file_handler.setLevel(logging.INFO)

file_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)-8s] %(name)-15s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
file_handler.setFormatter(file_formatter)

if not app_logger.hasHandlers():
    app_logger.addHandler(file_handler)

logging.getLogger("httpx").setLevel(logging.WARNING)