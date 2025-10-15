import os
from pathlib import Path
from typing import List, Tuple

from src.config import INPUT_DIR, OUTPUT_DIR, DATA_DIR, SCHEMA_FILE
from src.parser import YmlParserRozetka
from schemas import data_schema
from src.exporters import XmlExporterIntimo, XmlExporterKasta
from src.logger_config import app_logger
from validators.xsd_validator import XsdValidator


def process_folder(folder_path: Path) -> List[Tuple[str, data_schema.XmlCatalog]]:
    if not folder_path.is_dir():
        app_logger.error(f"Error: Folder '{folder_path}' not found.")
        return []

    catalogs = []
    app_logger.info(f"--- Processing files in folder: {folder_path} ---")

    xml_files = list(folder_path.glob("*.xml"))

    if not xml_files:
        app_logger.warning("No XML files found.")
        return []

    for xml_file in xml_files:
        app_logger.info(f"-> Processing file: {xml_file.name}")
        try:
            parser = YmlParserRozetka(file_path=str(xml_file))
            catalog = parser.parse()
            if catalog:
                catalogs.append((xml_file.name.split(".")[0], catalog))
                app_logger.info(
                    f"   [✓] Success! Found {len(catalog)} products in '{catalog.name}'."
                )
        except (ValueError, IOError) as e:
            app_logger.error(f"   [✗] Error processing file: {e}")
            continue

    return catalogs


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(INPUT_DIR, exist_ok=True)
    all_parsed_catalogs = process_folder(INPUT_DIR)
    app_logger.info("\n--- Finished processing all files. ---")

    if not all_parsed_catalogs:
        app_logger.warning("No catalogs were parsed.")
        return

    validator = XsdValidator(schema_path=SCHEMA_FILE)
    if not validator.xmlschema:
        app_logger.error(
            "Could not perform validation because the schema failed to load."
        )
    for catalog_path, catalog in all_parsed_catalogs:
        try:
            exporterIntimo = XmlExporterIntimo(catalog=catalog)
            output_file_path = OUTPUT_DIR / f"{catalog_path}_intimo.xml"
            exporterIntimo.export(str(output_file_path))
            is_valid = validator.validate(xml_path=Path(output_file_path))

            exporterKasta = XmlExporterKasta(catalog=catalog)
            output_file_path = OUTPUT_DIR / f"{catalog_path}_kasta.xml"
            exporterKasta.export(str(output_file_path))
            if is_valid:
                app_logger.info("Validation check completed successfully.")
            else:
                app_logger.warning(
                    "Validation check found errors. Please see details above in the log."
                )
        except Exception as e:
            app_logger.error(f"Unexpected error during export: {e}")


if __name__ == "__main__":
    main()
