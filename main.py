import os
from pathlib import Path
from typing import List, Tuple

from src.config import INPUT_DIR, OUTPUT_DIR, INTIMO_VALIDATION_SCHEMA_FILE, KASTA_VALIDATION_SCHEMA_FILE
from src.parser import YmlParserRozetka
from src.schemas import data_schema
from src.exporters import XmlExporterIntimo, XmlExporterKasta
from src.logger_config import app_logger
from validators.xsd_validator import XmlSchemaValidator


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

def transform(input_file: str, output_file: str) -> bool:
    try:
        parser = YmlParserRozetka(file_path=input_file)
        catalog = parser.parse()
        if not catalog:
            app_logger.error("Parsing returned no catalog.")
            return False

        exporterIntimo = XmlExporterIntimo(catalog=catalog)
        exporterIntimo.export(output_file)
        app_logger.info(f"Exported Intimo XML to {output_file}")
        return True
    except Exception as e:
        app_logger.error(f"Error during transformation: {e}")
        return False

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(INPUT_DIR, exist_ok=True)
    all_parsed_catalogs = process_folder(INPUT_DIR)
    app_logger.info("--- Finished processing all files. ---")

    if not all_parsed_catalogs:
        app_logger.warning("No catalogs were parsed.")
        return

    validator_intimo = XmlSchemaValidator(schema_path=INTIMO_VALIDATION_SCHEMA_FILE)
    if not validator_intimo.schema:
        app_logger.error(
            "Could not perform intimo validation because the schema failed to load."
        )
    validator_kasta = XmlSchemaValidator(schema_path=KASTA_VALIDATION_SCHEMA_FILE)
    if not validator_kasta.schema:
        app_logger.error(
            "Could not perform kasta validation because the schema failed to load."
        )
    for catalog_path, catalog in all_parsed_catalogs:
        try:
            # exporterIntimo = XmlExporterIntimo(catalog=catalog)
            # output_file_path = OUTPUT_DIR / f"{catalog_path}_intimo.xml"
            # exporterIntimo.export(str(output_file_path))
            # validator_intimo.validate(xml_path=Path(output_file_path))

            # exporterKasta = XmlExporterKasta(catalog=catalog)
            # output_file_path = OUTPUT_DIR / f"{catalog_path}_kasta.xml"
            # exporterKasta.export(str(output_file_path))
            # validator_kasta.validate(xml_path=Path(output_file_path))

            exporterIntimo = XmlExporterIntimo(catalog=catalog)
            output_file_path = OUTPUT_DIR / f"{catalog_path}_intimo.xml"
            exporterIntimo.export(str(output_file_path))
            validator_intimo.validate(xml_path=Path(output_file_path))

        except Exception as e:
            app_logger.error(f"Unexpected error during export: {e}")


if __name__ == "__main__":
    main()
