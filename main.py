import os
from pathlib import Path
from typing import List

from src.config import OUTPUT_DIR, DATA_DIR, SCHEMA_FILE
from src.parser import YmlParser
from schemas import data_schema
from src.exporters import XmlExporter
from src.logger_config import app_logger


def process_folder(folder_path: Path) -> List[data_schema.XmlCatalog]:
    if not folder_path.is_dir():
        app_logger.error(f"Error: Folder '{folder_path}' not found.")
        return []

    catalogs = []
    app_logger.info(f"--- Processing files in folder: {folder_path} ---")

    xml_files = list(folder_path.glob('*.xml'))
    
    if not xml_files:
        app_logger.warning("No XML files found.")
        return []

    for xml_file in xml_files:
        app_logger.info(f"-> Processing file: {xml_file.name}")
        try:
            parser = YmlParser(file_path=str(xml_file))
            catalog = parser.parse()
            if catalog:
                catalogs.append(catalog)
                app_logger.info(f"   [✓] Success! Found {len(catalog)} products in '{catalog.name}'.")
        except (ValueError, IOError) as e:
            app_logger.error(f"   [✗] Error processing file: {e}")
            continue
            
    return catalogs

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    all_parsed_catalogs = process_folder(DATA_DIR)
    app_logger.info("\n--- Finished processing all files. ---")

    if not all_parsed_catalogs:
        app_logger.warning("No catalogs were parsed.")
        return

    app_logger.info(f"\nSuccessfully processed catalogs: {len(all_parsed_catalogs)}")
    total_offers = sum(len(catalog) for catalog in all_parsed_catalogs)
    app_logger.info(f"Total number of products from all files: {total_offers}")

    app_logger.info("\nLoaded store names:")
    for catalog in all_parsed_catalogs:
        app_logger.info(f"- {catalog.name} (from {catalog.catalog_date})")

    first_catalog = all_parsed_catalogs[0]
    app_logger.info(f"\nStarting export for catalog: '{first_catalog.name}'")

    try:
        exporter = XmlExporter(catalog=first_catalog)
        exporter.export(str(OUTPUT_DIR / "exported_catalog.xml"))
    except Exception as e:
        app_logger.error(f"Unexpected error during export: {e}")

if __name__ == "__main__":
    main()
