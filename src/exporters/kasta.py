import csv
from pathlib import Path
from typing import Dict

from lxml import etree as ET # type: ignore

from ..schemas import data_schema
from src.logger_config import app_logger
from src.config import DATA_DIR
from .base import BaseExporter

class XmlExporterKasta(BaseExporter):
    """
    Class responsible for exporting an XmlCatalog object
    into a YML XML format compatible with Kasta.
    """

    def __init__(self, catalog: data_schema.XmlCatalog):
        """Ініціалізує експортер та завантажує довідник категорій з файлу."""
        super().__init__(catalog)
        self.rozetka_id_map = self._load_rozetka_id_map()

    def _load_rozetka_id_map(self) -> Dict[str, str]:
        """
        Завантажує та розбирає CSV-файл з мапінгом категорій.
        Шукає файл у папці 'data' в корені проєкту.
        """
        mapping = {}
        # Шлях до файлу відносно кореня проєкту
        file_path = DATA_DIR / "name_rzid_kasta.csv"

        try:
            with open(file_path, mode="r", encoding="utf-8") as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    if row.get("name_ru") and row.get("rz_id"):
                        mapping[row["name_ru"].lower()] = (
                            row["rz_id"],
                            row.get("name_ua"),
                        )
            app_logger.info(
                f"Successfully loaded {len(mapping)} category mappings from {file_path}"
            )
        except FileNotFoundError:
            app_logger.error(
                f"Category mapping file not found at {file_path}. `rz_id` will not be added."
            )
        except Exception as e:
            app_logger.error(
                f"An error occurred while reading the category mapping file: {e}"
            )

        return mapping

    def export(self, output_path: str):
        """
        Export the catalog data to a Kasta-specific XML file.
        Each offer from the input is treated as a separate product variation
        and grouped by its article number.

        :param output_path: The path where the XML file will be saved.
        """
        app_logger.info("Starting Kasta XML export...")

        root = ET.Element("yml_catalog", date=self.catalog.catalog_date)
        shop = self._create_sub_element(root, "shop")

        # --- Shop Details ---
        self._create_sub_element(shop, "name", self.catalog.name)

        # --- Currencies ---
        currencies_node = self._create_sub_element(shop, "currencies")  #
        for currency_id, rate in self.catalog.currencies.items():
            ET.SubElement(currencies_node, "currency", id=currency_id, rate=str(rate))

        # --- Categories ---
        categories_node = self._create_sub_element(shop, "categories")
        for cat_id, cat_name in self.catalog.categories.items():
            attributes = {"id": str(cat_id)}
            if cat_name.lower() in self.rozetka_id_map.keys():
                attributes["rz_id"] = self.rozetka_id_map[cat_name.lower()][0]

            cat_element = ET.SubElement(categories_node, "category", **attributes)
            cat_element.text = cat_name

        # --- Offers ---
        offers_node = self._create_sub_element(shop, "offers")
        for offer in self.catalog.offers:
            offer_node = ET.SubElement(offers_node, "offer")

            if offer.name_ua:
                self._create_sub_element(offer_node, "name_ua", offer.name_ua)
            if offer.name:
                self._create_sub_element(offer_node, "name", offer.name)

            self._create_sub_element(offer_node, "currencyId", offer.currency_id)
            self._create_sub_element(offer_node, "categoryId", str(offer.category_id))
            stock_quantity = offer.stock_quantity if offer.is_in_stock() else 0
            self._create_sub_element(offer_node, "stock_quantity", str(stock_quantity))
            self._create_sub_element(offer_node, "price", str(offer.price))

            for picture in offer.pictures:
                self._create_sub_element(offer_node, "picture", picture)

            self._create_sub_element(offer_node, "vendor", offer.vendor)
            self._create_sub_element(offer_node, "vendorcode", offer.article)

            if offer.description_ua:
                self._create_sub_element(
                    offer_node, "description_ua", offer.description_ua, cdata=True
                )
            if offer.description:
                self._create_sub_element(
                    offer_node, "description", offer.description, cdata=True
                )

            for param in offer.params:
                req = 2
                if param.name.lower() in ["color", "колір", "цвет"]:
                    param_element = ET.SubElement(offer_node, "param", name="Колір")
                    param_element.text = str(param.value)
                    req -= 1
                elif param.name.lower() in ["size", "розмір", "зріст"]:
                    param_element = ET.SubElement(offer_node, "param", name="Розмір")
                    req -= 1
                else:
                    param_element = ET.SubElement(offer_node, "param", name=param.name)
                param_element.text = str(param.value)
                if req == 1:
                    print(offer)

        # --- Writing to file ---
        tree = ET.ElementTree(root)
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            tree.write(
                output_path, pretty_print=True, xml_declaration=True, encoding="UTF-8"
            )
            app_logger.info(f"Successfully exported Kasta XML to {output_path}")
        except IOError as e:
            app_logger.error(f"Error writing to file {output_path}: {e}")
