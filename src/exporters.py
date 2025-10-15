from abc import ABC, abstractmethod
from collections import defaultdict
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from lxml import etree as ET

from schemas import data_schema
from src.logger_config import app_logger
from src.config import DATA_DIR

class BaseExporter(ABC):
    """
    Інтерфейс для всіх класів-експортерів.
    Визначає загальний контракт: будь-який експортер повинен вміти
    прийняти каталог даних і виконати експорт за вказаним шляхом.
    """

    def __init__(self, catalog: data_schema.XmlCatalog):
        """Ініціалізує експортер, приймаючи дані каталогу."""
        self.catalog = catalog
        super().__init__()

    @abstractmethod
    def export(self, output_path: str):
        """
        Основний метод, який запускає процес експорту даних у файл.
        """
        pass

    @staticmethod
    def _create_sub_element(
        parent: ET._Element, tag_name: str, text: str | None = None, cdata: bool = False
    ):
        """
        Helper function to create an XML sub-element with optional CDATA content.

        :param parent: Parent XML element.
        :param tag_name: Name of the new sub-element.
        :param text: Optional text content for the element.
        :param cdata: Whether to wrap text in a CDATA block.
        :return: Created XML sub-element.
        """
        element = ET.SubElement(parent, tag_name)
        if text is not None:
            if cdata and text:
                element.text = ET.CDATA(str(text))
            else:
                element.text = str(text)
        return element


class XmlExporterIntimo(BaseExporter):
    """
    Class responsible for exporting an XmlExporterIntimo object
    into a new, more structured and strict XML format.
    """

    def __init__(self, catalog: data_schema.XmlCatalog):
        """
        Initialize the exporter and set up internal mappings.

        :param catalog: Parsed XmlCatalog object containing products, categories, and metadata.
        """
        super().__init__(catalog)
        self.brand_map: Dict[str, int] = {}
        self.color_map: Dict[str, str] = {}
        self.category_to_line_id_map: Dict[str, str] = {}
        self.collection_to_brand_map: Dict[int, int] = {}
        self.article_groups: Dict[str, List[data_schema.Offer]] = defaultdict(list)

    def _prepare_data_maps(self):
        """
        Collect and prepare unique entities and their relationships:
        - Group offers by article number.
        - Create brand mappings.
        - Map categories to brands.
        - Build unique color mappings.
        """
        brand_id_counter = 1

        # Group offers by article number
        for offer in self.catalog.offers:
            if offer.article:
                self.article_groups[offer.article].append(offer)

        # 1. Collect unique brands
        for offer in self.catalog.offers:
            if offer.vendor and offer.vendor not in self.brand_map:
                self.brand_map[offer.vendor] = brand_id_counter
                brand_id_counter += 1

        # 2. Map categories (collections) to brands using offers
        for offer in self.catalog.offers:
            if offer.category_id not in self.collection_to_brand_map and offer.vendor:
                brand_id = self.brand_map.get(offer.vendor)
                if brand_id:
                    self.collection_to_brand_map[offer.category_id] = brand_id

        # 3. Collect all unique colors
        for offer in self.catalog.offers:
            for param in offer.params:
                if param.name.lower() in ["колір", "color"] and param.value:
                    color_name = param.value.strip().lower()

                    if color_name not in self.color_map:
                        key = str(len(self.color_map) + 1)
                        self.color_map[color_name] = key

    def export(self, output_path: str):
        """
        Main method to export the XmlCatalog into a new XML file.

        :param output_path: Path where the resulting XML file will be saved.
        """
        app_logger.info("Preparing data for export...")
        self._prepare_data_maps()

        app_logger.info("Building XML structure...")
        root = ET.Element("shop")
        self._build_catalog_section(root)
        self._build_store_section(root)
        self._build_brands_section(root)
        self._build_collections_section(root)
        self._build_lines_section(root)
        self._build_colors_section(root)
        self._build_items_section(root)

        tree = ET.ElementTree(root)
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        tree.write(
            output_path, pretty_print=True, xml_declaration=True, encoding="UTF-8"
        )
        app_logger.info(f"Export completed successfully. File saved to: {output_path}")

    def _build_catalog_section(self, parent: ET._Element):
        """
        Build the <catalog> section containing metadata such as the catalog date.
        """
        catalog_node = self._create_sub_element(parent, "catalog")
        date_str = self.catalog.catalog_date or datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        if len(date_str) == 16:
            date_str += ":00"
        self._create_sub_element(catalog_node, "catalog_date", date_str)

    def _build_store_section(self, parent: ET._Element):
        """
        Build the <store> section containing store metadata.
        """
        store_node = self._create_sub_element(parent, "store")
        self._create_sub_element(store_node, "title", self.catalog.name)
        self._create_sub_element(store_node, "description", self.catalog.company)
        self._create_sub_element(store_node, "link", self.catalog.url)

    def _build_brands_section(self, parent: ET._Element):
        """
        Build the <brands> section from the brand_map.
        """
        brands_node = self._create_sub_element(parent, "brands")
        for name, brand_id in self.brand_map.items():
            brand_node = self._create_sub_element(brands_node, "brand")
            self._create_sub_element(brand_node, "id", str(brand_id))
            self._create_sub_element(brand_node, "title", name)

    # src/exporter.py (фрагмент)

    def _build_collections_section(self, parent: ET._Element):
        """
        Builds the <collections> section based on the catalog's categories.
        Ensures that brand_id is present if any brands exist.
        """
        collections_node = self._create_sub_element(parent, "collections")
        
        fallback_brand_id = None
        if self.brand_map: # Якщо у нас є хоча б один бренд
            fallback_brand_id = next(iter(self.brand_map.values()), None)

        for category_id_str, category_name in self.catalog.categories.items():
            category_id = int(category_id_str)
            collection_node = self._create_sub_element(collections_node, "collection")
            self._create_sub_element(collection_node, "id", str(category_id))
            
            # Намагаємося знайти пов'язаний brand_id
            brand_id = self.collection_to_brand_map.get(category_id)
            
            # Якщо brand_id не знайдено, але бренди існують, використовуємо запасний
            if not brand_id and fallback_brand_id:
                brand_id = fallback_brand_id
            
            # ВИПРАВЛЕНО: Додаємо тег <brand_id>, якщо він був знайдений
            if brand_id:
                self._create_sub_element(collection_node, "brand_id", str(brand_id))
            
            self._create_sub_element(collection_node, "title", category_name)

    def _build_lines_section(self, parent: ET._Element):
        """
        Build the <lines> section representing product lines (linked to categories).
        """
        lines_node = self._create_sub_element(parent, "lines")
        for i, (category_id_str, category_name) in enumerate(
            self.catalog.categories.items()
        ):
            category_id = int(category_id_str)
            collection_node = self._create_sub_element(lines_node, "line")
            self.category_to_line_id_map[str(category_id)] = str(i + 1)
            self._create_sub_element(collection_node, "id", str(i + 1))
            self._create_sub_element(collection_node, "collection_id", str(category_id))
            self._create_sub_element(collection_node, "title", category_name)

    def _build_colors_section(self, parent: ET._Element):
        """
        Build the <colors> section containing all unique colors.
        """
        colors_node = self._create_sub_element(parent, "colors")
        for name, id in self.color_map.items():
            color_node = self._create_sub_element(colors_node, "color")
            self._create_sub_element(color_node, "id", id)
            self._create_sub_element(color_node, "title", name)
            # TODO: Add color image link if available
            # self._create_sub_element(color_node, "color_image_link", "https://placeholder.com/color.jpg")

    def _build_items_section(
        self, parent: ET._Element, price_r_rate=1.0, price_w_rate=1.0
    ):
        """
        Build the <items> section, grouping offers by article number.
        Each article group becomes a single <item> with multiple variations.

        :param parent: Parent <shop> XML element.
        :param price_r_rate: Multiplier for retail price calculation.
        :param price_w_rate: Multiplier for wholesale price calculation.
        """
        items_node = self._create_sub_element(parent, "items")

        if not hasattr(self, "article_groups") or not self.article_groups:
            app_logger.error(
                "Error: Offers were not grouped by article. Run _prepare_data_maps() first."
            )
            return

        for article, offer_group in self.article_groups.items():
            main_offer = offer_group[0]
            item_node = self._create_sub_element(items_node, "item")

            # Extract materials from offer parameters
            materials = "Composition not specified"
            for param in main_offer.params:
                if param.name.lower() in ["склад тканини"]:
                    materials = str(param.value)
                    break

            # Map category to corresponding line
            line_id = self.category_to_line_id_map.get(main_offer.category_id, "1")

            self._create_sub_element(
                item_node, "item_id", str(main_offer.id).split("-")[0]
            )
            self._create_sub_element(item_node, "art", article)
            self._create_sub_element(item_node, "line_id", line_id)
            self._create_sub_element(item_node, "title", main_offer.name)
            self._create_sub_element(
                item_node, "description_ua", main_offer.description, cdata=True
            )
            self._create_sub_element(item_node, "materials", materials)
            self._create_sub_element(item_node, "link", main_offer.url)

            # Price calculations (converted to integers)
            base_price = main_offer.price
            self._create_sub_element(item_node, "price", str(int(base_price)))
            self._create_sub_element(
                item_node, "price_r", str(int(base_price * price_r_rate))
            )
            self._create_sub_element(
                item_node, "price_w", str(int(base_price * price_w_rate))
            )

            # Collect unique images from all offers in the group
            unique_pictures = set()
            for offer_in_group in offer_group:
                unique_pictures.update(offer_in_group.pictures)

            for i, pic_url in enumerate(list(unique_pictures)):
                img_node = self._create_sub_element(item_node, "image_link", pic_url)

                # Attempt to associate color with image
                color_slug_id_for_pic = None
                for offer_in_group in offer_group:
                    if pic_url in offer_in_group.pictures:
                        for param in offer_in_group.params:
                            if param.name.lower() in ["колір", "color"]:
                                color_slug_id_for_pic = self.color_map.get(
                                    str(param.value)
                                )
                                break
                    if color_slug_id_for_pic:
                        break

                if color_slug_id_for_pic:
                    img_node.set("color", color_slug_id_for_pic)

            # Build variations for this item
            self._build_variations_section(item_node, offer_group)

    def _build_variations_section(
        self, parent: ET._Element, offer_group: List[data_schema.Offer]
    ):
        """
        Build the <variations> section for a group of offers (same article).
        Each offer in the group represents a separate variation.

        :param parent: Parent <item> XML element.
        :param offer_group: List of Offer objects representing variations.
        """
        variations_node = self._create_sub_element(parent, "variations")

        for offer_variation in offer_group:
            color_slug_id = ""
            size = "one-size"

            for param in offer_variation.params:
                param_name_lower = param.name.lower()
                if param_name_lower in ["колір", "color"]:
                    color_slug_id = self.color_map.get(str(param.value))
                elif param_name_lower in ["зріст", "розмір", "size"]:
                    size = str(param.value)

            variation_node = self._create_sub_element(variations_node, "variation")

            # Generate unique variation ID based on offer ID, color, and size
            variation_id = f"{offer_variation.id.split('-')[0]}_{color_slug_id}_{size}"
            self._create_sub_element(variation_node, "variation_id", variation_id)

            self._create_sub_element(variation_node, "color", color_slug_id)
            self._create_sub_element(variation_node, "size", size)

            # Stock quantity: 0 if item is out of stock
            stock_quantity = (
                offer_variation.stock_quantity if offer_variation.is_in_stock() else 0
            )
            self._create_sub_element(
                variation_node, "stock_quantity", str(stock_quantity)
            )


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
        file_path = DATA_DIR / 'name_rzid.csv'
        
        try:
            with open(file_path, mode='r', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    if row.get('name_ru') and row.get('rz_id'):
                        mapping[row['name_ru'].lower()] = (row['rz_id'], row.get('name_ua'))
            app_logger.info(f"Successfully loaded {len(mapping)} category mappings from {file_path}")
        except FileNotFoundError:
            app_logger.error(f"Category mapping file not found at {file_path}. `rz_id` will not be added.")
        except Exception as e:
            app_logger.error(f"An error occurred while reading the category mapping file: {e}")
            
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
        # self._create_sub_element(shop, "company", self.catalog.company)
        # self._create_sub_element(shop, "url", self.catalog.url)

        # --- Currencies ---
        currencies_node = self._create_sub_element(shop, "currencies") #
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
                 self._create_sub_element(offer_node, "description_ua", offer.description_ua, cdata=True)
            if offer.description:
                self._create_sub_element(offer_node, "description", offer.description, cdata=True)

            

            for param in offer.params:
                if param.name.lower() in ["color", "колір"]:
                    param_element = ET.SubElement(offer_node, "param", name="Колір")
                    param_element.text = str(param.value)
                elif param.name.lower() in ["size", "розмір", "зріст"]:
                    param_element = ET.SubElement(offer_node, "param", name="Розмір")
                else:
                    param_element = ET.SubElement(offer_node, "param", name=param.name)
                param_element.text = str(param.value)

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