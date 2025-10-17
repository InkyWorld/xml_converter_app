import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

from lxml import etree as ET  # type: ignore

from src.logger_config import app_logger
from ..schemas import data_schema
from .base import BaseExporter


class XmlExporterIntertop(BaseExporter):
    """
    Клас, відповідальний за експорт об'єкта XmlCatalog
    у формат JSON, сумісний з API Intertop.
    """

    def __init__(self, catalog: data_schema.XmlCatalog):
        """Ініціалізує експортер."""
        super().__init__(catalog)
        # Словник для групування варіантів товару за артикулом
        self.article_groups: Dict[str, List[data_schema.Offer]] = defaultdict(list)

    def _prepare_data(self):
        """
        Готує дані до експорту, групуючи всі офери (варіанти)
        за їхнім артикулом.
        """
        app_logger.info("Grouping offers by article...")
        for offer in self.catalog.offers:
            # Використовуємо артикул як ключ для групування
            if offer.article:
                self.article_groups[offer.article].append(offer)
            else:
                # Якщо артикул відсутній, логуємо попередження
                app_logger.warning(
                    f"Offer with ID={offer.id} has no article and will be skipped."
                )
        app_logger.info(
            f"Grouped {len(self.catalog.offers)} offers into {len(self.article_groups)} products."
        )

    def export(self, output_path: str):
        """
        Основний метод експорту. Перетворює дані каталогу в JSON
        і зберігає його у файл.

        :param output_path: Шлях для збереження вихідного JSON-файлу.
        """
        app_logger.info("Starting Intertop JSON export...")
        self._prepare_data()

        all_products_data = []

        # Обробляємо кожну групу товарів (об'єднаних за артикулом)
        for article, offer_group in self.article_groups.items():
            if not offer_group:
                continue

            # Беремо перший офер як основний для отримання загальної інформації
            main_offer = offer_group[0]

            # Формуємо основний об'єкт продукту
            product_data = {
                "product": {
                    "category_id": main_offer.category_id,
                    "brand": main_offer.vendor,
                    "name": main_offer.name_ua or main_offer.name,
                    "description": main_offer.description_ua or main_offer.description,
                    "attributes": self._extract_common_attributes(main_offer),
                    "photos": self._extract_photos(offer_group),
                },
                "offers": self._extract_offers(offer_group),
            }
            all_products_data.append(product_data)

        # Записуємо згенеровані дані у файл
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(all_products_data, f, ensure_ascii=False, indent=4)
            app_logger.info(f"Successfully exported Intertop JSON to {output_path}")
        except IOError as e:
            app_logger.error(f"Error writing to file {output_path}: {e}")

    def _extract_common_attributes(
        self, offer: data_schema.Offer
    ) -> List[Dict[str, str]]:
        """
        Витягує загальні атрибути з офера (наприклад, склад).
        """
        attributes = []
        for param in offer.params:
            param_name_lower = param.name.lower()
            # Додаємо атрибути, які є спільними для всіх варіацій
            if param_name_lower in ["склад тканини", "матеріал", "materials"]:
                attributes.append({"name": "Склад", "value": str(param.value)})
        return attributes

    def _extract_photos(
        self, offer_group: List[data_schema.Offer]
    ) -> List[Dict[str, Any]]:
        """
        Збирає унікальні фотографії з усіх варіантів товару.
        """
        unique_pictures = set()
        for offer in offer_group:
            unique_pictures.update(offer.pictures)

        # Формуємо список фото з позначкою "main" для першого фото
        return [
            {"url": pic_url, "main": i == 0}
            for i, pic_url in enumerate(list(unique_pictures))
        ]

    def _extract_offers(
        self, offer_group: List[data_schema.Offer]
    ) -> List[Dict[str, Any]]:
        """
        Створює список варіантів (оферів) для одного продукту.
        """
        offers_data = []
        for offer in offer_group:
            size = "one-size"  # Значення за замовчуванням
            color = None

            offer_attributes = []
            for param in offer.params:
                param_name_lower = param.name.lower()
                if param_name_lower in ["розмір", "size", "зріст"]:
                    size = str(param.value)
                elif param_name_lower in ["колір", "color", "цвет"]:
                    color = str(param.value)

            # Додаємо атрибути розміру та кольору до конкретного офера
            offer_attributes.append({"name": "Розмір", "value": size})
            if color:
                offer_attributes.append({"name": "Колір", "value": color})

            offers_data.append(
                {
                    "sku": offer.id,
                    "price": offer.price,
                    "stock": offer.stock_quantity if offer.is_in_stock() else 0,
                    "attributes": offer_attributes,
                }
            )
        return offers_data
