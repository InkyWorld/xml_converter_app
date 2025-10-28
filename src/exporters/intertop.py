import asyncio
import concurrent.futures
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
import csv
import os
import pickle
from typing import Dict

import httpx
from tqdm.asyncio import tqdm

from src.config import (
    BASE_LINK_INTERTOP,
    DATA_DIR,
    INTERTOP_APLICATION_KEY,
    INTERTOP_APLICATION_SECRET,
)
from src.logger_config import app_logger
from src.senders.intertop import (
    auth,
    get_product_articles,
    archive_offer_and_quantity_zero,
    get_offers_data_by_articles,
    get_product_offers,
    change_product_activity,
    get_product_offers_async,
    get_products,
    update_offer,
    update_offers_prices,
    change_product_status,
    updating_product_price,
    update_offers_quantity,
    run_all_offer_updates,
    async_load_wrapper,
)
from ..schemas import data_schema


class ExporterIntertop:
    """
    Клас, відповідальний за експорт об'єкта XmlCatalog
    у формат Intertop.
    """

    def __init__(self, catalog: data_schema.XmlCatalog):
        """Ініціалізує експортер."""
        self.catalog = catalog
        self.article_uniq_groups: Dict[str, data_schema.Offer] = {}
        self.size_mapping = self.load_size_mapping(
            str(DATA_DIR / "sizes_id_intertop.csv")
        )
        self.bearer = auth(
            BASE_LINK_INTERTOP + "auth",
            INTERTOP_APLICATION_KEY,
            INTERTOP_APLICATION_SECRET,
        )[0]

        self.products = get_products(self.bearer)

        self.article_sizeID_mapping = self.load_article_sizeID_mapping(
            self.products, self.bearer
        )

    def load_article_sizeID_mapping(self, products, bearer) -> dict:
        """
        Ця функція залишається СИНХРОННОЮ, як ви і хотіли.
        Вона запускає асинхронний код всередині себе.
        """
        return asyncio.run(async_load_wrapper(products, bearer))

    @staticmethod
    def load_size_mapping(csv_path: str) -> dict:
        """Завантажує таблицю відповідностей у словник."""
        mapping = {}
        with open(csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                mapping[row["rozetka_value"].strip()] = (
                    int(row["size_id"]),
                    row["intertop_value"].strip(),
                )
        return mapping

    def size_intertop_mapping(self, rozetka_value: str):
        """Повертає (size_id, intertop_value) для rozetka_value."""
        return self.size_mapping.get(rozetka_value.strip(), (None, None))

    def _prepare_data_maps(self):
        """
        Collect and prepare unique entities and their relationships:
        - Group offers by article number.
        """
        # prices for articles
        for offer in self.catalog.offers:
            if offer.article and offer.article not in self.article_uniq_groups.keys():
                self.article_uniq_groups[offer.article] = offer

    def update_product_price(self):
        self._prepare_data_maps()

        articles = get_product_articles(self.bearer)
        with open("file.txt", "a") as f:
            for article in articles:
                if article in self.article_uniq_groups.keys():
                    updating_product_price(
                        article,
                        self.bearer,
                        self.article_uniq_groups[article].price,
                        self.article_uniq_groups[article].discount_price,
                        True,
                    )
                else:
                    f.write(article + "\n")
                    app_logger.warning(f"артикля {article} немає в розетці")

    def update_intertop(self):
        if not self.article_uniq_groups:
            self._prepare_data_maps()

        all_products = dict(
            (product.get("vendor_code"), product.get("article"))
            for product in self.products
        )

        vendorCodes_articles_not_uploaded = dict(
            (product.get("vendor_code"), product.get("article"))
            for product in self.products
            if product.get("status", {}).get("code")
            not in ("uploaded", "moderate", "approved", "not_approved")
        )
        vendorCodes_not_uploaded = set(vendorCodes_articles_not_uploaded.keys())
        vendorCodes_draft = list(
            product.get("vendor_code")
            for product in self.products
            if product.get("status", {}).get("code") == "draft"
        )
        vendorCodes_not_approved = set(
            product.get("vendor_code")
            for product in self.products
            if product.get("status", {}).get("code") == "not_approved"
            and product.get("vendor_code") in self.article_uniq_groups.keys()
        )
        for item in vendorCodes_not_approved:
            app_logger.warning(
                f"Артикль не затверджений, виконайте вимогу модератора {item}"
            )

        vendorCodes_articles_only_on_intertop = dict(
            (product.get("vendor_code"), product.get("article"))
            for product in self.products
            if (article := product.get("vendor_code"))
            not in self.article_uniq_groups.keys()
        )
        vendorCodes_only_on_intertop = set(
            vendorCodes_articles_only_on_intertop.keys()
        )

        articles_only_on_rozetka = {
            article
            for offer in self.catalog.offers
            if (article := offer.article)
            not in {product.get("vendor_code") for product in self.products}
        }

        articles_to_archive = set(
            vendorCodes_only_on_intertop
            - vendorCodes_not_uploaded
        )
        for item in articles_to_archive:
            vendorCodes_draft.append(item)
            change_product_status(all_products.get(item), self.bearer, "draft")

        articles_moderate = tuple(
            product.get("article")
            for product in self.products
            if product.get("status", {}).get("code") == "moderate"
        )

        articles_to_moderate = set()

        for article in articles_moderate:
            articles_to_moderate.add(article)
            change_product_status(article, self.bearer, "draft")

        used_article_sizeID_mapping = set()
        update_offer_task_args = []

        for offer in self.catalog.offers:
            if offer.article in articles_only_on_rozetka:
                app_logger.warning(
                    f"vendor code that is not on intertop {offer.article}"
                )
                continue
            if offer.article in vendorCodes_not_uploaded:
                app_logger.warning(
                    f"vendor code that status is not uploaded {offer.article}"
                )
                if offer.article not in articles_to_moderate:
                    articles_to_moderate.add(all_products.get(offer.article))
                if offer.article not in vendorCodes_draft:
                    vendorCodes_draft.append(offer.article)
                    change_product_status(
                        all_products.get(offer.article), self.bearer, "draft"
                    )
            for param in offer.params:
                if param.name in ["розмір", "size", "зріст"]:
                    rozetka_size_value = param.value
                    intertop_size_id, intertop_size_value = self.size_intertop_mapping(
                        rozetka_size_value
                    )
                    if not intertop_size_id:
                        app_logger.warning(
                            f"Немає мапінга розміру для {rozetka_size_value=}"
                        )
                        break

                    data = self.article_sizeID_mapping.get(
                        (all_products.get(offer.article), intertop_size_id),
                        (None, None, None, None, None, None),
                    )
                    (
                        barcode,
                        base_price_amount,
                        discount_price_amount,
                        product_status,
                        offer_activity,
                        quantity,
                    ) = data
                    if barcode:
                        used_article_sizeID_mapping.add(
                            (all_products.get(offer.article), intertop_size_id)
                        )
                        if not (
                            offer.price == base_price_amount
                            and offer.discount_price == discount_price_amount
                            and offer.stock_quantity == quantity
                            and offer_activity
                        ):
                            update_offer_task_args.append(
                                (
                                    self.bearer,
                                    all_products.get(offer.article),
                                    barcode,
                                    offer.price,
                                    offer.discount_price,
                                    offer.stock_quantity,
                                    True,
                                )
                            )
                            break
                    else:
                        app_logger.warning(
                            f"немає офера для сайз ід {offer.article=} {rozetka_size_value=} {intertop_size_id=}"
                        )

                        # створення офера для сайз ид
                        break
        if update_offer_task_args:
            app_logger.info(f"Found {len(update_offer_task_args)} offers to update.")
            try:
                asyncio.run(
                    run_all_offer_updates(
                        update_offer_task_args, "Updating offers prices and quantities"
                    )
                )
            except RuntimeError as e:
                app_logger.critical(
                    f"Asyncio error: {e}. Event loop might be already running!"
                )
        self.products = get_products(self.bearer)
        vendorCodes_articles_not_uploaded = set(
            product.get("article")
            for product in self.products
            if product.get("status", {}).get("code")
            not in ("uploaded", "moderate", "approved", "not_approved")
        )
        existing_keys = set(self.article_sizeID_mapping.keys())
        not_updated_offers_keys = [
            item
            for item in existing_keys.difference(used_article_sizeID_mapping)
            if item[0] not in vendorCodes_articles_not_uploaded
        ]
        inactive_barcodes = [
            item[0]
            for item in self.article_sizeID_mapping.values()
            if item[4] is False and item[3] != "archived"
        ]
        deactivate_offer_task_args = []
        for offer_key in not_updated_offers_keys:
            # set activity, quantity = False, 0
            (
                barcode,
                base_price_amount,
                discount_price_amount,
                product_status,
                offer_activity,
                quantity,
            ) = self.article_sizeID_mapping.get((offer_key[0], offer_key[1]))
            if barcode not in inactive_barcodes:
                deactivate_offer_task_args.append(
                    (
                        self.bearer,
                        offer_key[0],
                        barcode,
                        base_price_amount,
                        discount_price_amount,
                        0,
                        False,
                    )
                )
        if deactivate_offer_task_args:
            app_logger.info(
                f"Found {len(deactivate_offer_task_args)} offers to deactivate."
            )
            try:
                asyncio.run(
                    run_all_offer_updates(
                        deactivate_offer_task_args, "Deactivating offers"
                    )
                )
            except RuntimeError as e:
                app_logger.critical(f"Asyncio error: {e}.")

        for article in articles_to_moderate:
            change_product_status(article, self.bearer, "moderate")
        app_logger.info("Intertop update completed.")
