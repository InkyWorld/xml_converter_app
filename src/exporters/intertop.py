import asyncio
import csv
from typing import Dict, List, Set, Tuple

from tqdm import tqdm

from src.config import (
    BASE_LINK_INTERTOP,
    DATA_DIR,
    INTERTOP_APLICATION_KEY,
    INTERTOP_APLICATION_SECRET,
)
from src.logger_config import app_logger
from src.senders.intertop import (
    async_load_wrapper,
    auth,
    change_product_status,
    create_offer_for_product,
    get_product_articles,
    get_products,
    run_all_offer_updates,
    updating_product_price,
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
            str(BASE_LINK_INTERTOP) + "auth",
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

    def _categorize_intertop_products(self) -> dict:
        """
        Аналізує self.products та self.catalog.offers,
        повертаючи словник зі згрупованими артикулами.
        """
        app_logger.info("Categorizing products...")
        all_products_map = dict(
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
        vendorCodes_only_on_intertop = set(vendorCodes_articles_only_on_intertop.keys())

        articles_only_on_rozetka = {
            article
            for offer in self.catalog.offers
            if (article := offer.article) not in all_products_map.keys()
        }

        articles_moderate = tuple(
            product.get("article")
            for product in self.products
            if product.get("status", {}).get("code") == "moderate"
        )

        return {
            "all_products_map": all_products_map,
            "vendorCodes_not_uploaded": vendorCodes_not_uploaded,
            "vendorCodes_draft": vendorCodes_draft,
            "vendorCodes_only_on_intertop": vendorCodes_only_on_intertop,
            "articles_only_on_rozetka": articles_only_on_rozetka,
            "articles_moderate": articles_moderate,
        }

    def _handle_product_status_changes(
        self, categorized_products: dict
    ) -> Tuple[Set[str], List[str]]:
        """
        Переводить старі продукти та "moderate" у "draft"
        для підготовки до оновлення.
        """
        app_logger.info("Handling product status changes (archiving and moderation)...")
        # Розпакування
        all_products_map = categorized_products["all_products_map"]
        vendorCodes_only_on_intertop = categorized_products[
            "vendorCodes_only_on_intertop"
        ]
        vendorCodes_not_uploaded = categorized_products["vendorCodes_not_uploaded"]
        vendorCodes_draft = categorized_products["vendorCodes_draft"]
        articles_moderate = categorized_products["articles_moderate"]

        # 1. Архівування (переведення в draft)
        articles_to_archive = set(
            vendorCodes_only_on_intertop - vendorCodes_not_uploaded
        )
        for item in articles_to_archive:
            vendorCodes_draft.append(item)
            change_product_status(all_products_map.get(item), self.bearer, "draft")

        # 2. Підготовка продуктів на модерації
        articles_to_moderate_set = set()
        for article in articles_moderate:
            articles_to_moderate_set.add(article)
            change_product_status(article, self.bearer, "draft")

        return articles_to_moderate_set, vendorCodes_draft

    def _prepare_offer_updates(
        self,
        categorized_products: dict,
        articles_to_moderate: Set[str],
        vendorCodes_draft: List[str],
    ) -> Tuple[List[tuple], Set[tuple]]:
        """
        Головний цикл по self.catalog.offers.
        Готує списки завдань для оновлення оферів.
        Модифікує articles_to_moderate та vendorCodes_draft!
        """
        app_logger.info("Preparing offer updates (main loop)...")
        # Розпакування
        all_products_map = categorized_products["all_products_map"]
        articles_only_on_rozetka = categorized_products["articles_only_on_rozetka"]
        vendorCodes_not_uploaded = categorized_products["vendorCodes_not_uploaded"]

        used_article_sizeID_mapping = set()
        update_offer_task_args = []

        created_offers_count = 0
        for offer in tqdm(self.catalog.offers):
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
                    articles_to_moderate.add(all_products_map.get(offer.article))
                if offer.article not in vendorCodes_draft:
                    vendorCodes_draft.append(str(offer.article))
                    change_product_status(
                        all_products_map.get(offer.article), self.bearer, "draft"
                    )
            for param in offer.params:
                if param.name in ["розмір", "size", "зріст"]:
                    rozetka_size_value = param.value
                    intertop_size_id, intertop_size_value = self.size_intertop_mapping(
                        str(rozetka_size_value)
                    )
                    if not intertop_size_id:
                        app_logger.warning(
                            f"Немає мапінга розміру для {rozetka_size_value=}"
                        )
                        break

                    data = self.article_sizeID_mapping.get(
                        (all_products_map.get(offer.article), intertop_size_id),
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
                            (all_products_map.get(offer.article), intertop_size_id)
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
                                    all_products_map.get(offer.article),
                                    barcode,
                                    offer.price,
                                    offer.discount_price,
                                    offer.stock_quantity,
                                    True,
                                )
                            )
                            break
                        break
                    else:
                        created_offers_count += 1
                        articles_to_moderate.add(all_products_map.get(offer.article))
                        if offer.article not in vendorCodes_draft:
                            vendorCodes_draft.append(str(offer.article))
                            change_product_status(all_products_map.get(offer.article), self.bearer, "draft")
                        create_offer_for_product(
                            self.bearer,
                            article=all_products_map.get(offer.article),
                            barcode=f"{offer.article}{intertop_size_id}",
                            quantity=offer.stock_quantity,
                            size_id=intertop_size_id,
                            base_price_amount=offer.price,
                            discount_price_amount=offer.discount_price,
                        )
                        break
        app_logger.info(f"Created {created_offers_count} new offers.")
        return update_offer_task_args, used_article_sizeID_mapping

    def _run_async_updates(
        self, tasks_args_list: list, task_description: str = "Updating offers"
    ):
        """
        Обертка для запуску asyncio.run(run_all_offer_updates(...))
        з обробкою RuntimeError.
        """
        if not tasks_args_list:
            app_logger.info(f"No tasks to run for '{task_description}'.")
            return

        app_logger.info(f"Found {len(tasks_args_list)} tasks for '{task_description}'.")
        try:
            asyncio.run(run_all_offer_updates(tasks_args_list, task_description))
        except RuntimeError as e:
            app_logger.critical(
                f"Asyncio error: {e}. Event loop might be already running!"
            )

    def _prepare_offer_deactivations(
        self, used_article_sizeID_mapping: set
    ) -> List[tuple]:
        """
        Готує список завдань для деактивації оферів,
        яких більше немає в каталозі.
        """
        app_logger.info("Preparing offer deactivations...")
        # Оновлюємо список продуктів, щоб отримати актуальні статуси
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
            ) = self.article_sizeID_mapping.get(
                (offer_key[0], offer_key[1]), (None, None, None, None, None, None)
            )

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
        return deactivate_offer_task_args

    def _finalize_status_updates(self, articles_to_moderate: set):
        """
        Повертає продукти, що були на модерації,
        назад у статус 'moderate'.
        """
        app_logger.info(
            f"Returning {len(articles_to_moderate)} articles to 'moderate' status."
        )
        for article in articles_to_moderate:
            change_product_status(article, self.bearer, "moderate")

    def update_intertop(self):
        """
        Головний метод-координатор для повного оновлення Intertop.
        """
        if not self.article_uniq_groups:
            self._prepare_data_maps()

        # 1. Аналізуємо поточний стан продуктів
        categorized_products = self._categorize_intertop_products()

        # 2. Переводить старі продукти та "moderate" у "draft"
        (
            articles_to_moderate,
            vendorCodes_draft,
        ) = self._handle_product_status_changes(categorized_products)

        # 3. Готуємо список оферів на оновлення
        # (Цей метод МОДИФІКУЄ articles_to_moderate та vendorCodes_draft)
        update_args, used_keys = self._prepare_offer_updates(
            categorized_products, articles_to_moderate, vendorCodes_draft
        )

        # 4. Виконуємо асинхронне оновлення
        self._run_async_updates(update_args, "Updating offers prices and quantities")

        # 5. Готуємо список оферів на деактивацію
        deactivate_args = self._prepare_offer_deactivations(used_keys)

        # 6. Виконуємо асинхронну деактивацію
        self._run_async_updates(deactivate_args, "Deactivating offers")

        # 7. Повертаємо продукти на модерацію
        self._finalize_status_updates(articles_to_moderate)

        app_logger.info("Intertop update completed.")
