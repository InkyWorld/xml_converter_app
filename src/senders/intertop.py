import asyncio
import logging
from enum import Enum
from typing import Any, Optional, Tuple

import httpx
import requests
from tqdm import tqdm

from src.config import BASE_LINK_INTERTOP

if not BASE_LINK_INTERTOP:
    raise ValueError("BASE_LINK_INTERTOP is not set in config.py")

app_logger = logging.getLogger(__name__)


class RequestMethod(Enum):
    GET = "get"
    POST = "post"
    PATCH = "patch"
    PUT = "put"


def make_request(
    link: str,
    method: str = "GET",
    bearer: Optional[str] = None,
    params: Optional[dict[str, Any]] = None,
    data: Optional[dict | list] = None,
    retries: int = 3,
    sleep_time: int = 3,
) -> Optional[Any]:
    """
    Надсилає HTTP-запит до вказаного посилання.

    Автоматично обробляє автентифікацію Bearer,
    параметри запиту (query params) та надсилання тіла у форматі JSON.

    Args:
        link: URL-адреса для запиту.
        method: HTTP-метод (наприклад, "GET", "POST", "PATCH", "DELETE").
        bearer: Токен Bearer для автентифікації (необов'язково).
        params: Словник параметрів запиту (query parameters, необов'язково).
        jdata: Об'єкт (dict або list) для надсилання як JSON-тіло.
                   Використовується для методів POST, PATCH тощо.

    Returns:
        Розпарсені JSON-дані з відповіді у разі успіху (статус 2xx),
        або None у разі будь-якої помилки (помилка з'єднання,
        статус 4xx/5xx, або успішна відповідь без тіла).
    """
    headers = {}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
    for i in range(retries):
        try:
            response = requests.request(
                method=method.upper(),
                url=link,
                headers=headers,
                params=params,
                json=data,
            )
            response.raise_for_status()
            if response.text:
                return response.json()

            return None

        except requests.HTTPError as e:
            if i == retries - 1:
                app_logger.error(f"Max retries reached for {method.upper()} {link}")
                app_logger.error(f"Params: {params}, JSON Data: {data}")
                app_logger.error(f"Response Body: {e.response.text}")


def auth(link, key, secret) -> Tuple[str | None, int | None]:
    payload = {
        "app_key": key,
        "app_secret": secret,
    }
    try:
        response = requests.post(link, json=payload)
        response.raise_for_status()
        token_data = response.json()
        token = token_data.get("data", {}).get("access_token", {}).get("token")
        expires_date = (
            token_data.get("data", {}).get("access_token", {}).get("expires_date")
        )
        if isinstance(token, str) and isinstance(expires_date, int):
            return (token, expires_date)
        else:
            # Логуємо, якщо структура відповіді була неочікуваною
            app_logger.error(
                "Token or expires_date missing or has invalid type in response."
            )
            app_logger.debug(f"Received data: {token_data}")
            return (None, None)

    except requests.HTTPError as e:
        app_logger.error(
            f"HTTP Error: {e.response.status_code} while trying to auth at {link}"
        )
        app_logger.error(f"Response Body: {e.response.text}")
        return (None, None)

    except requests.JSONDecodeError:
        app_logger.error(f"Failed to decode JSON response from {link}")
        app_logger.debug(f"Response text: {response.text}")
        return (None, None)

    except requests.RequestException as e:
        app_logger.error(f"Request failed during authentication: {e}")
        return (None, None)


def updating_product_price(
    article, bearer, base_price_amount, discount_price_amount, activity
):
    payload = {
        "active": activity,
        "base_price": {
            "amount": base_price_amount,
            "currency": "UAH",
        },
        "discount_price": {
            "amount": discount_price_amount,
            "currency": "UAH",
        },
    }

    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}/offers/prices",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def create_offer_for_product(
    bearer,
    article,
    barcode,
    quantity,
    size_id,
    base_price_amount,
    discount_price_amount,
):
    payload = {
        "barcode": barcode,
        "active": True,
        "base_price": {"amount": base_price_amount, "currency": "UAH"},
        "discount_price": {"amount": discount_price_amount, "currency": "UAH"},
        "quantity": quantity,
        "size_id": size_id,
    }

    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}/offers",
        bearer=bearer,
        data=payload,
        method=RequestMethod.POST.value,
    )
    return response


def archive_product(
    article,
    bearer,
):
    payload = {"active": False}

    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def change_product_activity(article, bearer, activity: bool):
    payload = {"active": activity}

    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def change_product_status(article, bearer, status):
    payload = {"status": status}

    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}/status",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PUT.value,
    )
    return response


async def update_offer(
    client: httpx.AsyncClient,
    bearer,
    article: str,
    barcode: str,
    base_price_amount,
    discount_price_amount,
    quantity,
    activity,
):
    payload = {
        "base_price": {
            "amount": base_price_amount,
            "currency": "UAH",
        },
        "discount_price": {
            "amount": discount_price_amount,
            "currency": "UAH",
        },
        "active": activity,
        "quantity": quantity,
    }
    response = await make_request_async(
        client=client,
        link=BASE_LINK_INTERTOP + f"products/{article}/offers/{barcode}",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def archive_offer_and_quantity_zero(
    bearer,
    article: str,
    barcode: str,
    base_price_amount,
    discount_price_amount,
):
    payload = {
        "base_price": {
            "amount": base_price_amount,
            "currency": "UAH",
        },
        "discount_price": {
            "amount": discount_price_amount,
            "currency": "UAH",
        },
        "delayed_prices": {
            "base_price": {
                "amount": base_price_amount,
                "currency": "UAH",
            },
            "discount_price": {
                "amount": discount_price_amount,
                "currency": "UAH",
            },
            "active": False,
            "quantity": 0,
        },
    }
    response = make_request(
        BASE_LINK_INTERTOP + f"products/{article}/offers/{barcode}",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def update_offers_quantity(bearer, payload: dict):
    """
    Args:
        bearer: The bearer token for authentication.
        offers_data: dict
                     Example:
                     {
                        "offers": [
                            {
                            "quantity": ...,
                            "barcode": ...,
                            "article": ...
                            }
                        ]
                    }
    Returns:
        The response from the make_request function.
    """
    response = make_request(
        BASE_LINK_INTERTOP + "offers/quantity",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def update_offers_prices(bearer, offers):
    payload = []
    for offer in offers:
        intertop_barcode = offer[0]
        intertop_base_price_amount = offer[1]
        intertop_discount_price_amount = offer[2]

        payload.append(
            {
                "barcode": intertop_barcode,
                "base_price": {
                    "amount": intertop_base_price_amount,
                    "currency": "UAH",
                },
                "discount_price": {
                    "amount": intertop_discount_price_amount,
                    "currency": "UAH",
                },
                "delayed_prices": {
                    "base_price": {
                        "amount": intertop_base_price_amount,
                        "currency": "UAH",
                    },
                    "discount_price": {
                        "amount": intertop_discount_price_amount,
                        "currency": "UAH",
                    },
                },
                "active": False,
                "quantity": 0,
            }
        )
    response = make_request(
        BASE_LINK_INTERTOP + "offers/prices",
        bearer=bearer,
        data=payload,
        method=RequestMethod.PATCH.value,
    )
    return response


def get_product_articles(bearer):
    all_articles = []
    offset = 0
    while True:
        params = {"limit": 300, "offset": offset}
        response = make_request(
            BASE_LINK_INTERTOP + "products", bearer=bearer, params=params
        )
        if response is None:
            break
        items = response.get("data", {}).get("items", [])
        if not items:
            break
        articles = []
        articles = [
            item.get("vendor_code")
            for item in items
            if item.get("status", {}).get("code") == "uploaded"
        ]
        all_articles.extend(articles)
        offset += 300
    return all_articles


def get_products(bearer):
    all_products = []
    offset = 0
    while True:
        params = {"limit": 300, "offset": offset}
        response = make_request(
            BASE_LINK_INTERTOP + "products", bearer=bearer, params=params
        )
        if response is None:
            break
        items = response.get("data", {}).get("items", [])
        if not items:
            break

        all_products.extend(items)
        offset += 300
    return all_products


def get_barcodes_and_sizes_for_articles(bearer: str, articles: list) -> dict:
    result = {}
    for article in tqdm(articles):
        response = make_request(
            BASE_LINK_INTERTOP + f"products/{article}/offers", bearer
        )
        if response:
            items = response.get("data", {}).get("items", [])
            for item in items:
                barcode = item.get("barcode")
                size_id = item.get("size_id")
                result[(article, size_id)] = barcode
    return result


def get_offers_data_by_articles(bearer: str, articles: list) -> dict:
    result = {}
    for article in tqdm(articles):
        response = make_request(
            BASE_LINK_INTERTOP + f"products/{article}/offers", bearer=bearer
        )
        if response:
            items = response.get("data", {}).get("items", [])
            for item in items:
                barcode = item.get("barcode")
                size_id = item.get("size_id")
                base_price_amount = item.get("base_price")
                discount_price_amount = item.get("discount_price")
                active = item.get("active")
                quantity = item.get("quantity")
                result[(article, size_id)] = [
                    barcode,
                    base_price_amount,
                    discount_price_amount,
                    active,
                    quantity,
                ]
    return result


def get_product_offers(bearer: str, product) -> dict:
    result = {}
    product_art = product.get("article")
    response = make_request(
        BASE_LINK_INTERTOP + f"products/{product_art}/offers", bearer=bearer
    )
    if response:
        items = response.get("data", {}).get("items", [])
        for item in items:
            barcode = item.get("barcode")
            size_id = item.get("size_id")
            base_price_amount = item.get("base_price")
            discount_price_amount = item.get("discount_price")
            offer_activity = item.get("active")
            quantity = item.get("quantity")
            result[(product_art, size_id)] = [
                barcode,
                base_price_amount,
                discount_price_amount,
                product.get("status", {}).get("code"),
                offer_activity,
                quantity,
            ]
    return result


async def make_request_async(
    client: httpx.AsyncClient,
    link: str,
    method: str = "GET",
    bearer: Optional[str] = None,
    params: Optional[dict[str, Any]] = None,
    data: Optional[dict | list] = None,
    retries: int = 10,
    sleep_time: int = 10,
    backoff_factor: float = 1.5,  # увеличиваем задержку после каждой неудачи
) -> Optional[Any]:
    """
    Асинхронно делает HTTP-запрос с гарантией, что он дойдет (многократные повторы).
    Если после всех попыток неудача — ждет и пробует снова бесконечно.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"

    attempt = 0
    delay = sleep_time

    while True:  # бесконечный цикл до успеха
        attempt += 1
        for i in range(retries):
            try:
                response = await client.request(
                    method=method.upper(),
                    url=link,
                    headers=headers,
                    params=params,
                    json=data,
                    timeout=30.0,
                )
                response.raise_for_status()

                if response.text:
                    return response.json()
                return None

            except httpx.HTTPStatusError as e:
                status = e.response.status_code

                # Не повторяем при 404
                if status == 404:
                    return None

                # Обрабатываем 429 (rate limit)
                if status == 429:
                    retry_after = int(e.response.headers.get("Retry-After", delay))
                    app_logger.warning(
                        f"429 Too Many Requests — спим {retry_after} сек"
                    )
                    await asyncio.sleep(retry_after)
                    continue

                if i == retries - 1:
                    app_logger.error(
                        f"HTTP Error {status}: attempt={attempt}-{i} for {method.upper()} {link}"
                    )
                    app_logger.error(f"Params: {params}, JSON Data: {data}")
                    app_logger.error(f"Response Body: {e.response.text}")

                    await asyncio.sleep(delay)

            except (httpx.ConnectError, httpx.TimeoutException, httpx.ReadError) as e:
                app_logger.warning(
                    f"⚠️ Connection/Timeout Error: attempt={attempt}-{i} for {link} | {e}"
                )
                await asyncio.sleep(delay)

        # После исчерпания попыток — ждём дольше и пробуем снова
        delay = min(delay * backoff_factor, 300)  # растёт до 5 минут
        app_logger.warning(
            f"🔁 Повторный цикл для {method.upper()} {link} после {delay:.1f} сек"
        )
        await asyncio.sleep(delay)


async def get_product_offers_async(
    client: httpx.AsyncClient,  # Приймає сесію клієнта
    bearer: str,
    product,
) -> dict:
    """
    Асинхронна функція для отримання пропозицій по продукту.
    """
    result = {}
    product_art = product.get("article")

    # Використовуємо асинхронний запит
    response = await make_request_async(
        client=client,
        link=BASE_LINK_INTERTOP + f"products/{product_art}/offers",
        bearer=bearer,
    )

    if response:
        items = response.get("data", {}).get("items", [])
        for item in items:
            barcode = item.get("barcode")
            size_id = item.get("size_id")
            base_price_amount = item.get("base_price")
            discount_price_amount = item.get("discount_price")
            offer_activity = item.get("active")
            quantity = item.get("quantity")
            result[(product_art, size_id)] = [
                barcode,
                float(base_price_amount),
                float(discount_price_amount),
                product.get("status", {}).get("code"),
                offer_activity,
                int(quantity),
            ]
    return result


async def run_all_offer_updates(
    tasks_args_list: list,
    task_description: str = "Updating offers",
    concurrency_limit: int = 10,
):
    """
    Створює один AsyncClient і паралельно виконує всі завдання update_offer.

    Args:
        tasks_args_list: Список, де кожен елемент - це кортеж
                         з аргументами для update_offer
                         (bearer, article, barcode, ...).
    """
    semaphore = asyncio.Semaphore(concurrency_limit)

    async with httpx.AsyncClient() as client:

        async def semaphore_task_wrapper(args):
            async with semaphore:
                return await update_offer(client, *args)

        tasks = []
        for args in tasks_args_list:
            tasks.append(semaphore_task_wrapper(args))

        app_logger.info(
            f"Running {len(tasks)} parallel {task_description} tasks "
            f"with a concurrency limit of {concurrency_limit}..."
        )

        for future in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc=task_description
        ):
            try:
                await future
            except Exception as e:
                app_logger.error(f"An offer update task failed: {e}")


async def async_load_wrapper(products, bearer) -> dict:
    """
    Асинхронна "обгортка", яка запускає всі запити паралельно
    З ОБМЕЖЕННЯМ одночасних запитів.
    """
    result = {}

    semaphore = asyncio.Semaphore(10)

    async def fetch_with_semaphore(client, product):
        async with semaphore:
            return await get_product_offers_async(
                client=client, bearer=bearer, product=product
            )

    async with httpx.AsyncClient() as client:
        tasks = []
        for product in products:
            tasks.append(fetch_with_semaphore(client, product))

        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            try:
                product_offers = await future
                result |= product_offers
            except Exception as e:
                app_logger.error(f"Error processing a product offer: {e}")

    return result
