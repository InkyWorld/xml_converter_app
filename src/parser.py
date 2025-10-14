import os
from lxml import etree as ET  # type: ignore
from schemas import data_schema
from typing import Optional, List, Dict


class YmlParser:
    """
    Class responsible for parsing YML files.
    Accepts a file path and converts it into an XmlCatalog object.
    """

    def __init__(self, file_path: str):
        """
        Initialize the parser and load the XML root element.

        :param file_path: Path to the YML/XML file.
        :raises FileNotFoundError: If the file does not exist.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found at path: {file_path}")
        self.file_path = file_path
        self._root = self._get_xml_root()

    def _get_xml_root(self) -> Optional[ET._Element]:
        """
        Parse the XML file and return the root element.

        :return: Root XML element or None if parsing fails.
        :raises ValueError: If the XML syntax is invalid.
        :raises IOError: If the file cannot be read.
        """
        try:
            parser = ET.XMLParser(resolve_entities=False)
            tree = ET.parse(self.file_path, parser)
            return tree.getroot()
        except ET.XMLSyntaxError as e:
            raise ValueError(f"XML syntax error in file {self.file_path}: {e}")
        except IOError as e:
            raise IOError(f"Error reading file {self.file_path}: {e}")

    def parse(self) -> Optional[data_schema.XmlCatalog]:
        """
        Main public method that performs full YML parsing.

        :return: Parsed XmlCatalog object or None if root not found.
        :raises ValueError: If the <shop> tag is missing.
        """
        if self._root is None:
            return None

        shop = self._root.find('shop')
        if shop is None:
            raise ValueError("<shop> tag not found in the file.")

        # Delegate parsing of each section to private methods
        currencies = self._parse_currencies(shop)
        categories = self._parse_categories(shop)
        offers = self._parse_offers(shop)

        return data_schema.XmlCatalog(
            name=shop.findtext('name', 'N/A'),
            company=shop.findtext('company', 'N/A'),
            url=shop.findtext('url', 'N/A'),
            catalog_date=self._root.get('date', 'N/A'),
            currencies=currencies,
            categories=categories,
            offers=offers
        )

    def _parse_currencies(self, shop_element: ET._Element) -> Dict[str, str]:
        """
        Parse the <currencies> section.

        :param shop_element: <shop> XML element.
        :return: Dictionary of currency ID → rate.
        """
        return {
            currency.get('id'): currency.get('rate')
            for currency in shop_element.xpath('./currencies/currency')
        }

    def _parse_categories(self, shop_element: ET._Element) -> Dict[str, str]:
        """
        Parse the <categories> section.

        :param shop_element: <shop> XML element.
        :return: Dictionary of category ID → category name.
        """
        return {
            category.get('id'): category.text
            for category in shop_element.xpath('./categories/category')
        }

    def _parse_offers(self, shop_element: ET._Element) -> List[data_schema.Offer]:
        """
        Parse the <offers> section and return a list of Offer objects.

        :param shop_element: <shop> XML element.
        :return: List of parsed Offer objects.
        """
        offers_list = []
        for offer_element in shop_element.xpath('./offers/offer'):
            try:
                offer_obj = self._parse_single_offer(offer_element)
                offers_list.append(offer_obj)
            except (ValueError, TypeError) as e:
                offer_id = offer_element.get('id', 'N/A')
                print(f"Warning: Skipped offer ID={offer_id} due to data error: {e}")
        return offers_list

    def _parse_single_offer(self, offer_element: ET._Element) -> data_schema.Offer:
        """
        Parse a single <offer> element and create an Offer object.

        :param offer_element: <offer> XML element.
        :return: Offer object containing parsed data.
        """
        params_data = []
        for param_element in offer_element.findall('param'):
            param_name = param_element.get('name')
            values = param_element.findall('value')
            param_value = {v.get('lang'): v.text for v in values} if values else param_element.text
            params_data.append(data_schema.Param(name=param_name, value=param_value))

        stock = offer_element.findtext('stock_quantity')

        return data_schema.Offer(
            id=offer_element.get('id'),
            url=offer_element.findtext('url', ''),
            available=offer_element.get('available') == 'true',
            price=float(offer_element.findtext('price')),
            currency_id=offer_element.findtext('currencyId'),
            category_id=offer_element.findtext('categoryId'),
            vendor=offer_element.findtext('vendor'),
            article=offer_element.findtext('model'),
            stock_quantity=int(stock) if stock is not None else None,
            name=offer_element.findtext('name', '').strip(),
            name_ua=offer_element.findtext('name_ua', '').strip(),
            description=offer_element.findtext('description', '').strip(),
            description_ua=offer_element.findtext('description_ua', '').strip(),
            pictures=[pic.text for pic in offer_element.findall('picture')],
            params=params_data
        )
