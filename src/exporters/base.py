from abc import ABC, abstractmethod
from typing import Any

from lxml import etree as ET # type: ignore

from ..schemas import data_schema


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
        parent: ET._Element, tag_name: str, text: Any = None, cdata: bool = False
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


