from dataclasses import dataclass, field
from typing import List, Dict, Union, Optional

@dataclass
class Param:
    name: str
    value: Union[str, Dict[str, str]]

@dataclass
class Offer:
    id: str
    url: str
    available: bool
    price: float
    discount_price: float
    currency_id: str
    category_id: str
    vendor: Optional[str]
    article: Optional[str]
    stock_quantity: Optional[int]
    name: str
    name_ua: str
    description: str
    description_ua: str
    pictures: List[str] = field(default_factory=list)
    params: List[Param] = field(default_factory=list)

    def is_in_stock(self) -> bool:
        return self.available and (self.stock_quantity is None or self.stock_quantity > 0)

    def __str__(self) -> str:
        return f"Offer(id={self.id}, name='{self.name_ua}', price={self.price} {self.currency_id})"
    
@dataclass
class XmlCatalog:
    name: str
    company: str
    url: str
    catalog_date: str
    currencies: Dict[str, str]
    categories: Dict[str, str]
    offers: List[Offer]

    def __len__(self) -> int:
        return len(self.offers)