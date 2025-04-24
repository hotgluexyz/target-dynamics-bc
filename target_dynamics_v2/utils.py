from typing import TypedDict, List

class Currency(TypedDict):
    id: str
    code: str
    displayName: str
    symbol: str
    amountDecimalPlaces: str
    amountRoundingPrecision: float
    lastModifiedDateTime: str

class PaymentMethod(TypedDict):
    id: str
    code: str
    displayName: str
    lastModifiedDateTime: str

class Company(TypedDict):
    id: str
    name: str
    displayName: str
    businessProfileId: str
    currencies: List[Currency]
    paymentMethods: List[PaymentMethod]

class ReferenceData(TypedDict):
    companies: List[Company]