from typing import TypedDict, List

class InvalidConfigurationError(Exception):
    pass

class DimensionDefinitionNotFound(InvalidConfigurationError):
    pass

class InvalidCustomFieldDefinition(InvalidConfigurationError):
    pass

class InvalidInputError(Exception):
    pass

class RecordNotFound(InvalidInputError):
    pass

class CompanyNotFound(InvalidInputError):
    pass

class InvalidDimensionValue(InvalidInputError):
    pass

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

class DimensionValue(TypedDict):
    id: str
    code: str
    dimensionId: str
    displayName: str

class Dimension(TypedDict):
    id: str
    code: str
    displayName: str
    dimensionValues: List[DimensionValue]

class Company(TypedDict):
    id: str
    name: str
    displayName: str
    businessProfileId: str
    currencies: List[Currency]
    paymentMethods: List[PaymentMethod]
    dimensions: List[Dimension]

class ReferenceData(TypedDict):
    companies: List[Company]
