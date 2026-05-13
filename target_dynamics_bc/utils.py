import json
from typing import List, Optional
from typing_extensions import TypedDict

from target_hotglue.common import HGJSONEncoder


def extract_error_message(response: dict) -> Optional[str]:
    if not isinstance(response, dict):
        return None
    body = response.get("body")
    if body is None:
        return None
    if not isinstance(body, dict):
        return str(body)
    error = body.get("error")
    if error is None:
        return str(body)
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message:
            return message
        return json.dumps(error, cls=HGJSONEncoder)
    return str(error)

class InvalidConfigurationError(Exception):
    pass

class DimensionDefinitionNotFound(InvalidConfigurationError):
    pass

class InvalidCustomFieldDefinition(InvalidConfigurationError):
    pass

class InvalidRecordState(Exception):
    pass

class InvalidInputError(Exception):
    pass

class RecordNotFound(InvalidInputError):
    pass

class DuplicatedRecord(InvalidInputError):
    pass

class MissingField(InvalidInputError):
    pass

class InvalidFieldValue(InvalidInputError):
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

class Account(TypedDict):
    id: str
    number: str
    displayName: str
    category: str
    subCategory: str
    blocked: bool
    accountType: str
    lastModifiedDateTime: str

class Location(TypedDict):
    id: str
    code: str
    displayName: str
    contact: str
    addressLine1: str
    addressLine2: str
    city: str
    state: str
    country: str
    postalCode: str
    phoneNumber: str
    email: str
    website: str
    lastModifiedDateTime: str
class Company(TypedDict):
    id: str
    name: str
    displayName: str
    businessProfileId: str
    currencies: List[Currency]
    paymentMethods: List[PaymentMethod]
    dimensions: List[Dimension]
    accounts: List[Account]
    locations: List[Location]

class ReferenceData(TypedDict):
    companies: List[Company]
