from typing import Dict, List, Optional

from target_dynamics_v2.utils import ReferenceData, CompanyNotFound, InvalidDimensionValue, RecordNotFound, DimensionDefinitionNotFound

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""
    
    def __init__(
            self,
            record,
            sink,
            reference_data
    ) -> None:
        self.record = record
        self.sink = sink
        self.reference_data: ReferenceData = reference_data
        self.company = self._map_company()
        self.existing_record = self._find_existing_record(self.reference_data.get(self.sink.name, {}))

    @staticmethod
    def get_company_from_record(company_list: List[dict], record: dict) -> dict:
        company = None

        if subsidiary_id := record.get("subsidiaryId"):
            company = next(
                (company for company in company_list
                if company["id"] == subsidiary_id),
                None
            )

        if (subsidiary_name := record.get("subsidiaryName")) and company is None:
            company = next(
                (company for company in company_list
                if company["name"] == subsidiary_name),
                None
            )

        return company

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        if self.company is None:
            return None
        
        existing_entities_in_dynamics = reference_list.get(self.company["id"], [])

        if record_id := self.record.get("id"):
            # Try matching internal ID first
            found_record = next(
                (record for record in existing_entities_in_dynamics
                if record["id"] == record_id),
                None
            )
            if found_record is None:
                raise RecordNotFound(f"Record ID={record_id} not found Dynamics. Skipping it")
            
            return found_record
            
        if record_external_id := self.record.get("externalId"):
            # Try matching externalId
            found_record = next(
                (record for record in existing_entities_in_dynamics
                if record.get("number") == record_external_id),
                None
            )
            if found_record:
                return found_record
        
        return None

    def _map_internal_id(self):
        if self.existing_record:
            return { "id": self.existing_record["id"]}

        return {}

    def _map_phone_number(self):
        """Extracts phone numbers in Dynamics format."""
        phone = {}

        if phone_numbers := self.record.get("phoneNumbers", []):
            found_record = next(
                (phone_number for phone_number in phone_numbers
                if phone_number["type"] == "unknown"),
                None
            )

            if not found_record:
                found_record = phone_numbers[0]
            
            phone = {"phoneNumber": found_record["phoneNumber"]}

        return phone

    def _map_address(self):
        """Extracts addresses to Dynamics format."""
        address_info = {}

        if addresses := self.record.get("addresses", []):
            found_record = next(
                (address for address in addresses
                if address["addressType"] == "shipping"),
                None
            )

            if not found_record:
                found_record = addresses[0]

            address_info = {
                "addressLine1": found_record.get("line1"),
                "addressLine2": found_record.get("line2"),
                "city": found_record.get("city"),
                "state": found_record.get("state"),
                "country": found_record.get("country"),
                "postalCode": found_record.get("postalCode"),
            }

        return address_info

    def _map_currency(self):
        """Extracts currency to Dynamics format."""
        currency_info = {}

        found_currency = None

        if currency_id := self.record.get("currencyId"):
            found_currency = next(
                (currency for currency in self.company["currencies"]
                if currency["id"] == currency_id),
                None
            )

        if (currency_code := self.record.get("currency")) and not found_currency:
            found_currency = next(
                (currency for currency in self.company["currencies"]
                if currency["code"] == currency_code),
                None
            )

        if (currency_name := self.record.get("currencyName")) and not found_currency:
            found_currency = next(
                (currency for currency in self.company["currencies"]
                if currency["displayName"] == currency_name),
                None
            )

        if found_currency:
            currency_info = {
                "currencyId": found_currency["id"],
                "currencyCode": found_currency["code"]
            }
        elif currency_code:
            currency_info = { "currencyCode": currency_code }
            

        return currency_info

    def _map_company(self):
        return BaseMapper.get_company_from_record(self.reference_data.get("companies", []), self.record)

    def _validate_company(self):
        if not self.company:
            subsidiary_id = self.record.get("subsidiaryId")
            subsidiary_name = self.record.get("subsidiaryName")
            raise CompanyNotFound(f"Could not find Company with subsidiaryId={subsidiary_id} / subsidiaryName={subsidiary_name}")

    def _get_dimension(self, dimension_id: Optional[str] = None, dimension_code: Optional[str] = None, dimension_display_name: Optional[str] = None):
        found_dimension = next(
            (dimension for dimension in self.company["dimensions"]
                if dimension["id"] == dimension_id or dimension["code"] == dimension_code or dimension["displayName"] == dimension_display_name
            ),
            None)
        
        if not found_dimension:
            raise DimensionDefinitionNotFound(f"Could not find dimension with id={dimension_id} / code={dimension_code} / displayName={dimension_display_name} for companyId={self.company['id']}")
        
        return found_dimension

    def _get_dimension_value(self, dimension: dict, value_id: str, value_code: str, value_display_name: str):
        """Find dimension value by looking for dimension id, code or displayName"""
        if found_dimension_value := next(
            (dimension_value for dimension_value in dimension.get("dimensionValues", []) if dimension_value["id"] == value_id),
            None
        ):
            return found_dimension_value
        
        if found_dimension_value := next(
            (dimension_value for dimension_value in dimension.get("dimensionValues", []) if dimension_value["code"] == value_code),
            None
        ):
            return found_dimension_value
        
        if found_dimension_value := next(
            (dimension_value for dimension_value in dimension.get("dimensionValues", []) if dimension_value["displayName"] == value_display_name),
            None
        ):
            return found_dimension_value

        if not found_dimension_value:
            raise InvalidDimensionValue(f"Dimension could not find a Dimension Value for dimension {dimension['code']} when looking up dimension value id={value_id} / code={value_code} / displayName={value_display_name}")

        return None

    def _get_existing_default_dimension(self, dimension_id: str):
        if not self.existing_record:
            return None
        
        existing_dimensions = self.existing_record.get("defaultDimensions", [])
        return next(
            (existing_dimension for existing_dimension in existing_dimensions if existing_dimension["dimensionId"] == dimension_id),
            None
        )

    def _map_default_dimensions_from_root_fields(self):
        default_dimensions = []

        dimension_mapping = self.sink._target.dimensions_mapping
        for field_name, dimension_code in dimension_mapping.items():
            dimension = self._get_dimension(dimension_code=dimension_code)
            field_id = self.record.get(f"{field_name}Id", None)
            field_external_id = self.record.get(f"{field_name}ExternalId", None)
            field_name = self.record.get(f"{field_name}Name", None)

            if not field_id and not field_external_id and not field_name:
                continue

            dimension_value = self._get_dimension_value(dimension, field_id, field_external_id, field_name)
            default_dimension = {
                "dimensionId": dimension_value["dimensionId"],
                "dimensionValueId": dimension_value["id"]
            }
            if existing_default_dimension := self._get_existing_default_dimension(dimension["id"]):
                default_dimension["id"] = existing_default_dimension["id"]
            default_dimensions.append(default_dimension)  

        return default_dimensions

    def _map_default_dimensions_from_dimensions_field(self, existing_dimensions: Optional[List[Dict]]=[]):
        default_dimensions = []

        for record_dimension in self.record.get("dimensions", []):
            dimension_id = record_dimension.get("id")
            dimension_code = record_dimension.get("externalId")
            dimension_name = record_dimension.get("name")

            dimension_value_id = record_dimension.get("valueId")
            dimension_value_code = record_dimension.get("valueExternalId")
            dimension_value_name = record_dimension.get("value")

            # first validate that the dimension exists in Dynamics
            dimension = self._get_dimension(dimension_id=dimension_id, dimension_code=dimension_code, dimension_display_name=dimension_name)

            if not dimension_value_id and not dimension_value_code and not dimension_value_name:
                raise InvalidDimensionValue(f"No value was provided for dimension {dimension['code']}")

            dimension_value = self._get_dimension_value(dimension, dimension_value_id, dimension_value_code, dimension_value_name)
            default_dimension = {
                "dimensionId": dimension_value["dimensionId"],
                "dimensionValueId": dimension_value["id"]
            }
            if existing_default_dimension := self._get_existing_default_dimension(dimension["id"]):
                default_dimension["id"] = existing_default_dimension["id"]
            default_dimensions.append(default_dimension)  

        return default_dimensions

    def _map_default_dimensions_dimensions(self):
        # we first try to map dimensions that is in the root field of the record, example classExternalId="CLASS01"
        default_dimensions = self._map_default_dimensions_from_root_fields()

        # then we map dimensions that is in the "dimensions" field of the record, example dimensions = [{ "externaId": "AREA", "valueExternalId": "15" }]
        default_dimensions += self._map_default_dimensions_from_dimensions_field(existing_dimensions=default_dimensions)
        
        return {"defaultDimensions": default_dimensions} if default_dimensions else {}


    def _map_fields(self, payload):
        for record_key, payload_key in self.field_mappings.items():
            if record_key in self.record and self.record.get(record_key) != None:
                if isinstance(payload_key, list):
                    for key in payload_key:
                        payload[key] = self.record.get(record_key)
                else:
                    payload[payload_key] = self.record.get(record_key)
