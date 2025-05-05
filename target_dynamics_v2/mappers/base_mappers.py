from typing import List

from target_dynamics_v2.utils import ReferenceData, CompanyNotFound, InvalidDimensionValue, RecordNotFound

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""
    existing_record_pk_mappings = []

    def __init__(
            self,
            record,
            sink,
            reference_data
    ) -> None:
        self.record = record
        self.sink = sink
        self._map_custom_fields()
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

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            if record_id := self.record.get(existing_record_pk_mapping["record_field"]):
                found_record = next(
                    (dynamics_record for dynamics_record in existing_entities_in_dynamics
                    if dynamics_record[existing_record_pk_mapping["dynamics_field"]] == record_id),
                    None
                )
                if existing_record_pk_mapping["required_if_present"] and found_record is None:
                    raise RecordNotFound(f"Record {existing_record_pk_mapping['record_field']}={record_id} not found Dynamics. Skipping it")
                
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

    def _get_dimension(self, dimension_code: str):
        return next(
            (dimension for dimension in self.company["dimensions"] if dimension["code"] == dimension_code),
            None)

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

        return None

    def _get_existing_default_dimension(self, dimension_id: str):
        if not self.existing_record:
            return None
        
        existing_dimensions = self.existing_record.get("defaultDimensions", [])
        return next(
            (existing_dimension for existing_dimension in existing_dimensions if existing_dimension["dimensionId"] == dimension_id),
            None
        )

    def _map_default_dimensions_dimensions(self):
        default_dimensions = []
        dimension_mapping = self.sink._target.dimensions_mapping.get(self.sink.name, {})
        for field_name, dimension_code in dimension_mapping.items():
            dimension = self._get_dimension(dimension_code)
            field_id = self.record.get(f"{field_name}Id", None)
            field_external_id = self.record.get(f"{field_name}ExternalId", None)
            field_name = self.record.get(f"{field_name}Name", None)

            if not field_id and not field_external_id and not field_name:
                continue

            if dimension_value := self._get_dimension_value(dimension, field_id, field_external_id, field_name):
                default_dimension = {
                    "dimensionId": dimension_value["dimensionId"],
                    "dimensionValueId": dimension_value["id"]
                }
                if existing_default_dimension := self._get_existing_default_dimension(dimension["id"]):
                    default_dimension["id"] = existing_default_dimension["id"]
                default_dimensions.append(default_dimension)  
            else:
                raise InvalidDimensionValue(f"Dimension could not find a Dimension Value for dimension {dimension['code']} when looking up dimension id={field_id} / code={field_external_id} / displayName={field_name}")

        return {"defaultDimensions": default_dimensions} if default_dimensions else {}

    def _map_vendor(self):
        vendor_info = {}

        found_vendor = None
        vendors_reference_data = self.reference_data.get("Vendors", {}).get(self.company["id"], [])

        if vendor_id := self.record.get("vendorId"):
            found_vendor = next(
                (vendor for vendor in vendors_reference_data
                if vendor["id"] == vendor_id),
                None
            )

        if (vendor_number := self.record.get("vendorExternalId")) and not found_vendor:
            found_vendor = next(
                (vendor for vendor in vendors_reference_data
                if vendor["number"] == vendor_number),
                None
            )

        if (vendor_name := self.record.get("vendorName")) and not found_vendor:
            found_vendor = next(
                (vendor for vendor in vendors_reference_data
                if vendor["displayName"] == vendor_name),
                None
            )

        if found_vendor:
            vendor_info = {
                "vendorId": found_vendor["id"]
            }

        return vendor_info

    def _map_fields(self, payload):
        for record_key, payload_key in self.field_mappings.items():
            if record_key in self.record and self.record.get(record_key) != None:
                if isinstance(payload_key, list):
                    for key in payload_key:
                        payload[key] = self.record.get(record_key)
                else:
                    payload[payload_key] = self.record.get(record_key)

    def _map_custom_fields(self):
        self.field_mappings = {**self.field_mappings, **self.sink._target.fields_mapping.get(self.sink.name, {})}
