from target_dynamics_v2.utils import ReferenceData

class BaseMapper:
    """A base class responsible for mapping a record ingested in the unified schema format to a payload for NetSuite"""
    
    def __init__(
            self,
            record,
            sink_name,
            reference_data
    ) -> None:
        self.record = record
        self.sink_name = sink_name
        self.reference_data: ReferenceData = reference_data
        self.existing_record = self._find_existing_record(self.reference_data.get(sink_name, []))
        self._map_company()

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """

        if record_id := self.record.get("id"):
            # Try matching internal ID first
            found_record = next(
                (record for record in reference_list
                if record["id"] == record_id),
                None
            )
            if found_record:
                return found_record
        
        return None

    def _find_company_by_id(self, company_id):
        found_record = next(
            (company for company in self.reference_data["companies"]
            if company["id"] == company_id),
            None
        )

        return found_record
    
    def _find_company_by_name(self, company_name):
        found_record = next(
            (company for company in self.reference_data["companies"]
            if company["name"] == company_name),
            None
        )

        return found_record

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
            address = addresses[0]
            address_info = {
                "addressLine1": address.get("line1"),
                "addressLine2": address.get("line2"),
                "city": address.get("city"),
                "state": address.get("state"),
                "country": address.get("country"),
                "postalCode": address.get("postalCode"),
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
        company = None

        if subsidiary_id := self.record.get("subsidiaryId"):
            company = self._find_company_by_id(subsidiary_id)

        if (subsidiary_name := self.record.get("subsidiaryName")) and company is None:
            company = self._find_company_by_name(subsidiary_name)

        if company:
            self.company = company

    def _map_fields(self, payload):
        for record_key, payload_key in self.field_mappings.items():
            if record_key in self.record and self.record.get(record_key) != None:
                if isinstance(payload_key, list):
                    for key in payload_key:
                        payload[key] = self.record.get(record_key)
                else:
                    payload[payload_key] = self.record.get(record_key)