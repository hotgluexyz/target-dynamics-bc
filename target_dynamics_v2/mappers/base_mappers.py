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
        self.existing_record = self._find_existing_record(self.reference_data[sink_name])
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

    def _map_company(self):
        company = None

        if subsidiary_id := self.record.get("subsidiaryId"):
            company = self._find_company_by_id(subsidiary_id)

        if subsidiary_name := self.record.get("subsidiaryName") and company is None:
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