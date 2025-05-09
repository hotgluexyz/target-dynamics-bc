from typing import List

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.mappers.vendor_schema_mapper import VendorSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class VendorSink(DynamicsBaseBatchSink):
    name = "Vendors"
    record_type = "Vendors"

    def preprocess_batch(self, records: List[dict]):
        # fetch reference data related to existing vendors
        filter_mappings = [
            {"field_from": "id", "field_to": "id", "should_quote": False},
            {"field_from": "externalId", "field_to": "number", "should_quote": True}
        ]

        existing_company_vendors = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            self.record_type,
            records,
            filter_mappings,
            expand="defaultDimensions"
        )

        self.reference_data = {**self._target.reference_data, self.name: existing_company_vendors}

    def process_batch_record(self, record: dict) -> dict:
        # perform the mapping
        mapped_record = VendorSchemaMapper(record, self, self.reference_data)
        payload = mapped_record.to_dynamics()

        request_params = DynamicsClient.get_entity_upsert_request_params(self.record_type, mapped_record.company["id"], payload.get("id"))

        default_dimensions_requests = []
        if mapped_record.existing_record and payload.get("defaultDimensions"):
            default_dimensions = payload.pop("defaultDimensions")
            default_dimensions_requests = DynamicsClient.create_default_dimensions_requests(
                self.record_type,
                mapped_record.company["id"],
                payload["id"],
                default_dimensions
            )

        records = [{"payload": payload, "request_params": request_params }]
        records += default_dimensions_requests

        return {"records": records}
