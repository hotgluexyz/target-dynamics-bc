from typing import List

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.mappers.customer_schema_mapper import CustomerSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class CustomerSink(DynamicsBaseBatchSink):
    name = "Customers"
    # fields in the tenant-config of type=field that are allowed to be overwritten
    allowed_fields_override = ["parentId"]

    def preprocess_batch(self, records: List[dict]):
        # fetch reference data related to existing customers
        existing_company_customers = self.dynamics_client.get_existing_entities_for_records(
            self._target.reference_data.get("companies", []),
            self.name,
            records
        )

        self.reference_data = {**self._target.reference_data, self.name: existing_company_customers}

    def process_batch_record(self, record: dict) -> dict:
        # perform the mapping
        mapped_record = CustomerSchemaMapper(record, self, self.reference_data)
        payload = mapped_record.to_dynamics()

        request_params = self.get_request_params(mapped_record)

        default_dimensions_requests = []
        if mapped_record.existing_record and payload.get("defaultDimensions"):
            default_dimensions = payload.pop("defaultDimensions")
            default_dimensions_requests = mapped_record._create_default_dimensions_requests(default_dimensions)

        records = [{"payload": payload, "request_params": request_params }]
        records += default_dimensions_requests

        return {"records": records}

    def get_request_params(self, mapped_record):
        endpoint = DynamicsClient.ref_request_endpoints[self.name]
        endpoint = endpoint.format(companyId=mapped_record.company['id'])
        request_params = {
            "url": endpoint,
            "method": "POST"
        }

        if mapped_record.existing_record:
            request_params = {
                "url": f"{endpoint}({mapped_record.existing_record['id']})",
                "method": "PATCH"
            }
        
        return request_params
