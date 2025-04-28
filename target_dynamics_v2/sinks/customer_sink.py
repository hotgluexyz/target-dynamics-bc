from typing import List

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.mappers.customer_schema_mapper import CustomerSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class CustomerSink(DynamicsBaseBatchSink):
    name = "Customers"
    allowed_fields_override = ["parentId"]

    def preprocess_batch(self, records: List[dict]):
        # we need to map the company to query the existing customers
        mapped_records = [CustomerSchemaMapper(record, self, self._target.reference_data) for record in records if record.get("id")]
        company_customers_mapping = {}

        for mapped_record in mapped_records:
            if not mapped_record.company:
                continue

            if mapped_record.company["id"] not in company_customers_mapping.keys():
                company_customers_mapping[mapped_record.company["id"]] = []
            
            company_customers_mapping[mapped_record.company["id"]].append(mapped_record.record["id"])

        existing_customers = []
        # make requests to get existing Customers for each company from Dynamics
        for company_id in company_customers_mapping:
            url_params = { "companyId": company_id }
            _, _, customers = self.dynamics_client.get_entities(self.name, url_params=url_params, ids=company_customers_mapping[company_id], expand="defaultDimensions")
            existing_customers += customers

        self.reference_data = {**self._target.reference_data, "Customers": existing_customers}

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
