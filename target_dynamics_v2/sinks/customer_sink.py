from typing import List
from target_dynamics_v2.mappers.customer_schema_mapper import CustomerSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class CustomerSink(DynamicsBaseBatchSink):
    name = "Customers"

    def preprocess_batch(self, records: List[dict]):
        # we need to map the company to query the existing customers
        mapped_records = [CustomerSchemaMapper(record, self.name, self._target.reference_data) for record in records if record.get("id")]
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
            _, _, customers = self.dynamics_client.get_reference_data("customers", url_params=url_params, ids=company_customers_mapping[company_id])
            existing_customers += customers

        self.reference_data = {**self._target.reference_data, "Customers": existing_customers}

    def process_batch_record(self, record: dict, index: int) -> dict:
        # perform the mapping
        return CustomerSchemaMapper(record, self.name, self.reference_data).to_dynamics()
