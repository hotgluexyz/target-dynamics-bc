from typing import List
from target_dynamics_v2.mappers.customer_schema_mapper import CustomerSchemaMapper
from target_dynamics_v2.sinks.base_sinks import DynamicsBaseBatchSink

class CustomerSink(DynamicsBaseBatchSink):
    name = "Customers"

    def preprocess_batch(self, records: List[dict]):
        self.reference_data = {**self._target.reference_data, "Customers": []}

    def process_batch_record(self, record: dict, index: int) -> dict:
        # perform the mapping
        return CustomerSchemaMapper(record, self.name, self.reference_data).to_dynamics()
