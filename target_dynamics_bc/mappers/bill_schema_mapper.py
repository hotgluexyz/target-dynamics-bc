from target_dynamics_bc.mappers.base_mappers import BaseMapper
from target_dynamics_bc.mappers.bill_expense_item_schema_mapper import BillExpenseItemSchemaMapper
from target_dynamics_bc.mappers.bill_line_item_schema_mapper import BillLineItemSchemaMapper


class BillSchemaMapper(BaseMapper):
    name = "Bills"
    existing_record_pk_mappings = [
        {"record_field": "id", "dynamics_field": "id", "required_if_present": True},
        {"record_field": "transactionNumber", "dynamics_field": "number", "required_if_present": False},
        {"record_field": "billNumber", "dynamics_field": "vendorInvoiceNumber", "required_if_present": False}
    ]

    field_mappings = {
        "billNumber": "vendorInvoiceNumber",
        "dueDate": "dueDate",
        "issueDate": "invoiceDate",
        "postingDate": "postingDate"
    }

    def to_dynamics(self) -> dict:
        self._validate_company()

        payload = {
            **self._map_internal_id(),
            **self._map_vendor(required=True),
            **self._map_currency(),
            **self._map_dimension_set_lines()
        }

        self._map_fields(payload)

        self._map_bill_line_items(payload)

        status = self.existing_record["status"] if self.existing_record else None

        return {
            "payload": payload,
            "id": payload.get("id"),
            "company_id": self.company["id"],
            "is_draft": self.record.get("isDraft", False),
            "status": status}

    def _map_bill_line_items(self, payload):
        mapped_line_items = []
        existing_lines = self.existing_record.get("purchaseInvoiceLines", []) if self.existing_record else []

        line_items = self.record.get("lineItems", [])
        for line_item in line_items:
            line_item["subsidiaryId"] = self.company["id"]
            line_payload = BillLineItemSchemaMapper(line_item, self.sink, self.reference_data, existing_lines).to_netsuite()
            mapped_line_items.append(line_payload)
        
        expense_items = self.record.get("expenses", [])
        for expense_item in expense_items:
            expense_item["subsidiaryId"] = self.company["id"]
            expense_line_payload = BillExpenseItemSchemaMapper(expense_item, self.sink, self.reference_data, existing_lines).to_netsuite()
            mapped_line_items.append(expense_line_payload)

        if mapped_line_items:
            payload["purchaseInvoiceLines"] = mapped_line_items
