from target_dynamics_bc.mappers.base_mappers import BaseMapper
from target_dynamics_bc.mappers.bill_expense_item_schema_mapper import BillExpenseItemSchemaMapper
from target_dynamics_bc.mappers.bill_line_item_schema_mapper import BillLineItemSchemaMapper
from target_dynamics_bc.utils import RecordNotFound


class BillSchemaMapper(BaseMapper):
    name = "Bills"
    existing_record_pk_mappings = [
        {"record_field": "id", "dynamics_field": "id", "required_if_present": True},
        {"record_field": "transactionNumber", "dynamics_field": "number", "required_if_present": False},
        {"record_field": "billNumber", "dynamics_field": "vendorInvoiceNumber", "required_if_present": False},
        {"record_field": "vendorNumber", "dynamics_field": "vendorNumber", "required_if_present": False},
    ]

    field_mappings = {
        "billNumber": "vendorInvoiceNumber",
        "dueDate": "dueDate",
        "issueDate": "invoiceDate",
        "postingDate": "postingDate"
    }

    def _find_existing_record(self, reference_list):
        """Match existing bills by both invoice number AND vendor to prevent
        cross-vendor overwrites when two vendors share the same invoice number."""
        if self.company is None:
            return None

        existing_entities_in_dynamics = reference_list.get(self.company["id"], [])
        resolved_vendor_id = self._map_vendor(required=True).get("vendorId")

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            record_id = self.record.get(existing_record_pk_mapping["record_field"])
            if not record_id:
                continue

            is_id_field = existing_record_pk_mapping["record_field"] == "id"

            if is_id_field or not resolved_vendor_id:
                found_record = next(
                    (r for r in existing_entities_in_dynamics
                     if r[existing_record_pk_mapping["dynamics_field"]] == record_id),
                    None
                )
            else:
                found_record = next(
                    (r for r in existing_entities_in_dynamics
                     if r[existing_record_pk_mapping["dynamics_field"]] == record_id
                     and r.get("vendorId") == resolved_vendor_id),
                    None
                )

            if existing_record_pk_mapping["required_if_present"] and found_record is None:
                raise RecordNotFound(
                    f"Record {existing_record_pk_mapping['record_field']}={record_id} not found Dynamics. Skipping it"
                )

            if found_record:
                return found_record

        return None

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
