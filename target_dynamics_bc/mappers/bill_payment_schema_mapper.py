from target_dynamics_bc.mappers.base_mappers import BaseMapper
from target_dynamics_bc.utils import InvalidInputError, MissingField, RecordNotFound

class BillPaymentSchemaMapper(BaseMapper):
    name = "BillPayments"
    existing_record_pk_mappings = [
        {"record_field": "id", "dynamics_field": "id", "required_if_present": True},
        {"record_field": "paymentNumber", "dynamics_field": "documentNumber", "required_if_present": False}
    ]

    field_mappings = {
        "paymentNumber": "documentNumber",
        "transactionNumber": "lineNumber",
        "paymentDate": "postingDate",
        "amount": "amount"
    }

    def to_dynamics(self) -> dict:
        self._validate_company()
        self._validate_external_id()

        payload = {
            **self._map_internal_id(),
            **self._map_vendor(required=True),
            **self._map_bill(),
            **self._map_payment_journal(required=True),
            **self._map_dimension_set_lines()
        }

        self._map_fields(payload)

        return {"payload": payload, "company_id": self.company["id"]}

    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        if self.company is None:
            return None
        
        existing_entities_in_dynamics = reference_list.get(self.company["id"], [])
        payment_journal = self._map_payment_journal(required=True)
        payment_journal_id = payment_journal["journalId"]

        for existing_record_pk_mapping in self.existing_record_pk_mappings:
            if record_id := self.record.get(existing_record_pk_mapping["record_field"]):
                found_record = next(
                    (dynamics_record for dynamics_record in existing_entities_in_dynamics
                    if dynamics_record[existing_record_pk_mapping["dynamics_field"]] == record_id
                    and dynamics_record["journalId"] == payment_journal_id),
                    None
                )
                if existing_record_pk_mapping["required_if_present"] and found_record is None:
                    raise RecordNotFound(f"Record {existing_record_pk_mapping['record_field']}={record_id} not found Dynamics. Skipping it")
                
                if found_record:
                    return found_record
        
        return None

    def _validate_external_id(self):
        external_id = self.record.get("externalId")
        if not external_id:
            raise MissingField(f"The required field 'externalId' was not provided")

        if len(external_id) > 20:
            raise InvalidInputError(f"The length of externalId={external_id} should be less or equal to 20.")


    def _map_bill(self):
        found_bill = None
        bill_reference_data = self.reference_data.get("Bills", {}).get(self.company["id"], [])

        if bill_id := self.record.get("billId"):
            found_bill = next(
                (bill for bill in bill_reference_data
                if bill["id"] == bill_id),
                None
            )

        if (bill_number := self.record.get("billNumber")) and not found_bill:
            found_bill = next(
                (bill for bill in bill_reference_data
                if bill["vendorInvoiceNumber"] == bill_number),
                None
            )

        if (bill_invoice_number := self.record.get("billExternalId")) and not found_bill:
            found_bill = next(
                (bill for bill in bill_reference_data
                if bill["vendorInvoiceNumber"] == bill_invoice_number),
                None
            )

        if bill_id is None and bill_invoice_number is None and bill_number is None:
            raise InvalidInputError(f"Bill not informed. Please provide one of billId / billNumber / billExternalId")

        if not found_bill:
            raise RecordNotFound(f"Bill not found for billId={bill_id} / billNumber={bill_number} / billExternalId={bill_invoice_number}")


        return { "appliesToInvoiceId": found_bill["id"] }
