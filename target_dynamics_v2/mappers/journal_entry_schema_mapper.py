import hashlib
from target_dynamics_v2.mappers.base_mappers import BaseMapper
from target_dynamics_v2.mappers.journal_entry_line_schema_mapper import JournalEntryLineSchemaMapper
from target_dynamics_v2.utils import InvalidInputError, MissingField, RecordNotFound

class JournalEntrySchemaMapper(BaseMapper):
    name = "Journals"
    existing_record_pk_mappings = [
        {"record_field": "journalEntryNumber", "dynamics_field": "displayName", "required_if_present": False}
    ]

    field_mappings = {
        "journalEntryNumber": "displayName"
    }

    def to_dynamics(self) -> dict:
        self._validate_company()
        self._validate_journal_entry_number()
        self._validate_transaction_date()

        payload = {
            **self._map_internal_id(),
            **self._map_journal_entry_lines()
        }

        self._map_fields(payload)

        
        payload["code"] = hashlib.sha256(payload["displayName"].encode()).hexdigest()[:10]

        return {
            "payload": payload,
            "is_draft": self.record.get("isDraft", False),
            "company_id": self.company["id"]
        }

    def _validate_journal_entry_number(self):
        journal_entry_number = self.record.get("journalEntryNumber")
        if not journal_entry_number:
            raise MissingField(f"The required field 'externalId' was not provided")

        if len(journal_entry_number) > 20:
            raise InvalidInputError(f"The length of externalId={journal_entry_number} should be less or equal to 20.")

    def _validate_transaction_date(self):
        if not self.record.get("transactionDate"):
            raise MissingField(f"The required field 'transactionDate' was not provided")

    def _map_journal_entry_lines(self):
        lines = []
        transaction_date = self.record.get("transactionDate")

        if not self.record.get("lineItems", []):
            raise MissingField(f"The required field 'lineItems' was not provided")

        lines_amount_sum = 0
        for item in self.record.get("lineItems", []):
            item.update(
                {
                    "subsidiaryId": self.record.get("subsidiaryId"),
                    "subsidiaryName": self.record.get("subsidiaryName"),
                    "transactionDate": transaction_date,
                    "documentNumber": self.record.get("journalEntryNumber")
                }
            )

            mapped_line_item = JournalEntryLineSchemaMapper(item, self.sink, self.reference_data).to_dynamics()
            lines_amount_sum += mapped_line_item["amount"]
            lines.append(mapped_line_item)

        if lines_amount_sum != 0:
            raise InvalidInputError(f"The Journal is out of balance by {lines_amount_sum}. Please check that the Amount is correct for each line")

        return {"journalLines": lines}
