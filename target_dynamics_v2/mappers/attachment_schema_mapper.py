from target_dynamics_v2.mappers.base_mappers import BaseMapper
from target_dynamics_v2.utils import ReferenceData
import os
import base64

class AttachmentSchemaMapper(BaseMapper):
    name = "Attachments"
    existing_record_pk_mappings = [
        {"record_field": "id", "dynamics_field": "id", "required_if_present": True},
        {"record_field": "fileName", "dynamics_field": "fileName", "required_if_present": True},
        {"record_field": "parentId", "dynamics_field": "parentId", "required_if_present": True},
    ]

    def __init__(
            self,
            record,
            sink,
            reference_data
    ) -> None:
        self.record = record
        self.sink = sink
        self.input_path = sink.config.get("input_path", "")
        self.reference_data: ReferenceData = reference_data
        self.company = self._map_company()
        self.existing_record = self._find_existing_record(reference_data.get("Bills"))


    def _find_existing_record(self, reference_list):
        """Finds an existing record in the reference data by matching internal.
        """
        if self.company is None:
            return None
        
        existing_bills = reference_list.get(self.company["id"], [])
        parent_bill = next(
            (bill for bill in existing_bills if bill["id"] == self.record.get("parentId")),
            None
        )

        if not parent_bill:
            return None
        

        existing_attachments = parent_bill.get("attachments", [])
        found_record = next(
            (attachment for attachment in existing_attachments
            if attachment["fileName"] == self.record.get("fileName")),
            None
        )

        return found_record
        

    field_mappings = {
        "fileName": "fileName"
    }

    def to_dynamics(self) -> dict:
        self._validate_company()

        payload = {
            **self._map_internal_id(),
            **self._map_attachment_content()
        }

        self._map_fields(payload)

        return {"payload": payload, "company_id": self.company["id"]}

    def _map_attachment_content(self):
        attachment_file_path = os.path.join(self.input_path, self.record.get("fileName"))
        if not os.path.exists(attachment_file_path):
            raise Exception(f"attachment file={attachment_file_path} does not exist")
        
        with open(attachment_file_path, "rb") as f:
            attachment_content = f.read()

        return {"attachmentContent": attachment_content}