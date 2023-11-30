"""DynamicsV2 target sink class, which handles writing streams."""
from target_dynamics_v2.mapping import UnifiedMapping

from target_dynamics_v2.client import DynamicsSink


class ContactsSink(DynamicsSink):
    """DynamicsV2 target sink class."""
    name = "Contacts"
    endpoint = "contacts"
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record,"contacts",self.target_name)
        self.entityid = "contactid"
        #@TODO check why none of documented values are working
        # status = record.get("active",True)
        # if status:
        #     payload.update({"statuscode":"0"})
        # else:
        #     payload.update({"statuscode":"1"})
        return payload

class CustomersSink(ContactsSink):
    name = "Customers"

class OpportunitiesSink(DynamicsSink):
    """DynamicsV2 target sink class."""
    name = "Opportunities"
    endpoint = "opportunities"
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record,"opportunity",self.target_name)
        self.entityid = "opportunityid"
        return payload

