from target_dynamics_v2.mappers.base_mappers import BaseMapper

class ItemSchemaMapper(BaseMapper):
    name = "Items"
    existing_record_pk_mappings = [
        {"record_field": "id", "dynamics_field": "id", "required_if_present": True},
        {"record_field": "displayName", "dynamics_field": "displayName", "required_if_present": False}
    ]
    
    field_mappings = {
        "displayName": "displayName",
        "unitPrice": "unitPrice",
        "email": "email",
        "website": "website"
    }

    def to_dynamics(self) -> dict:
        self._validate_company()

        payload = {
            **self._map_internal_id(),
        }
 
        self._map_fields(payload)

        return payload
