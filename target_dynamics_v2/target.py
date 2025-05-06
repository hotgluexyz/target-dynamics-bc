"""DynamicsV2 target class."""
import json
import os

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_dynamics_v2.client import DynamicsClient
from target_dynamics_v2.sinks.customer_sink import CustomerSink
from target_dynamics_v2.sinks.vendor_sink import VendorSink
from target_dynamics_v2.utils import ReferenceData, DimensionDefinitionNotFound, InvalidCustomFieldDefinition, InvalidConfigurationError

class TargetDynamicsV2(TargetHotglue):
    """Sample target for DynamicsV2."""
    SINK_TYPES = [
        CustomerSink,
        VendorSink
    ]
    name = "target-dynamics-v2"
    def __init__(
        self,
        config=None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None,
    ) -> None:
        self.config_file = config[0]
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.dynamics_client = DynamicsClient(self)
        self.reference_data: ReferenceData = self.get_reference_data()
        self.dimensions_mapping, self.fields_mapping = self.load_fields_and_dimensions_mapping_config()

    def get_reference_data(self) -> ReferenceData:
        self.logger.info(f"Getting reference data...")

        reference_data: ReferenceData = ReferenceData()
        _, _, companies = self.dynamics_client.get_companies()
        reference_data["companies"] = companies

        self.logger.info(f"Done getting reference data...")
        return reference_data

    def parse_field_mapping(self, type: str, field_mappings: dict):
        filtered_mapping = {}

        for sink, sink_field_map in field_mappings.items():
            sink_mapping = {field_name: field_config["name"] for field_name, field_config in sink_field_map.items() if field_config.get("type") == type}
            if sink_mapping:
                filtered_mapping[sink] = sink_mapping

        return filtered_mapping

    def validate_dimensions_mapping(self, dimensions_mapping: dict):
        # make a set of unique dimension names
        dimensions_names = set()
        for dimension_map in dimensions_mapping.values():
            for dimension_name in dimension_map.values():
                dimensions_names.add(dimension_name)

        # for every company check if the dimension exists
        for company in self.reference_data["companies"]:
            self.logger.info(f"Validating field -> dimension mapping for companyId={company['id']}")
            for dimension_name in dimensions_names:
                found_dimension = next((dimension for dimension in company["dimensions"] if dimension["code"] == dimension_name), None)

                if not found_dimension:
                    raise DimensionDefinitionNotFound(f"Could not find dimension={dimension_name} for companyId={company['id']}")

    def validate_fields_mapping(self, fields_mapping: dict):
        for sink in self.SINK_TYPES:
            override_fields_name = {field_name for field_name in fields_mapping.get(sink.name, {})}
            not_overridable_fields = override_fields_name - set(sink.allowed_fields_override)
            if not_overridable_fields:
                raise InvalidCustomFieldDefinition(f"Non-overridable fields provided in config for sink={sink.name}, fields={not_overridable_fields}")

    def get_tenant_config(self):
        snapshot_directory = self.config.get("snapshot_dir", None)
        tenant_config = None

        if snapshot_directory:
            config_path = os.path.join(snapshot_directory, "tenant-config.json")
            if not os.path.exists(config_path):
                raise InvalidConfigurationError(f"tenant-config.json does not exist in the snapshot directory={snapshot_directory}")
            with open(config_path) as f:
                tenant_config = json.load(f)
        else:
            tenant_config = {
                "dynamics_bc_field_mapping": {
                    "field_mappings": {
                    }
                }
            }

        return tenant_config

    def load_fields_and_dimensions_mapping_config(self):
        tenant_config = self.get_tenant_config()
        config = tenant_config.get("dynamics_bc")

        if config == None:
            raise InvalidConfigurationError("dynamics_bc_field_mapping is not provided in the tenant-config.json")

        dimensions_mapping = {}
        fields_mapping = {}
        if mappings := config.get("field_mappings"):
            dimensions_mapping = self.parse_field_mapping("dimension", mappings)
            fields_mapping = self.parse_field_mapping("field", mappings)

            self.validate_dimensions_mapping(dimensions_mapping)
            self.validate_fields_mapping(fields_mapping)

        return dimensions_mapping, fields_mapping

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True
        ),
        th.Property(
            "redirect_uri",
            th.StringType,
            required=True
        ),
        th.Property(
            "refresh_token",
            th.StringType,
            required=True
        ),

    ).to_dict()

if __name__ == '__main__':
    TargetDynamicsV2.cli()