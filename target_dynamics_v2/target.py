"""DynamicsV2 target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th
from target_hotglue.target import TargetHotglue

from target_dynamics_v2.sinks import (
    ContactsSink,
    OpportunitiesSink,
)


class TargetDynamicsV2(TargetHotglue):
    """Sample target for DynamicsV2."""
    SINK_TYPES = [
        ContactsSink,
        OpportunitiesSink,
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
        self.dynamics_client = DynamicsClient(self.config)
        self.reference_data: ReferenceData = self.get_reference_data()

    def get_reference_data(self) -> ReferenceData:
        self.logger.info(f"Getting reference data...")
        
        reference_data: ReferenceData = ReferenceData()
        _, _, companies = self.dynamics_client.get_companies()
        reference_data["companies"] = companies

        self.logger.info(f"Done getting reference data...")
        return reference_data

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