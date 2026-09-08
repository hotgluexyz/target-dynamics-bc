import vcr
import os
from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetDynamicsBCSmoke(VCRTargetTestRunner):
    
    def module(self) -> str:
        return "target_dynamics_bc"

    def launch(self):
        from target_dynamics_bc.target import TargetDynamicsV2
        TargetDynamicsV2.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        # Legacy test-framework used body matching for OData batch requests — keep this.
        my_vcr = vcr.VCR()
        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetDynamicsBCSmoke.main()