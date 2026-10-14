"""Colocated smoke-test runner for target-dynamics-bc (hotglue-smoke-test)."""

import json
import os
import re

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner

_BC_API_ENV_IN_URI = re.compile(
    r"(https://api\.businesscentral\.dynamics\.com/v2\.0/)[^/\"]+(/api/v2\.0/)"
)


class TargetDynamicsV2TestRunner(VCRTargetTestRunner):
    # PRESERVE_KEYS = JSON property names whose string values must not be faked
    # (see hotglue_smoke_test.vcr.target_scrub). Use this for Singer + BC fields
    # that carry dimension/reference codes — more maintainable than listing every
    # value in PRESERVE_VALUES (e.g. MH, CLASS01, PURCHASER).
    # externalId* fields are integration ids (not PII); BC bill payments also enforce
    # len(externalId) <= 20, so they must not become -Fallback-scrubbed-… literals.
    PRESERVE_KEYS = {
        "grant_type",
        "method",
        "token_type",
        "url",
        "billExternalId",
        "billNumber",
        "externalId",
        "accountNumber",
        "journalEntryNumber",
        "journalExternalId",
        "paymentNumber",
        "paymentMethod",
        "customerNumber",
        "type",
        "classNumber",
        "code",
        "departmentNumber",
        "entryType",
        "itemNumber",
        "lineType",
        "locationNumber",
        "number",
        "subsidiaryName",
        "taxCode",
        "valueCode",
        "valueNumber",
        "vendorNumber",
        "vendorInvoiceNumber",
        "documentNumber",
        "displayName",
        "itemId",
    }
    # PRESERVE_VALUES = specific literals that still appear outside those keys
    # (embedded text, display names, address type enums, etc.).
    PRESERVE_VALUES = {
        "Credit",
        "CRONUS USA, Inc.",
        "Debit",
        # BC API path segment (config environment_name); also a DEPARTMENT dimension label.
        "Production",
        "Sandbox",
        "billing",
        "home",
        "invalid",
        "mobile",
        "shipping",
        "unknown",
        # BC $batch sub-request ids for correlating line + dimension writes on create
        "temp_0",
        "temp_1",
        "temp_2",
        "temp_3",
        "GENERAL",
        "Draft",
        "ACCOUNT",
        "Company",
        "Person",
        "All",
    }

    def module(self) -> str:
        return "target_dynamics_bc"

    def launch(self):
        from target_dynamics_bc.target import TargetDynamicsV2

        TargetDynamicsV2.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        # BC batches OData in JSON bodies; default VCR matching is too loose for replay.
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
        )


if __name__ == "__main__":
    TargetDynamicsV2TestRunner.main()
