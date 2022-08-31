#!/usr/bin/python
"""
Unit tests for InterAction.

"""
from os.path import join

import pytest
from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.location.country import Country
from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent
from ifrc import IFRC


class TestIFRC:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join("config", "project_configuration.yml"),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        return Configuration.read()

    @pytest.fixture(scope="function")
    def fixtures(self):
        return join("tests", "fixtures")

    @pytest.fixture(scope="function")
    def input_folder(self, fixtures):
        return join(fixtures, "input")

    def test_generate_datasets_and_showcases(
        self, configuration, fixtures, input_folder
    ):
        with temp_dir(
            "test_ifrc", delete_on_success=True, delete_on_failure=False
        ) as folder:
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, input_folder, folder, False, True
                )
                ifrc = IFRC(configuration)
                appeal_rows, appeal_country_rows = ifrc.get_appealdata(retriever)
                (
                    whowhatwhere_rows,
                    whowhatwhere_country_rows,
                ) = ifrc.get_whowhatwheredata(retriever)
                assert len(appeal_rows) == 5
                assert len(appeal_country_rows["MRT"]) == 2
                assert len(whowhatwhere_rows) == 16
                assert len(whowhatwhere_country_rows["ARM"]) == 11

                countries = set(appeal_country_rows).union(
                    set(whowhatwhere_country_rows)
                )
                countries.add("world")
                Locations.set_validlocations(
                    [{"name": x.lower(), "title": x.lower()} for x in countries]
                )

                appeals_dataset = ifrc.generate_dataset(folder, appeal_rows, "appeal")
                assert appeals_dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[2022-07-01T00:00:00 TO 2022-12-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-ifrc-appeals-data",
                    "notes": "This data can also be found as individual country datasets on HDX.",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aid funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global - IFRC Appeals",
                }
                resources = appeals_dataset.get_resources()
                assert len(resources) == 1
                resource = resources[0]
                assert resource == {
                    "description": "IFRC Appeals data with HXL tags",
                    "format": "csv",
                    "name": "Global IFRC Appeals Data",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                filename = "appeals_data_global.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)

                whowhatwhere_dataset = ifrc.generate_dataset(
                    folder,
                    whowhatwhere_rows,
                    "whowhatwhere",
                )
                assert whowhatwhere_dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[2018-06-01T00:00:00 TO 2022-07-31T23:59:59]",
                    "groups": [{"name": "world"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-ifrc-3w-data",
                    "notes": "This data can also be found as individual country datasets on HDX.",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "who is doing what and where - 3w - 4w - 5w",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global - IFRC 3W",
                }
                resources = whowhatwhere_dataset.get_resources()
                assert len(resources) == 1
                resource = resources[0]
                assert resource == {
                    "description": "IFRC 3W data with HXL tags",
                    "format": "csv",
                    "name": "Global IFRC 3W Data",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                filename = "3w_data_global.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)

                dataset = ifrc.generate_dataset(
                    folder,
                    appeal_country_rows,
                    "appeal",
                    "MRT",
                    appeals_dataset.get_hdx_url(),
                )
                assert dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[2022-07-01T00:00:00 TO 2022-12-31T23:59:59]",
                    "groups": [{"name": "mrt"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "ifrc-appeals-data-for-mauritania",
                    "notes": "There is also a [global dataset](https://stage.data-humdata-org.ahconu.org/dataset/global-ifrc-appeals-data).",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "aid funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Mauritania - IFRC Appeals",
                }
                resources = dataset.get_resources()
                assert len(resources) == 1
                resource = resources[0]
                assert resource == {
                    "description": "IFRC Appeals data with HXL tags",
                    "format": "csv",
                    "name": "IFRC Appeals Data for Mauritania",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                filename = "appeals_data_mrt.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)

                dataset = ifrc.generate_dataset(
                    folder,
                    whowhatwhere_country_rows,
                    "whowhatwhere",
                    "ARM",
                    whowhatwhere_dataset.get_hdx_url(),
                )
                assert dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[2021-12-27T00:00:00 TO 2022-04-30T23:59:59]",
                    "groups": [{"name": "arm"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "ifrc-3w-data-for-armenia",
                    "notes": "There is also a [global dataset](https://stage.data-humdata-org.ahconu.org/dataset/global-ifrc-3w-data).",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "who is doing what and where - 3w - 4w - 5w",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Armenia - IFRC 3W",
                }
                resources = dataset.get_resources()
                assert len(resources) == 1
                resource = resources[0]
                assert resource == {
                    "description": "IFRC 3W data with HXL tags",
                    "format": "csv",
                    "name": "IFRC 3W Data for Armenia",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }
                filename = "3w_data_arm.csv"
                assert_files_same(join(fixtures, filename), resource.file_to_upload)
