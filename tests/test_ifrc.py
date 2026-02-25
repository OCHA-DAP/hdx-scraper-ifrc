#!/usr/bin/python
"""
Unit tests for InterAction.

"""

from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.location.country import Country
from hdx.scraper.ifrc.pipeline import Pipeline
from hdx.utilities.compare import assert_files_same
from hdx.utilities.dateparse import parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve
from hdx.utilities.useragent import UserAgent


class TestIFRC:
    @pytest.fixture(scope="function")
    def configuration(self):
        Configuration._create(
            hdx_read_only=True,
            user_agent="test",
            project_config_yaml=join(
                "src", "hdx", "scraper", "ifrc", "config", "project_configuration.yaml"
            ),
        )
        UserAgent.set_global("test")
        Country.countriesdata(use_live=False)
        tags = ["funding"]
        Vocabulary._tags_dict = {tag: {"Action to Take": "ok"} for tag in tags}
        tags = [{"name": tag} for tag in tags]
        Vocabulary._approved_vocabulary = {
            "tags": tags,
            "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            "name": "approved",
        }
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
                ifrc = Pipeline(
                    configuration,
                    retriever,
                    parse_date("2023-03-01"),
                    parse_date("2023-02-01"),
                )
                ifrc.get_countries()
                (
                    appeal_rows,
                    appeal_country_rows,
                    appeal_countries_to_update,
                ) = ifrc.get_appealdata()
                (
                    whowhatwhere_rows,
                    whowhatwhere_country_rows,
                    whowhatwhere_countries_to_update,
                ) = ifrc.get_whowhatwheredata()
                assert len(appeal_rows) == 144
                assert len(appeal_country_rows["BDI"]) == 1
                assert len(appeal_countries_to_update) == 44
                assert whowhatwhere_rows is None
                assert whowhatwhere_country_rows is None

                countries = set(appeal_country_rows)
                countries.add("world")
                Locations.set_validlocations(
                    [{"name": x.lower(), "title": x.lower()} for x in countries]
                )

                (
                    appeals_dataset,
                    showcase,
                ) = ifrc.generate_dataset_and_showcase(folder, appeal_rows, "appeals")
                assert appeals_dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[1993-03-08T00:00:00 TO 2028-02-29T23:59:59]",
                    "groups": [{"name": "world"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "global-ifrc-appeals-data",
                    "notes": "This data can also be found as individual country datasets on HDX.",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global - IFRC Appeals",
                }
                resource = appeals_dataset.get_resource()
                assert resource == {
                    "description": "IFRC Appeals data",
                    "format": "csv",
                    "name": "Global IFRC Appeals Data",
                }
                filename = "appeals_data_global.csv"
                assert_files_same(
                    join(fixtures, filename), resource.get_file_to_upload()
                )
                assert showcase == {
                    "image_url": "https://avatars.githubusercontent.com/u/22204810?s=200&v=4",
                    "name": "global-ifrc-appeals-data-showcase",
                    "notes": "IFRC Go Dashboard of Appeals Data",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Global - IFRC Appeals showcase",
                    "url": "https://go.ifrc.org/",
                }

                dataset, showcase = ifrc.generate_dataset_and_showcase(
                    folder,
                    appeal_country_rows,
                    "appeals",
                    "BDI",
                    appeals_dataset,
                )
                assert dataset == {
                    "data_update_frequency": "7",
                    "dataset_date": "[2022-11-16T00:00:00 TO 2023-03-31T23:59:59]",
                    "groups": [{"name": "bdi"}],
                    "maintainer": "196196be-6037-4488-8b71-d786adf4c081",
                    "name": "ifrc-appeals-data-for-burundi",
                    "notes": "There is also a [global "
                    "dataset](https://stage.data-humdata-org.ahconu.org/dataset/global-ifrc-appeals-data).",
                    "owner_org": "3ada79f1-a239-4e09-bb2e-55743b7e6b69",
                    "subnational": "0",
                    "tags": [
                        {
                            "name": "funding",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "title": "Burundi - IFRC Appeals",
                }
                resource = dataset.get_resource()
                assert resource == {
                    "description": "IFRC Appeals data",
                    "format": "csv",
                    "name": "IFRC Appeals Data for Burundi",
                }
                filename = "appeals_data_bdi.csv"
                assert_files_same(
                    join(fixtures, filename), resource.get_file_to_upload()
                )
                assert showcase is None
