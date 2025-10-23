#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""

import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.scraper.ifrc.pipeline import Pipeline
from hdx.utilities.dateparse import iso_string_from_datetime, now_utc, parse_date
from hdx.utilities.downloader import Download
from hdx.utilities.path import (
    progress_storing_folder,
    script_dir_plus_file,
    wheretostart_tempdir_batch,
)
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State

logger = logging.getLogger(__name__)

lookup = "hdx-scraper-ifrc"
updated_by_script = "HDX Scraper: IFRC"


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and create them in HDX

    Args:
        save (bool): Save downloaded data. Defaults to False.
        use_saved (bool): Use saved data. Defaults to False.

    Returns:
        None
    """

    configuration = Configuration.read()
    with State("last_run_date.txt", parse_date, iso_string_from_datetime) as state:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                now = now_utc()
                ifrc = Pipeline(configuration, retriever, now, state.get())
                ifrc.get_countries()
                (
                    appeal_rows,
                    appeal_country_rows,
                    appeal_quickcharts,
                    appeal_countries_to_update,
                ) = ifrc.get_appealdata()
                countries_list = []
                if appeal_countries_to_update:
                    countries_list.append(set(appeal_countries_to_update))
                (
                    whowhatwhere_rows,
                    whowhatwhere_country_rows,
                    whowhatwhere_quickcharts,
                    whowhatwhere_countries_to_update,
                ) = ifrc.get_whowhatwheredata()
                if whowhatwhere_countries_to_update:
                    countries_list.append(set(whowhatwhere_countries_to_update))

                countries = set().union(*countries_list)
                countries = [{"iso3": x} for x in sorted(countries)]
                logger.info(f"Number of countries: {len(countries)}")

                def create_dataset(
                    dataset,
                    showcase,
                    qc_resource,
                    dataset_path,
                    resource_view_path,
                    quickcharts,
                ):
                    if not dataset:
                        return
                    notes = f"\n\n{dataset['notes']}"
                    dataset.update_from_yaml(dataset_path)
                    notes = f"{dataset['notes']}{notes}"
                    # ensure markdown has line breaks
                    dataset["notes"] = notes.replace("\n", "  \n")

                    qcstatus = quickcharts.get("status_country")
                    if qcstatus is None:
                        findreplace = None
                    else:
                        countryiso = dataset.get_location_iso3s()[0]
                        qcstatus_country = qcstatus.get(countryiso)
                        if qcstatus_country is None:
                            findreplace = None
                        else:
                            findreplace = {"{{#status+name}}": qcstatus_country}
                    dataset.generate_quickcharts(
                        qc_resource, path=resource_view_path, findreplace=findreplace
                    )
                    dataset.create_in_hdx(
                        remove_additional_resources=True,
                        hxl_update=False,
                        updated_by_script=updated_by_script,
                        batch=info["batch"],
                    )

                    if showcase:
                        showcase.create_in_hdx()
                        showcase.add_dataset(dataset)

                if countries:
                    (
                        appeals_dataset,
                        showcase,
                        qc_resource,
                    ) = ifrc.generate_dataset_and_showcase(
                        folder, appeal_rows, "appeals", appeal_quickcharts
                    )
                    create_dataset(
                        appeals_dataset,
                        showcase,
                        qc_resource,
                        script_dir_plus_file(
                            join("config", "hdx_appeals_dataset.yaml"), main
                        ),
                        script_dir_plus_file(
                            join("config", "hdx_global_appeals_resource_view.yaml"),
                            main,
                        ),
                        appeal_quickcharts,
                    )
                    (
                        whowhatwhere_dataset,
                        showcase,
                        qc_resource,
                    ) = ifrc.generate_dataset_and_showcase(
                        folder,
                        whowhatwhere_rows,
                        "whowhatwhere",
                        whowhatwhere_quickcharts,
                    )
                    create_dataset(
                        whowhatwhere_dataset,
                        showcase,
                        qc_resource,
                        script_dir_plus_file(
                            join("config", "hdx_whowhatwhere_dataset.yaml"), main
                        ),
                        script_dir_plus_file(
                            join(
                                "config", "hdx_global_whowhatwhere_resource_view.yaml"
                            ),
                            main,
                        ),
                        whowhatwhere_quickcharts,
                    )
                for _, country in progress_storing_folder(info, countries, "iso3"):
                    countryiso = country["iso3"]
                    (
                        dataset,
                        showcase,
                        qc_resource,
                    ) = ifrc.generate_dataset_and_showcase(
                        folder,
                        appeal_country_rows,
                        "appeals",
                        appeal_quickcharts,
                        countryiso,
                        appeals_dataset,
                    )
                    create_dataset(
                        dataset,
                        showcase,
                        qc_resource,
                        script_dir_plus_file(
                            join("config", "hdx_appeals_dataset.yaml"), main
                        ),
                        script_dir_plus_file(
                            join("config", "hdx_country_appeals_resource_view.yaml"),
                            main,
                        ),
                        quickcharts=appeal_quickcharts,
                    )
                    dataset, showcase, qc_resource = ifrc.generate_dataset_and_showcase(
                        folder,
                        whowhatwhere_country_rows,
                        "whowhatwhere",
                        whowhatwhere_quickcharts,
                        countryiso,
                        whowhatwhere_dataset,
                    )
                    create_dataset(
                        dataset,
                        showcase,
                        qc_resource,
                        script_dir_plus_file(
                            join("config", "hdx_whowhatwhere_dataset.yaml"), main
                        ),
                        script_dir_plus_file(
                            join(
                                "config", "hdx_country_whowhatwhere_resource_view.yaml"
                            ),
                            main,
                        ),
                        quickcharts=whowhatwhere_quickcharts,
                    )
                else:
                    logger.info("Nothing to update!")

        state.set(now_utc())


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=lookup,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
