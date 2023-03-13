#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.dateparse import iso_string_from_datetime, now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
from hdx.utilities.state import State
from ifrc import IFRC

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
    with State("last_run_date.txt", write_fn=iso_string_from_datetime) as state:
        with wheretostart_tempdir_batch(lookup) as info:
            folder = info["folder"]
            with Download() as downloader:
                retriever = Retrieve(
                    downloader, folder, "saved_data", folder, save, use_saved
                )
                ifrc = IFRC(configuration, retriever, state.get())
                ifrc.get_countries()
                (
                    appeal_rows,
                    appeal_country_rows,
                    appeal_qc_status,
                ) = ifrc.get_appealdata()
                countries_list = []
                if appeal_country_rows:
                    countries_list.append(set(appeal_country_rows))
                (
                    whowhatwhere_rows,
                    whowhatwhere_country_rows,
                    whowhatwhere_qc_status,
                ) = ifrc.get_whowhatwheredata()
                if whowhatwhere_country_rows:
                    countries_list.append(set(whowhatwhere_country_rows))

                countries = set().union(*countries_list)
                countries = [{"iso3": x} for x in sorted(countries)]
                logger.info(f"Number of countries: {len(countries)}")

                def create_dataset(
                    dataset, showcase, dataset_path, resource_view_path, qcstatus=None
                ):
                    if not dataset:
                        return
                    notes = f"\n\n{dataset['notes']}"
                    dataset.update_from_yaml(dataset_path)
                    notes = f"{dataset['notes']}{notes}"
                    # ensure markdown has line breaks
                    dataset["notes"] = notes.replace("\n", "  \n")

                    if qcstatus is None:
                        findreplace = None
                    else:
                        countryiso = dataset.get_location_iso3s()[0]
                        qcstatus_country = qcstatus.get(countryiso)
                        if qcstatus_country is None:
                            findreplace = None
                        else:
                            findreplace = {"{{#status+name}}": qcstatus_country}
                    dataset.generate_resource_view(
                        path=resource_view_path, findreplace=findreplace
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

                appeals_dataset, showcase = ifrc.generate_dataset_and_showcase(
                    folder, appeal_rows, "appeals"
                )
                create_dataset(
                    appeals_dataset,
                    showcase,
                    join("config", "hdx_appeals_dataset.yml"),
                    join("config", "hdx_global_appeals_resource_view.yml"),
                )
                whowhatwhere_dataset, showcase = ifrc.generate_dataset_and_showcase(
                    folder,
                    whowhatwhere_rows,
                    "whowhatwhere",
                )
                create_dataset(
                    whowhatwhere_dataset,
                    showcase,
                    join("config", "hdx_whowhatwhere_dataset.yml"),
                    join("config", "hdx_global_whowhatwhere_resource_view.yml"),
                )
                for _, country in progress_storing_folder(info, countries, "iso3"):
                    countryiso = country["iso3"]
                    dataset, showcase = ifrc.generate_dataset_and_showcase(
                        folder,
                        appeal_country_rows,
                        "appeals",
                        countryiso,
                        appeals_dataset,
                    )
                    create_dataset(
                        dataset,
                        showcase,
                        join("config", "hdx_appeals_dataset.yml"),
                        join("config", "hdx_country_appeals_resource_view.yml"),
                        qcstatus=appeal_qc_status,
                    )
                    dataset, showcase = ifrc.generate_dataset_and_showcase(
                        folder,
                        whowhatwhere_country_rows,
                        "whowhatwhere",
                        countryiso,
                        whowhatwhere_dataset,
                    )
                    create_dataset(
                        dataset,
                        showcase,
                        join("config", "hdx_whowhatwhere_dataset.yml"),
                        join("config", "hdx_country_whowhatwhere_resource_view.yml"),
                        qcstatus=whowhatwhere_qc_status,
                    )
        state.set(now_utc())


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
