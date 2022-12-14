#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from os.path import expanduser, join

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.path import progress_storing_folder, wheretostart_tempdir_batch
from hdx.utilities.retriever import Retrieve
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
    with wheretostart_tempdir_batch(lookup) as info:
        folder = info["folder"]
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved
            )
            ifrc = IFRC(configuration)
            appeal_rows, appeal_country_rows = ifrc.get_appealdata(retriever)
            whowhatwhere_rows, whowhatwhere_country_rows = ifrc.get_whowhatwheredata(
                retriever
            )
            countries = set(appeal_country_rows).union(set(whowhatwhere_country_rows))
            countries = [{"iso3": x} for x in sorted(countries)]
            logger.info(f"Number of countries: {len(countries)}")

            def create_dataset(dataset, path):
                if not dataset:
                    return
                notes = f"\n\n{dataset['notes']}"
                dataset.update_from_yaml(path)
                notes = f"{dataset['notes']}{notes}"
                # ensure markdown has line breaks
                dataset["notes"] = notes.replace("\n", "  \n")

                dataset.create_in_hdx(
                    remove_additional_resources=True,
                    hxl_update=False,
                    updated_by_script=updated_by_script,
                    batch=info["batch"],
                )

            appeals_dataset = ifrc.generate_dataset(folder, appeal_rows, "appeal")
            create_dataset(appeals_dataset, join("config", "hdx_appeals_dataset.yml"))
            whowhatwhere_dataset = ifrc.generate_dataset(
                folder,
                whowhatwhere_rows,
                "whowhatwhere",
            )
            create_dataset(
                whowhatwhere_dataset, join("config", "hdx_whowhatwhere_dataset.yml")
            )
            for _, country in progress_storing_folder(info, countries, "iso3"):
                countryiso = country["iso3"]
                dataset = ifrc.generate_dataset(
                    folder,
                    appeal_country_rows,
                    "appeal",
                    countryiso,
                    appeals_dataset.get_hdx_url(),
                )
                create_dataset(dataset, join("config", "hdx_appeals_dataset.yml"))
                dataset = ifrc.generate_dataset(
                    folder,
                    whowhatwhere_country_rows,
                    "whowhatwhere",
                    countryiso,
                    whowhatwhere_dataset.get_hdx_url(),
                )
                create_dataset(dataset, join("config", "hdx_whowhatwhere_dataset.yml"))


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yml"),
        user_agent_lookup=lookup,
        project_config_yaml=join("config", "project_configuration.yml"),
    )
