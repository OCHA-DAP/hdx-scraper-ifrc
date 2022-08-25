#!/usr/bin/python
"""
IFRC:
----

Reads IFRC data and creates datasets.

"""
import logging
from copy import deepcopy

from hdx.data.dataset import Dataset
from hdx.location.country import Country
from slugify import slugify

logger = logging.getLogger(__name__)


class IFRC:
    def __init__(self, configuration):
        self.configuration = configuration
        self.base_url = self.configuration["base_url"]
        self.get_params = self.configuration["get_params"]

    def get_appealdata(self, retriever):
        dataset_info = self.configuration["appeal"]
        appeal_path = dataset_info["url_path"]
        url = f"{self.base_url}{appeal_path}{self.get_params}csv"
        _, iterator = retriever.get_tabular_rows(
            url,
            filename=dataset_info["download_path"],
            format="csv",
            headers=1,
            dict_form=True,
        )
        rows = list()
        rows_by_country = dict()
        for row in iterator:
            rows.append(row)
            countryiso = row["country.iso3"]
            row["country.name"] = Country.get_country_name_from_iso3(countryiso)
            if countryiso not in rows_by_country:
                rows_by_country[countryiso] = list()
            rows_by_country[countryiso].append(row)
        return rows, rows_by_country

    def get_whowhatwheredata(self, retriever):
        dataset_info = self.configuration["whowhatwhere"]
        whowhatwhere_path = dataset_info["url_path"]
        url = f"{self.base_url}{whowhatwhere_path}{self.get_params}json"
        json = retriever.download_json(url, filename=dataset_info["download_path"])
        rows = list()
        rows_by_country = dict()
        for info in json["results"]:
            countryiso = info["project_country_detail"]["iso3"]
            countryname = Country.get_country_name_from_iso3(countryiso)
            societyname = info["reporting_ns_detail"]["society_name"]
            primary_sector = info["primary_sector_display"]
            secondary_sectors = ", ".join(info["secondary_sectors_display"])
            programme_type = info["programme_type_display"]
            operation_type = info["operation_type_display"]
            status = info["status_display"]
            start_date = info["start_date"]
            end_date = info["end_date"]
            budget_amount = info["budget_amount"]
            actual_expenditure = info["actual_expenditure"]
            target_male = info["target_male"]
            target_female = info["target_female"]
            target_other = info["target_other"]
            target_total = info["target_total"]
            reached_male = info["reached_male"]
            reached_female = info["reached_female"]
            reached_other = info["reached_other"]
            reached_total = info["reached_total"]
            name = info["name"]
            base_row = {
                "country.iso": countryiso,
                "country.name": countryname,
                "country.society_name": societyname,
                "primary_sector": primary_sector,
                "secondary_sectors": secondary_sectors,
                "programme_type": programme_type,
                "operation_type": operation_type,
                "status": status,
                "start_date": start_date,
                "end_date": end_date,
                "budget_amount": budget_amount,
                "actual_expenditure": actual_expenditure,
                "target_male": target_male,
                "target_female": target_female,
                "target_other": target_other,
                "target_total": target_total,
                "reached_male": reached_male,
                "reached_female": reached_female,
                "reached_other": reached_other,
                "reached_total": reached_total,
                "name": name,
            }
            if countryiso not in rows_by_country:
                rows_by_country[countryiso] = list()
            for district_detail in info["project_districts_detail"]:
                row = deepcopy(base_row)
                row["district.name"] = district_detail["name"]
                rows.append(row)
                rows_by_country[countryiso].append(row)
        return rows, rows_by_country

    def generate_dataset(
        self,
        folder,
        rows,
        dataset_type,
        countryiso=None,
        global_dataset_url=None,
    ):
        """ """
        dataset_info = self.configuration[dataset_type]
        heading = dataset_info["heading"]
        global_name = f"Global IFRC {heading} Data"
        if countryiso is not None:
            rows = rows.get(countryiso)
            countryname = Country.get_country_name_from_iso3(countryiso)
            if countryname is None:
                logger.error(f"Unknown ISO 3 code {countryiso}!")
                return None
            title = f"{countryname} - IFRC {heading}"
            name = f"IFRC {heading} Data for {countryname}"
            filename = f"{heading.lower()}_data_{countryiso}.csv"
            notes = f"There is also a [global dataset]({global_dataset_url})."
        else:
            title = f"Global - IFRC {heading}"
            name = global_name
            filename = f"{heading.lower()}_data_global.csv"
            notes = f"This data can also be found as individual country datasets on HDX."

        if rows is None:
            return None

        logger.info(f"Creating dataset: {title}")
        dataset = Dataset(
            {
                "name": slugify(name).lower(),
                "title": title,
                "notes": notes,
            }
        )
        dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
        dataset.set_organization("3ada79f1-a239-4e09-bb2e-55743b7e6b69")
        dataset.set_expected_update_frequency("Every week")
        dataset.set_subnational(False)
        if countryiso:
            dataset.add_country_location(countryiso)
        else:
            dataset.add_other_location("world")

        dataset.add_tag("hxl")
        dataset.add_tags(dataset_info["tags"])

        resourcedata = {
            "name": name,
            "description": f"IFRC {heading} data with HXL tags",
        }

        success, results = dataset.generate_resource_from_iterator(
            list(rows[0].keys()),
            rows,
            dataset_info["hxltags"],
            folder,
            filename,
            resourcedata,
            "start_date",
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None, None
        return dataset
