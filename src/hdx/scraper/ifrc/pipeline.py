#!/usr/bin/python
"""
IFRC:
----

Reads IFRC data and creates datasets.

"""

import logging
from copy import deepcopy

from dateutil.relativedelta import relativedelta
from slugify import slugify

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add

logger = logging.getLogger(__name__)


def flatten(data):
    new_data = {}
    for key, value in data.items():
        if not isinstance(value, dict):
            new_data[key] = value
        else:
            for k, v in value.items():
                new_data[f"{key}.{k}"] = v
    return new_data


class Pipeline:
    def __init__(self, configuration, retriever, now, last_run_date):
        self.configuration = configuration
        self.retriever = retriever
        self.base_url = self.configuration["base_url"]
        self.get_params = self.configuration["get_params"]
        self.now = now
        self.last_run_date = last_run_date
        self.iso3_to_id = {}

    def download_data(self, url, basename, add_rows_fn):
        rows = []
        rows_by_country = {}
        quickcharts = {}
        countries_to_update = {}
        i = 0
        while url:
            filename = basename.format(index=i)
            json = self.retriever.download_json(url, filename=filename)
            for row in json["results"]:
                add_rows_fn(
                    rows, rows_by_country, quickcharts, row, countries_to_update
                )
            url = json["next"]
            i += 1
        return rows, rows_by_country, quickcharts, countries_to_update

    def get_countries(self):
        dataset_info = self.configuration["countries"]
        country_path = dataset_info["url_path"]
        url = f"{self.base_url}{country_path}{self.get_params}"
        filename = dataset_info["filename"]

        def add_rows(rows, rows_by_country, quickcharts, row, countries_to_update):
            countryiso = row["iso3"]
            ifrc_id = row["id"]
            rows_by_country[countryiso] = ifrc_id

        _, self.iso3_to_id, _, _ = self.download_data(
            url, filename, add_rows_fn=add_rows
        )

    def get_appealdata(self):
        dataset_info = self.configuration["appeals"]
        publish = dataset_info["publish"]
        if not publish:
            return None, None, None, None
        appeal_path = dataset_info["url_path"]
        additional_params = dataset_info["additional_params"]
        url = f"{self.base_url}{appeal_path}{self.get_params}{additional_params}2020-01-01T00:00:00"
        filename = dataset_info["filename"]
        indicators = {}

        def add_row(rows, rows_by_country, quickcharts, row, countries_to_update):
            status = row["status"]
            if status == 3:  # Ignore Archived status
                return

            beneficiaries = row["num_beneficiaries"]
            row["initial_num_beneficiaries"] = beneficiaries
            del row["num_beneficiaries"]
            row = flatten(row)
            startdate = parse_date(row["start_date"])
            year_month = startdate.strftime("%Y-%m")
            monthly_indicators = indicators.get(year_month, {})
            countryiso = row["country.iso3"]
            if not countryiso:  # Ignore blank country
                logger.error(
                    f"Missing country iso3 for appeal with aid {row['aid']} and name {row['name']}!"
                )
                return
            updated_date = parse_date(row["real_data_update"])
            if updated_date > self.last_run_date:
                countries_to_update[countryiso] = True
            country_indicators = monthly_indicators.get(countryiso, {})
            if row["atype"] == 0:
                atype = "DREFs"
            else:
                atype = "Appeals"
            country_indicators_atype = country_indicators.get(atype, {})
            country_indicators_atype["number"] = (
                country_indicators_atype.get("number", 0) + 1
            )
            country_indicators_atype["funded"] = country_indicators_atype.get(
                "funded", 0
            ) + float(row["amount_funded"])
            country_indicators_atype["beneficiaries"] = (
                country_indicators_atype.get("beneficiaries", 0) + beneficiaries
            )
            country_indicators[atype] = country_indicators_atype
            monthly_indicators[countryiso] = country_indicators
            indicators[year_month] = monthly_indicators
            row["country.name"] = Country.get_country_name_from_iso3(countryiso)
            rows.append(row)
            dict_of_lists_add(rows_by_country, countryiso, row)

        rows, rows_by_country, quickcharts, countries_to_update = self.download_data(
            url, filename, add_rows_fn=add_row
        )
        oneyearago = self.now - relativedelta(years=1)
        last_year = oneyearago.year
        current_month = self.now.month
        tensyearsago = self.now - relativedelta(years=10)
        min_year = tensyearsago.year

        qcrows = []
        qcrows_by_country = {}
        for year_month in sorted(indicators):
            year, month = year_month.split("-")
            year = int(year)
            if year < min_year:
                continue
            month = int(month)
            if year == min_year and month < current_month:
                continue
            row = {"Year": f"{year}-01-01", "Year Month": f"{year_month}-01"}
            if year > last_year or (year == last_year and month > current_month):
                row["Last Year"] = "Y"
            else:
                row["Last Year"] = "N"
            monthly_indicators = indicators[year_month]
            global_indicators = {
                "DREFs": {"number": 0, "funded": 0, "beneficiaries": 0},
                "Appeals": {"number": 0, "funded": 0, "beneficiaries": 0},
            }
            for countryiso in sorted(monthly_indicators):
                country_indicators = monthly_indicators[countryiso]
                for atype in sorted(country_indicators):
                    country_indicators_atype = country_indicators[atype]
                    number = country_indicators_atype["number"]
                    global_indicators[atype]["number"] += number
                    beneficiaries = country_indicators_atype["beneficiaries"]
                    global_indicators[atype]["beneficiaries"] += beneficiaries
                    funded = country_indicators_atype["funded"]
                    global_indicators[atype]["funded"] += funded
                    country_row = deepcopy(row)
                    country_row["Appeal Type"] = atype
                    country_row["Number of Appeals"] = number
                    country_row["Funded"] = funded
                    country_row["Beneficiaries"] = beneficiaries
                    dict_of_lists_add(qcrows_by_country, countryiso, country_row)
            for atype in sorted(global_indicators):
                global_row = deepcopy(row)
                global_row["Appeal Type"] = atype
                global_row["Number of Appeals"] = global_indicators[atype]["number"]
                global_row["Funded"] = global_indicators[atype]["funded"]
                global_row["Beneficiaries"] = global_indicators[atype]["beneficiaries"]
                qcrows.append(global_row)

        quickcharts = {"rows": qcrows, "rows_by_country": qcrows_by_country}
        return rows, rows_by_country, quickcharts, countries_to_update

    def get_whowhatwheredata(self):
        dataset_info = self.configuration["whowhatwhere"]
        publish = dataset_info["publish"]
        if not publish:
            return None, None, None, None

        whowhatwhere_path = dataset_info["url_path"]
        additional_params = dataset_info["additional_params"]
        url = f"{self.base_url}{whowhatwhere_path}{self.get_params}{additional_params}{self.last_run_date}T00:00:00"
        filename = dataset_info["filename"]

        def add_row(rows, rows_by_country, quickcharts, row, countries_to_update):
            countryiso = row["project_country_detail"]["iso3"]
            countryname = Country.get_country_name_from_iso3(countryiso)
            district_names = ", ".join(
                [d["name"] for d in row["project_districts_detail"]]
            )
            societyname = row["reporting_ns_detail"]["society_name"]
            primary_sector = row["primary_sector_display"]
            secondary_sectors = ", ".join(row["secondary_sectors_display"])
            programme_type = row["programme_type_display"]
            operation_type = row["operation_type_display"]
            status_display = row["status_display"]
            start_date = row["start_date"]
            end_date = row["end_date"]
            budget_amount = row["budget_amount"]
            actual_expenditure = row["actual_expenditure"]
            target_male = row["target_male"]
            target_female = row["target_female"]
            target_other = row["target_other"]
            target_total = row["target_total"]
            reached_male = row["reached_male"]
            reached_female = row["reached_female"]
            reached_other = row["reached_other"]
            reached_total = row["reached_total"]
            name = row["name"]
            row = {
                "country.iso3": countryiso,
                "country.name": countryname,
                "district.names": district_names,
                "country.society_name": societyname,
                "primary_sector": primary_sector,
                "secondary_sectors": secondary_sectors,
                "programme_type": programme_type,
                "operation_type": operation_type,
                "status_display": status_display,
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
            rows.append(row)
            dict_of_lists_add(rows_by_country, countryiso, row)
            quickcharts = quickcharts.get("status_country", {})
            qc_status = quickcharts.get(countryiso)
            if qc_status is None or status_display == "Ongoing":
                quickcharts[countryiso] = status_display
            quickcharts["status_country"] = quickcharts

        return self.download_data(url, filename, add_rows_fn=add_row)

    def generate_dataset_and_showcase(
        self,
        folder,
        rows,
        dataset_type,
        quickcharts,
        countryiso=None,
        global_dataset=None,
    ):
        """ """
        if rows is None:
            return None, None, None
        dataset_info = self.configuration[dataset_type]
        heading = dataset_info["heading"]
        global_name = f"Global IFRC {heading} Data"
        if countryiso is not None:
            rows = rows.get(countryiso)
            countryname = Country.get_country_name_from_iso3(countryiso)
            if countryname is None:
                logger.error(f"Unknown ISO 3 code {countryiso}!")
                return None, None, None
            title = f"{countryname} - IFRC {heading}"
            name = f"IFRC {heading} Data for {countryname}"
            filename = f"{heading.lower()}_data_{countryiso.lower()}.csv"
            global_dataset_url = global_dataset.get_hdx_url()
            notes = f"There is also a [global dataset]({global_dataset_url})."
        else:
            title = f"Global - IFRC {heading}"
            name = global_name
            filename = f"{heading.lower()}_data_global.csv"
            notes = "This data can also be found as individual country datasets on HDX."

        logger.info(f"Creating dataset: {title}")
        slugified_name = slugify(name).lower()
        dataset = Dataset(
            {
                "name": slugified_name,
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

        tags = ["hxl"] + dataset_info["tags"]
        dataset.add_tags(tags)

        resourcedata = {
            "name": name,
            "description": f"IFRC {heading} data with HXL tags",
        }

        def process_date(row):
            start_date = parse_date(row["start_date"])
            end_date = parse_date(row["end_date"])
            society = row["country.society_name"]
            identifier = row.get("aid")
            if identifier:
                identifier = f"aid = {identifier}"
            else:
                identifier = f"country = {row['country.name']}"
            if end_date < start_date:
                logger.warning(f"End date < start date for {society} {identifier}")
                return None
            result = {}
            if start_date.year > 1900:
                result["startdate"] = start_date
            else:
                logger.warning(f"Start date year < 1900 for {society} {identifier}")
            if end_date.year > 1900:
                result["enddate"] = end_date
            else:
                logger.warning(f"End date year < 1900 for {society} {identifier}")
            return result

        success, results = dataset.generate_resource_from_iterator(
            list(rows[0].keys()),
            rows,
            dataset_info["hxltags"],
            folder,
            filename,
            resourcedata,
            date_function=process_date,
        )
        if success is False:
            logger.warning(f"{name} has no data!")
            return None, None, None

        if not countryiso:
            qcrows = quickcharts.get("rows")
        else:
            qcrows_by_country = quickcharts.get("rows_by_country", {})
            qcrows = qcrows_by_country.get(countryiso)
        if qcrows:
            resourcedata = {
                "name": name.replace("Data", "QuickCharts Data"),
                "description": f"IFRC {heading} QuickCharts data with HXL tags",
            }
            success, results = dataset.generate_resource_from_iterator(
                list(qcrows[0].keys()),
                qcrows,
                dataset_info["quickcharts_hxltags"],
                folder,
                f"qc_{filename}",
                resourcedata,
            )
        if success is False:
            qc_resource = None
        else:
            qc_resource = results["resource"]
        showcase_urls = dataset_info["showcase_urls"]
        if countryiso:
            showcase_url = showcase_urls.get("country")
            if showcase_url:
                showcase_url = showcase_url.format(id=self.iso3_to_id[countryiso])
        else:
            showcase_url = showcase_urls.get("global")
        if showcase_url:
            showcase = Showcase(
                {
                    "name": f"{slugified_name}-showcase",
                    "title": f"{title} showcase",
                    "notes": f"IFRC Go Dashboard of {heading} Data",
                    "url": showcase_url,
                    "image_url": "https://avatars.githubusercontent.com/u/22204810?s=200&v=4",
                }
            )
            showcase.add_tags(tags)
        else:
            showcase = None
        return dataset, showcase, qc_resource
