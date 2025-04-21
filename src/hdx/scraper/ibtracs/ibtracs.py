#!/usr/bin/python
"""ibtracs scraper"""

import logging
from typing import List, Optional

from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from pandas import read_csv

logger = logging.getLogger(__name__)


class Ibtracs:
    def __init__(
        self, configuration: Configuration, retriever: Retrieve, temp_dir: str
    ):
        self._configuration = configuration
        self._retriever = retriever
        self._temp_dir = temp_dir
        self.data = {}

    def generate_dataset(self, countryiso3: str) -> Optional[Dataset]:
        dataset_name = self._configuration["dataset_names"][countryiso3]
        dataset_title = self._configuration["dataset_titles"][countryiso3]
        if countryiso3 != "world":
            dataset_name = dataset_name.format(iso=countryiso3)
            dataset_title = dataset_title.format(iso=countryiso3)
        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )
        dataset.add_tags(self._configuration["tags"])
        if countryiso3 == "world":
            dataset.add_other_location("world")
        else:
            dataset.add_country_location(countryiso3)
        ibtracs_df = self.data[countryiso3]
        ibtracs_dict = ibtracs_df.apply(lambda x: x.to_dict(), axis=1)
        start_date = min(ibtracs_df["ISO_TIME"][1:])
        start_year = start_date.year
        dataset.set_time_period(
            startdate=start_date,
            enddate=max(ibtracs_df["ISO_TIME"][1:]),
        )
        logger.info(
            f"Generating dataset {dataset.get_name_or_id()} from {len(ibtracs_df)} rows."
        )

        if countryiso3 == "world":
            resource_name = "ibtracs_ALL_list_v04r01.csv"
        else:
            resource_name = f"ibtracs_{countryiso3}_list_v04r01.csv"
        dataset.generate_resource_from_rows(
            headers=list(ibtracs_dict[0].keys()),
            rows=ibtracs_dict,
            folder=self._temp_dir,
            filename=resource_name,
            resourcedata={
                "name": resource_name,
                "description": f"IBTrACS storm tracks from {start_year} to date.",
            },
            encoding="utf-8",
        )

        # Subset with HXL tags sid, basin, year, nature
        hxl_tags = [
            "#id+code",
            "#region+name",
            "#region+name+subbasin",
            "#impact+type",
            "#date+year+reported",
        ]
        qc_df = ibtracs_df.loc[:, ["SID", "BASIN", "SUBBASIN", "ISO_TIME", "NATURE"]]
        qc_df["YEAR"] = qc_df["ISO_TIME"].str[:4]
        qc_df.drop(columns=["ISO_TIME"], inplace=True)
        qc_df.loc[qc_df.index[0]] = hxl_tags
        qc_df.drop_duplicates(inplace=True)
        qc_df_dict = qc_df.apply(lambda x: x.to_dict(), axis=1)

        dataset.generate_resource_from_rows(
            headers=list(qc_df_dict[0].keys()),
            rows=qc_df_dict,
            folder=self._temp_dir,
            filename="data_for_quickcharts.csv",
            resourcedata={
                "name": "data_for_quickcharts.csv",
                "description": f"Simplified quick charts data, without latitude or longitude, "
                f"with HXL tags from {start_year} to date.",
            },
            encoding="utf-8",
        )
        return dataset

    def get_data(self) -> None:
        try:
            csv_file = self._retriever.download_file(self._configuration.get("url"))
        except DownloadError:
            logger.error(f"Couldn't download {self._configuration.get('url')}")
            return

        ibtracs_df = read_csv(
            csv_file,
            sep=",",
            keep_default_na=False,
            usecols=self._configuration["columns_subset"],
            low_memory=False,
        )
        ibtracs_df = ibtracs_df.replace(
            {"NATURE": self._configuration["nature_mapping"]}
        )
        ibtracs_df = ibtracs_df.replace({"BASIN": self._configuration["basin_mapping"]})
        ibtracs_df = ibtracs_df.replace(
            {"SUBBASIN": self._configuration["subbasin_mapping"]}
        )
        self.data["world"] = ibtracs_df
        return

    def process_countries(self) -> List[str]:
        return ["world"]
