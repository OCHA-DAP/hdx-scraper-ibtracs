#!/usr/bin/python
"""ibtracs scraper"""

import logging
from typing import List, Optional

import geopandas
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.dateparse import parse_date
from hdx.utilities.retriever import Retrieve
from pandas import concat, read_csv
from shapely.validation import make_valid

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
        if countryiso3 == "world":
            dataset_name = self._configuration["dataset_names"][countryiso3]
            dataset_title = self._configuration["dataset_titles"][countryiso3]
        else:
            country_name = Country.get_country_name_from_iso3(countryiso3)
            dataset_name = self._configuration["dataset_names"]["country"]
            dataset_title = self._configuration["dataset_titles"]["country"]
            dataset_name = dataset_name.format(iso=countryiso3.lower())
            dataset_title = dataset_title.format(country=country_name)
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
        dates = list(set(ibtracs_df["ISO_TIME"][1:]))
        dates = [parse_date(d) for d in dates]
        start_year = min(dates).year
        dataset.set_time_period(
            startdate=min(dates),
            enddate=max(dates),
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
        logger.info("Downloading global boundary")
        global_boundary = self.download_global_boundary()
        global_data = self.data["world"][1:]
        geo_df = geopandas.GeoDataFrame(
            global_data,
            geometry=geopandas.points_from_xy(global_data.LON, global_data.LAT),
            crs="EPSG:4326",
        )
        geo_df = geo_df.to_crs(crs="ESRI:54009")

        logger.info("Processing countries")
        for iso3 in global_boundary["ISO_3"]:
            if iso3[0] == "X":
                continue
            country_lyr = global_boundary[global_boundary["ISO_3"] == iso3]
            country_lyr.loc[:, "geometry"] = country_lyr.geometry.buffer(
                distance=2000000
            )
            joined_lyr = geopandas.overlay(geo_df, country_lyr, how="intersection")
            if len(joined_lyr) == 0:
                continue
            sid_list = joined_lyr["SID"].unique()
            country_data = self.data["world"][self.data["world"]["SID"].isin(sid_list)]
            country_data = concat([self.data["world"][0:1], country_data])
            self.data[iso3] = country_data

        return list(self.data.keys())

    def download_global_boundary(self):
        dataset_info = self._configuration["global_boundaries"]
        dataset = Dataset.read_from_hdx(dataset_info["dataset"])
        resource = [
            r for r in dataset.get_resources() if r["name"] == dataset_info["resource"]
        ][0]
        if self._retriever.use_saved:
            file_path = self._retriever.download_file(
                resource["url"], filename=resource["name"]
            )
        else:
            folder = (
                self._retriever.saved_dir if self._retriever.save else self._temp_dir
            )
            _, file_path = resource.download(folder)
        lyr = geopandas.read_file(file_path)
        lyr = lyr.to_crs(crs="ESRI:54009")
        for i, row in lyr.iterrows():
            if not lyr.geometry[i].is_valid:
                lyr.loc[i, "geometry"] = make_valid(lyr.geometry[i])
            if row["STATUS"] and row["STATUS"][:4] == "Adm.":
                lyr.loc[i, "ISO_3"] = row["Color_Code"]
        lyr = lyr.dissolve(by="ISO_3", as_index=False)
        lyr = lyr.drop(
            [f for f in lyr.columns if f.lower() not in ["iso_3", "geometry"]],
            axis=1,
        )
        return lyr
