from os.path import join

from hdx.utilities.compare import assert_files_same
from hdx.utilities.downloader import Download
from hdx.utilities.path import temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.ibtracs.ibtracs import Ibtracs


class TestIbtracs:
    def test_ibtracs(
        self, configuration, read_dataset, fixtures_dir, input_dir, config_dir
    ):
        with temp_dir(
            "Test_ibtracs",
            delete_on_success=True,
            delete_on_failure=False,
        ) as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader=downloader,
                    fallback_dir=tempdir,
                    saved_dir=input_dir,
                    temp_dir=tempdir,
                    save=False,
                    use_saved=True,
                )
                ibtracs = Ibtracs(configuration, retriever, tempdir)
                ibtracs.get_data()
                countryiso3s = ibtracs.process_countries()
                assert countryiso3s == ["world", "CUB", "JAM"]
                dataset = ibtracs.generate_dataset("world")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "ibtracs-global-tropical-storm-tracks",
                    "title": "IBTrACS: Global Storm Tracks",
                    "tags": [
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "world"}],
                    "dataset_date": "[1842-10-25T00:00:00 TO 2074-12-01T23:59:59]",
                    "license_id": "cc-by-igo",
                    "methodology": "Other",
                    "methodology_other": "Please find the methodology at [this link](https://www.ncei.noaa.gov/sites/g/files/anmtlf171/files/2024-07/IBTrACS_version4r01_Technical_Details.pdf)",
                    "caveats": "Some early (pre-1950) storms were not correctly matched, so the number of storms in the record is artificially high. For example, SIO storms in 1901 are not matched, so the same storm is tracked by the following identifiers: ds824, td9636, and reunion. This storm is broken into different tracks because of temporal differences in the storm data.",
                    "dataset_source": "National Oceanic and Atmospheric Administration (NOAA) / National Centers for Environmental Information (NCEI)",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "2f9fd160-2a16-49c0-89d6-0bc3230599bf",
                    "owner_org": "hdx",
                    "data_update_frequency": 7,
                    "notes": "The International Best Track Archive for Climate Stewardship (IBTrACS) project is the most complete global collection of tropical cyclones available. It merges recent and historical tropical cyclone data from multiple agencies to create a unified, publicly available, best-track dataset that improves inter-agency comparisons. \n \n Fields available: \n SID: A unique storm identifier (SID) assigned by IBTrACS algorithm.\n ISO_TIME: Time of the observation in ISO format (YYYY-MM-DD hh:mm:ss) \n BASIN: Basin of the current storm position \n SUBBASIN: Sub-basin of the current storm position \n NATURE: Type of storm (a combination of the various types from the available sources) \n NUMBER: Number of the storm for the year (restarts at 1 for each year \n LAT: Mean position - latitude (a combination of the available positions) \n LON: Mean position - longitude (a combination of the available positions) \n WMO_WIND: Maximum sustained wind speed assigned by the responsible WMO agency \n WMO_PRES: Minimum central pressure assigned by the responsible WMO agency.",
                    "subnational": "0",
                }

                resources = dataset.get_resources()
                assert len(resources) == 3
                assert resources[0] == {
                    "name": "ibtracs_ALL_list_v04r01.csv",
                    "description": "IBTrACS storm tracks from 1842 to date.",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                assert_files_same(
                    join(fixtures_dir, "ibtracs_ALL_list_v04r01.csv"),
                    join(tempdir, "ibtracs_ALL_list_v04r01.csv"),
                )
                assert_files_same(
                    join(fixtures_dir, "ibtracs_ALL_list_v04r01_lines.geojson"),
                    join(tempdir, "ibtracs_ALL_list_v04r01_lines.geojson"),
                )

                dataset = ibtracs.generate_dataset("CUB")
                dataset.update_from_yaml(
                    path=join(config_dir, "hdx_dataset_static.yaml")
                )
                assert dataset == {
                    "name": "cub-ibtracs-tropical-storm-tracks",
                    "title": "Cuba: IBTrACS Storm Tracks",
                    "tags": [
                        {
                            "name": "cyclones-hurricanes-typhoons",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                        {
                            "name": "hxl",
                            "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
                        },
                    ],
                    "groups": [{"name": "cub"}],
                    "dataset_date": "[1853-08-03T00:00:00 TO 2051-12-01T23:59:59]",
                    "license_id": "cc-by-igo",
                    "methodology": "Other",
                    "methodology_other": "Please find the methodology at [this link](https://www.ncei.noaa.gov/sites/g/files/anmtlf171/files/2024-07/IBTrACS_version4r01_Technical_Details.pdf)",
                    "caveats": "Some early (pre-1950) storms were not correctly matched, so the number of storms in the record is artificially high. For example, SIO storms in 1901 are not matched, so the same storm is tracked by the following identifiers: ds824, td9636, and reunion. This storm is broken into different tracks because of temporal differences in the storm data.",
                    "dataset_source": "National Oceanic and Atmospheric Administration (NOAA) / National Centers for Environmental Information (NCEI)",
                    "package_creator": "HDX Data Systems Team",
                    "private": False,
                    "maintainer": "2f9fd160-2a16-49c0-89d6-0bc3230599bf",
                    "owner_org": "hdx",
                    "data_update_frequency": 7,
                    "notes": "The International Best Track Archive for Climate Stewardship (IBTrACS) project is the most complete global collection of tropical cyclones available. It merges recent and historical tropical cyclone data from multiple agencies to create a unified, publicly available, best-track dataset that improves inter-agency comparisons. \n \n Fields available: \n SID: A unique storm identifier (SID) assigned by IBTrACS algorithm.\n ISO_TIME: Time of the observation in ISO format (YYYY-MM-DD hh:mm:ss) \n BASIN: Basin of the current storm position \n SUBBASIN: Sub-basin of the current storm position \n NATURE: Type of storm (a combination of the various types from the available sources) \n NUMBER: Number of the storm for the year (restarts at 1 for each year \n LAT: Mean position - latitude (a combination of the available positions) \n LON: Mean position - longitude (a combination of the available positions) \n WMO_WIND: Maximum sustained wind speed assigned by the responsible WMO agency \n WMO_PRES: Minimum central pressure assigned by the responsible WMO agency.",
                    "subnational": "0",
                }

                resources = dataset.get_resources()
                assert len(resources) == 3
                assert resources[0] == {
                    "name": "ibtracs_ALL_list_v04r01_CUB.csv",
                    "description": "IBTrACS storm tracks from 1853 to date.",
                    "format": "csv",
                    "resource_type": "file.upload",
                    "url_type": "upload",
                }

                assert_files_same(
                    join(fixtures_dir, "ibtracs_ALL_list_v04r01_CUB.csv"),
                    join(tempdir, "ibtracs_ALL_list_v04r01_CUB.csv"),
                )
                assert_files_same(
                    join(fixtures_dir, "ibtracs_ALL_list_v04r01_lines_CUB.geojson"),
                    join(tempdir, "ibtracs_ALL_list_v04r01_lines_CUB.geojson"),
                )
