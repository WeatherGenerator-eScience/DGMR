"""
Download data from KNMI data portal.

Example download script:
https://developer.dataplatform.knmi.nl/open-data-api#example-last

Dataset info:
https://dataplatform.knmi.nl/group/precipitation?res_format=HDF5&tags=Radar
Relevant seem:
- radar_forecast
- radar_reflectivity_composites (one file every 5 minutes)
- radar_tar_refl_composites (archive one file per day)

I think Charlotte used one of these:
- nl_rdr_data_rtcor_5m (one file every 5 minutes)
- nl_rdr_data_rtcor_5m_tar (archive one file per day)
except that the files I got from there have "rt" in the filename instead of "5m".
"""

import logging
import os
from pathlib import Path
import sys
from dotenv import load_dotenv

import requests

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

DATAPATH = Path("~/weathergenerator/data").expanduser()


class OpenDataAPI:
    def __init__(self, api_token: str):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.headers = {"Authorization": api_token}

    def __get_data(self, url, params=None):
        return requests.get(url, headers=self.headers, params=params).json()

    def list_files(self, dataset_name: str, dataset_version: str, params: dict):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files",
            params=params,
        )

    def get_file_url(self, dataset_name: str, dataset_version: str, file_name: str):
        return self.__get_data(
            f"{self.base_url}/datasets/{dataset_name}/versions/{dataset_version}/files/{file_name}/url"
        )


def download_file_from_temporary_download_url(download_url, filename):
    try:
        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
    except Exception:
        logger.exception("Unable to download file using download URL")
        sys.exit(1)

    logger.info(f"Successfully downloaded dataset file to {filename}")


def get_token():
    """
    Get the KNMI API token, stored in .env as "KNMI_TOKEN".

    See https://developer.dataplatform.knmi.nl/open-data-api#token
    """
    load_dotenv()

    token = os.getenv("KDP_TOKEN")

    if token is None:
        logger.error("Failed to read KDP_TOKEN from environment variables.")
        sys.exit(1)

    return token


def main():
    # Key expires 1 July 2025
    api_key = get_token()
    dataset_name = "nl_rdr_data_rtcor_5m_tar"
    dataset_version = "1.0"

    logger.info(f"Fetching recent files of {dataset_name} version {dataset_version}")

    api = OpenDataAPI(api_token=api_key)

    params = {"maxKeys": 10, "orderBy": "created", "sorting": "desc"}
    response = api.list_files(dataset_name, dataset_version, params)
    if "error" in response:
        logger.error(f"Unable to retrieve list of files: {response['error']}")
        sys.exit(1)

    for file in response["files"]:
        filename = file.get("filename")
        logger.info(f"Downloading file is: {filename}")

        # fetch the download url and download the file
        response = api.get_file_url(dataset_name, dataset_version, filename)
        download_file_from_temporary_download_url(
            response["temporaryDownloadUrl"], DATAPATH / filename
        )


if __name__ == "__main__":
    main()
