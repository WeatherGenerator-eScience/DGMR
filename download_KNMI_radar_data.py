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

import datetime
import logging
import os
import sys
import tarfile
import time
from pathlib import Path

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3 import Retry

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", logging.INFO))

DATAPATH = Path("~/weathergenerator/data").expanduser()


def get_token():
    """Get the KNMI API token, stored in .env as "KNMI_TOKEN".

    See https://developer.dataplatform.knmi.nl/open-data-api#token
    """
    load_dotenv()

    token = os.getenv("KDP_TOKEN")

    if token is None:
        logger.error("Failed to read KDP_TOKEN from environment variables.")
        sys.exit(1)

    return token


class DatasetAPI:
    def __init__(self, dataset_name, dataset_version):
        self.base_url = "https://api.dataplatform.knmi.nl/open-data/v1"
        self.dataset_name = dataset_name
        self.dataset_version = dataset_version

        self.session = self.__create_session()
        self.session.headers.update({"Authorization": get_token()})

    def __create_session(self):
        session = requests.Session()

        retries = Retry(total=5, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        return session

    def __get_data(self, url, params=None):
        time.sleep(0.5)  # Avoid hitting rate limits
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def list_files(self, **params):
        return self.__get_data(
            f"{self.base_url}/datasets/{self.dataset_name}/versions/{self.dataset_version}/files",
            params=params,
        )

    def get_file_url(self, filename: str):
        response = self.__get_data(
            f"{self.base_url}/datasets/{self.dataset_name}/versions/{self.dataset_version}/files/{filename}/url"
        )
        return response.get("temporaryDownloadUrl")

    def download_file(self, filename: str, path: Path, overwrite=False):
        if (path / filename).exists() and not overwrite:
            logger.info(f"File {path / filename} already exists, skipping download.")
            return

        logger.info(f"Downloading file: {filename}")
        download_url = self.get_file_url(filename)

        with requests.get(download_url, stream=True) as r:
            r.raise_for_status()
            with open(path / filename, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)

        logger.info(f"Successfully downloaded dataset file to {path / filename}")


def extract_tar(tar_path):
    with tarfile.open(tar_path, "r") as tar:
        for member in tar.getmembers():
            member_path = tar_path.parent / member.name
            if not member_path.exists():
                tar.extract(member, path=tar_path.parent)
                print(f"Extracted: {member.name}")
            else:
                print(f"Skipped (already exists): {member.name}")


def main():
    begin = datetime.date(2024, 1, 1).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    end = datetime.date(2024, 2, 1).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    api = DatasetAPI(dataset_name="nl_rdr_data_rtcor_5m_tar", dataset_version="1.0")
    response = api.list_files(maxKeys=31, orderBy="created", begin=begin, end=end)

    for file in response["files"]:
        filename = file.get("filename")
        api.download_file(filename, DATAPATH)

    # Extract all tar files in the data path
    for tar_file in Path(DATAPATH).glob("*.tar"):
        extract_tar(tar_file)


if __name__ == "__main__":
    main()
