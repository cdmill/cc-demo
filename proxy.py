import json
from dataclasses import dataclass
from datetime import datetime
from urllib.parse import quote_plus
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator

CC_INIT_YR = 2008


@dataclass
class CCProxyConfig:
    # This tells Common Crawl who is accessing their api, should be descriptive
    agent_decl: str
    # Format: MM
    # Ex: January -> "01"
    target_month: str
    year_range: range = range(CC_INIT_YR, datetime.now().year)
    savepath: str = f"{Path.cwd()}/records"


class CCProxy:
    def __init__(self, cfg: CCProxyConfig):
        self._cfg = cfg
        self.server = "http://index.commoncrawl.org"
        self.idxs = self._init_idxs()
        self.records = {}

    @property
    def cfg(self):
        return self._cfg

    def build_records(self, urls: list[str]):
        """For each year in the configured year range, this method
        searches Common Crawl indexes for the provided URLs, retrieves the
        archived HTML content, and extracts clean text using
        BeautifulSoup. The extracted text is stored in DataFrames
        organized by year.

        Args:
            urls: List of URLs to search for and process. Each URL should
            be a complete URL including protocol (e.g., 'https://example.com').

        Side Effects:
            - Populates self.records with DataFrames containing URL and content pairs
            - Creates empty DataFrames for years with no successful retrievals
        """
        for yr, idx in self.idxs.items():
            self.records[yr] = pl.DataFrame(schema=["url", "content"])
            for u in urls:
                records = self._query(u, idx)
                if records:
                    page = self._fetch(records)
                    if page is not None:
                        try:
                            soup = BeautifulSoup(page, "html.parser")
                            text = soup.get_text(strip=True, separator=" ")
                            if text:
                                self.records[yr][u] = text
                        except Exception as e:
                            print(f"Error parsing HTML for url {u}: {e}")

    def save(self):
        """Save all collected records as separate CSV files organized by year.

        Creates the configured save directory if it doesn't exist and writes each
        year's DataFrame to a CSV file named after the year. Only saves DataFrames
        that contain data (length > 0).

        File Structure:
            - Creates directory at self.cfg.savepath if it doesn't exist
            - Saves files as: {savepath}/{year}.csv
            - Each CSV contains columns: ["url", "content"]

        Note:
            This method should be called after build_records() has populated
            self.records with data. Empty DataFrames are silently skipped.
        """
        savepath = Path(self.cfg.savepath)
        savepath.mkdir(exist_ok=True)
        for year, df in self.records.items():
            if len(df) > 0:
                filename = savepath / f"{year}.csv"
                df.write_csv(filename)
                print(f"Saved {len(df)} records for {year} to {filename}")

    def _init_idxs(self):
        """Initialize and filter Common Crawl indexes for the target month and years.

        Fetches the complete list of available Common Crawl indexes from the collinfo.json
        endpoint and filters them to find crawls from the configured target month within
        the specified year range. Returns a mapping of years to their corresponding
        Common Crawl index IDs.

        Returns:
            dict[int, str]: Dictionary mapping years to Common Crawl index IDs.
                        Keys are years (int), values are index IDs like 'CC-MAIN-2024-33'.
                        Only includes years that have crawls from the target month.

        Note:
            - Exits the program if unable to access Common Crawl API
            - Handles malformed date strings gracefully by checking length
            - Continues processing if individual crawl records are malformed
        """
        crawls = {}
        idxs = {}
        response = None
        try:
            response = requests.get(
                f"{self.server}/collinfo.json",
                headers={"user-agent": self.cfg.agent_decl},
            )
        except Exception as e:
            print(f"Error accessing Common Crawl indexes: {e}")
            exit(1)
        if response.status_code == 200:
            crawls = response.json()
        for c in crawls:
            id = c["id"]
            if "timegate" in c:
                # Parse the start date (format: YYYY-MM-DD)
                start_date = c["timegate"].split("/")[0]
                if len(start_date) >= 7:  # YYYY-MM format minimum
                    year_month = start_date[:7]  # YYYY-MM
                    year = int(year_month[:4])
                    month = year_month[5:7]
                    if (
                        month == self.cfg.target_month
                        and year in self.cfg.year_range
                        and year not in idxs
                    ):
                        idxs[year] = id
        return idxs

    def _query(self, url: str, idx: str):
        """Queries the Common Crawl index API to find all archived records for
        a given URL.

        Args:
            url: The URL to search for in the Common Crawl index. Should be a
                complete URL including protocol (e.g., 'https://example.com').

        Returns:
            A list of dictionaries containing record metadata if the URL is
            found in the index. Each dictionary includes fields like 'offset',
            'length', 'filename', 'timestamp', etc. Returns None if the URL is
            not found or if the API request fails.
        """
        encoded_url = quote_plus(url)
        index_url = f"{self.server}/{idx}-index?url={encoded_url}&output=json"
        response = requests.get(index_url, headers={"user-agent": self.cfg.agent_decl})
        if response.status_code == 200:
            records = response.text.strip().split("\n")
            return [json.loads(record) for record in records]
        else:
            return None

    def _fetch(self, records: list[dict]) -> str:
        """Given a list of Common Crawl record metadata, attempts to retrieve
        the actual HTML content from the first successfully accessible record.
        Uses byte-range requests to efficiently download only the specific
        archived page data.

        Args:
            records: A list of record dictionaries returned from search_cc_index().
                    Each record must contain 'offset', 'length', and 'filename' keys.

        Returns:
            The decoded HTML content of the first successfully retrieved page
            as a UTF-8 string. Returns None if no records can be successfully
            retrieved or if all requests fail.

        Note:
            - Only processes 'response' type WARC records
            - Handles encoding issues by using 'ignore' error handling
        """
        for record in records:
            offset, length = int(record["offset"]), int(record["length"])
            s3_url = f"https://data.commoncrawl.org/{record['filename']}"
            # Define the byte range for the request
            byte_range = f"bytes={offset}-{offset + length - 1}"
            # NOTE: Use `stream=True` to get a raw byte stream since the
            # response returns gzip compressed data
            response = requests.get(
                s3_url,
                headers={"user-agent": self.cfg.agent_decl, "Range": byte_range},
                stream=True,
            )
            if response.status_code == 206:
                try:
                    stream = ArchiveIterator(response.raw)
                    for warc_record in stream:
                        if warc_record.rec_type == "response":
                            html_content = warc_record.content_stream().read()
                            if isinstance(html_content, bytes):
                                html_content = html_content.decode(
                                    "utf-8", errors="ignore"
                                )
                            return html_content
                except Exception as e:
                    print(f"Error processing WARC record: {e}")
        return None
