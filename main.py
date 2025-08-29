import json
from urllib.parse import quote_plus
import polars as pl
import requests
from bs4 import BeautifulSoup
from warcio.archiveiterator import ArchiveIterator

SERVER = "http://index.commoncrawl.org/"
CC_IDX = "CC-MAIN-2024-33"
AGENT_DECL = "cc-demo/0.0.1"


def search_cc_index(url: str) -> list[dict] | None:
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
    index_url = f"{SERVER}{CC_IDX}-index?url={encoded_url}&output=json"
    response = requests.get(index_url, headers={"user-agent": AGENT_DECL})
    if response.status_code == 200:
        records = response.text.strip().split("\n")
        return [json.loads(record) for record in records]
    else:
        return None


def fetch_page_from_cc(records: list[dict]) -> str | None:
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
            s3_url, headers={"user-agent": AGENT_DECL, "Range": byte_range}, stream=True
        )
        if response.status_code == 206:
            try:
                stream = ArchiveIterator(response.raw)
                for warc_record in stream:
                    if warc_record.rec_type == "response":
                        html_content = warc_record.content_stream().read()
                        if isinstance(html_content, bytes):
                            html_content = html_content.decode("utf-8", errors="ignore")
                        return html_content
            except Exception as e:
                print(f"Error processing WARC record: {e}")
    return None


def main():
    df = pl.read_csv("./data/sample.csv")
    urls = df["website"].unique()

    for u in urls:
        records = search_cc_index(u)
        if records:
            page = fetch_page_from_cc(records)
            if page is not None:
                try:
                    soup = BeautifulSoup(page, "html.parser")
                    text = soup.get_text(strip=True, separator=" ")
                    # Print first 500 characters as a sample
                    if text:
                        print(f"Sample text: {text[:500]}...")
                    else:
                        print("No text content found")
                except Exception as e:
                    print(f"Error parsing HTML for url {u}: {e}")
        else:
            print(f"No records found for {u}")


if __name__ == "__main__":
    main()
