import aiohttp
import asyncio
import pandas as pd
from lxml import etree
from typing import Optional, List

REPORT_LIST_URL = "https://reports.sem-o.com/api/v1/documents/static-reports"
DOCUMENT_BASE = "https://reports.sem-o.com/documents"

async def get_urls(
    dpug_id: Optional[str] = None,
    report_name: Optional[str] = None,
    resource_name: Optional[str] = None,
    group: Optional[str] = None,
    date: Optional[str] = None,
    dpug_ids: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Query SEMO API for BM-103 (Energy Market Financial Publication) reports
    and return full document URLs.
    """
    urls = []
    params = {"DPuG_ID": dpug_id, "ReportName": report_name, "ResourceName": resource_name, "Group": group, "Date": date}
    non_null_params = {k: v for k, v in params.items() if v is not None}
    async with aiohttp.ClientSession() as session:
        async with session.get(REPORT_LIST_URL, params=non_null_params) as resp:
            resp.raise_for_status()
            data = await resp.json()

    items = data.get('items')
    if not items:
        print("No items found in API response.")
        return urls

    temp = []
    
    for item in items:
        temp.append({
            "settlement_date": item.get("Date"),
            "dpug_id": item.get("DPuG_ID"),
            "url": item.get("ResourceName")
        })
    
    df = pd.DataFrame(temp)
    filtered_df = df[df['dpug_id'].isin(dpug_ids)] if dpug_ids else df

    return filtered_df

async def fetch_dataframe_from_url(url: str) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            xml_bytes = await resp.read()

    # Parse XML content in memory
    root = etree.fromstring(xml_bytes)
    rows = []

    # Traverse the nested structure: RESOURCE → DETERMINANT → VALUE
    for resource in root.xpath(".//RESOURCE"):
        resource_name = resource.get("name")
        for det in resource.xpath(".//DETERMINANT"):
            determinant_name = det.get("name")
            unit = det.get("unit")
            for val in det.xpath(".//VALUE"):
                rows.append({
                    "resource": resource_name,
                    "determinant": determinant_name,
                    "unit": unit,
                    "datetime": val.get("datetime"),
                    "amount": float(val.get("amount")),
                })

    df = pd.DataFrame(rows)
    return df

async def fetch_data_from_reports(urls: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """
    Fetch and combine data from multiple BM-103 report URLs into a single DataFrame.
    """
    tasks_with_ids = {}
    for row in urls.itertuples():
        full_url = f"{DOCUMENT_BASE}/{row.url}"
        tasks_with_ids[row.dpug_id] = fetch_dataframe_from_url(full_url)
    dataframes = await asyncio.gather(*tasks_with_ids.values())
    result_dict = {}
    for dpug_id, df in zip(tasks_with_ids.keys(), dataframes):
        result_dict[dpug_id] = df

    return result_dict