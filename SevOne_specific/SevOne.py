#!/usr/bin/env python3

from asyncio.tasks import gather
import aiohttp
import asyncio
import json
import time
import yaml

# ****** Edit Config ******
url = "https://example.com/api/v2"  #  SevOne API URL - e.g. "https://example.com/api/v2" - only tested with SevOne API v2
username = "user"
password = "password"
api_request_page_size = 20  # The size of the requested page, defaults to 20; limited to a configurable maximum (max 10000 by default)
time_interval = 300  # How far back in time to get data from (in seconds) - this should equal to how often this script runs.
total_timeout = 9000  # Timeout for all API calls to be made (total) - seconds.
ssl = True  # Should SSL certificates be checked? This should be set to True in production?
tcp_force_close = (
    True  # Force the TCP connection to be closed and re-opened every call?
)
# ****** Stop Editing Config ******


# Login to SevOne API using user/pass and get an API token
async def api_login(session, url):
    async with session.post(
        f"{url}/authentication/signin",
        data=json.dumps({"name": username, "password": password}),
        headers={"content-type": "application/json"},
        ssl=ssl,
    ) as resp:
        response = await resp.text()
        response = json.loads(response)
        return response["token"]


async def get_request(session, url, api_token):
    output = []
    async with session.get(
        url,
        headers={"content-type": "application/json", "X-AUTH-TOKEN": api_token},
        ssl=ssl,
    ) as resp:
        response = await resp.text()
        response = json.loads(response)
        output = response

    return output


# Get details of a sinlge metric
async def get_device(
    session,
    url,
    api_token,
    deviceId,
    objectId,
    indicatorId,
    data_start_time_milliseconds,
    data_end_time_milliseconds,
):
    response = {}
    device = await get_request(session, f"{url}/devices/{deviceId}", api_token)
    response["deviceId"] = deviceId

    response["deviceName"] = device["name"]
    response["deviceAlternateName"] = device["alternateName"]
    response["deviceDescription"] = device["description"]
    response["deviceIp"] = device["ipAddress"]

    object = await get_request(
        session, f"{url}/devices/{deviceId}/objects/{objectId}", api_token
    )

    response["objectId"] = objectId
    response["objectName"] = object["name"]
    response["objectDescription"] = object["description"]
    response["objectAlternateName"] = object["alternateName"]

    indicator = await get_request(
        session,
        f"{url}/devices/{deviceId}/objects/{objectId}/indicators/{indicatorId}",
        api_token,
    )
    response["indicatorId"] = indicatorId
    response["indicatorName"] = indicator["name"]
    response["indicatorDescription"] = indicator["description"]
    response["dataUnits"] = indicator["dataUnits"]

    async with session.get(
        f"{url}/devices/{indicator['deviceId']}/objects/{indicator['objectId']}/indicators/{indicator['id']}/data",
        params={
            "startTime": data_start_time_milliseconds,
            "endTime": data_end_time_milliseconds,
        },
        headers={"content-type": "application/json", "X-AUTH-TOKEN": api_token},
        ssl=ssl,
    ) as resp:
        response1 = await resp.text()
        response["data"] = json.loads(response1)
    if response["data"] and response["data"][0]["value"] is not None:
        # Output data in Wavefront Format
        tags = ""
        tags = f'deviceId="{response["deviceId"]}"'
        if response["deviceAlternateName"]:
            tags = f'{tags} deviceAlternateName="{response["deviceAlternateName"]}"'
        if response["deviceDescription"]:
            tags = f'{tags} deviceDescription="{response["deviceDescription"]}"'
        if response["objectId"]:
            tags = f'{tags} objectId="{response["objectId"]}"'
        if response["objectName"]:
            tags = f'{tags} objectName="{response["objectName"]}"'
        if response["objectDescription"]:
            tags = f'{tags} objectDescription="{response["objectDescription"]}"'
        if response["objectAlternateName"]:
            tags = f'{tags} objectAlternateName="{response["objectAlternateName"]}"'
        if response["indicatorId"]:
            tags = f'{tags} indicatorId="{response["indicatorId"]}"'
        if response["indicatorName"]:
            tags = f'{tags} indicatorName="{response["indicatorName"]}"'
        if response["indicatorDescription"]:
            tags = f'{tags} indicatorDescription="{response["indicatorDescription"]}"'
        if response["dataUnits"]:
            tags = f'{tags} dataUnits="{response["dataUnits"]}"'
        metricName = f'"sevone.{response["deviceName"].replace(".","_")}.{response["objectName"].replace(".","_")}.{response["indicatorName"].replace(".","_")}"'.replace(
            " ", "_"
        )
        output = f'{metricName} {response["data"][0]["value"]} {data_end_time_milliseconds} source="SevOne" {tags}'
        print(output)


async def main():
    # Set timestamps
    start_time = time.time()
    data_end_time_milliseconds = int(int(time.time()) * 1000)
    data_start_time_milliseconds = int(
        data_end_time_milliseconds - (time_interval * 1000)
    )

    # Load indicators.yaml file
    with open("indicators.yaml", "r") as file:
        indicators = yaml.safe_load(file)
        indicators = indicators[0]["indicators"]

    # Create aiohttp session
    timeout = aiohttp.ClientTimeout(total=total_timeout)
    connector = aiohttp.TCPConnector(force_close=tcp_force_close)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Get API token using username/password
        api_token = await api_login(session, url)
        # Get details of all specificed indicators
        tasks = []
        for item in indicators:
            tasks.append(
                asyncio.create_task(
                    get_device(
                        session,
                        url,
                        api_token,
                        item["deviceId"],
                        item["objectId"],
                        item["indicatorId"],
                        data_start_time_milliseconds,
                        data_end_time_milliseconds,
                    )
                )
            )
        await asyncio.gather(*tasks)


asyncio.run(main())
