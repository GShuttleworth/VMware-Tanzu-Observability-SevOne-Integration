#!/usr/bin/env python3

from asyncio.tasks import gather
import aiohttp
import asyncio
import json
import time

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
        params={"page": 0, "size": api_request_page_size},
        headers={"content-type": "application/json", "X-AUTH-TOKEN": api_token},
        ssl=ssl,
    ) as resp:
        response = await resp.text()
        response = json.loads(response)
        output.extend(response["content"])
    if response["totalPages"] > 1:
        tasks = []
        for i in range(1, response["totalPages"]):

            async def get_next_page(session, url, api_token, page_number):
                async with session.get(
                    url,
                    params={"page": page_number, "size": api_request_page_size},
                    headers={
                        "content-type": "application/json",
                        "X-AUTH-TOKEN": api_token,
                    },
                    ssl=ssl,
                ) as resp:
                    response = await resp.text()
                    response = json.loads(response)
                    output.extend(response["content"])

            tasks.append(asyncio.create_task(get_next_page(session, url, api_token, i)))
        await asyncio.gather(*tasks)
    return output


# Get list of all objects from a list of devices
async def get_objects(session, url, api_token, devices):
    objects = []
    tasks = []
    for device in devices:

        async def get_object_from_device(session, url, api_token, device):
            response = await get_request(
                session, f"{url}/devices/{device['id']}/objects", api_token
            )
            # if object is returned
            if response:
                for item in response:
                    item["deviceName"] = device["name"]
                    item["deviceAlternateName"] = device["alternateName"]
                    item["deviceDescription"] = device["description"]
                    item["deviceIp"] = device["ipAddress"]

                objects.extend(response)

        tasks.append(
            asyncio.create_task(get_object_from_device(session, url, api_token, device))
        )
    await asyncio.gather(*tasks)
    return objects


# Get list of all indicators from a list of objects
async def get_indicators(session, url, api_token, objects):
    indicators = []
    tasks = []
    for object in objects:

        async def get_indicators_from_object(session, url, api_token, object):
            response = await get_request(
                session,
                f"{url}/devices/{object['deviceId']}/objects/{object['id']}/indicators",
                api_token,
            )
            # if indicator(s) is returned
            if response:
                for item in response:
                    item["deviceName"] = object["deviceName"]
                    item["deviceAlternateName"] = object["deviceAlternateName"]
                    item["deviceDescription"] = object["deviceDescription"]
                    item["deviceIp"] = object["deviceIp"]
                    item["objectName"] = object["name"]
                    item["objectDescription"] = object["description"]
                    item["objectAlternateName"] = object["alternateName"]
                indicators.extend(response)

        tasks.append(
            asyncio.create_task(
                get_indicators_from_object(session, url, api_token, object)
            )
        )
    await asyncio.gather(*tasks)
    return indicators


# Get list of all data points from a list of indicators
async def get_data(
    session,
    url,
    api_token,
    indicators,
    data_start_time_milliseconds,
    data_end_time_milliseconds,
):
    tasks = []
    for indicator in indicators:

        async def get_data_from_indicator(
            session,
            url,
            api_token,
            indicator,
            data_start_time_milliseconds,
            data_end_time_milliseconds,
        ):
            async with session.get(
                f"{url}/devices/{indicator['deviceId']}/objects/{indicator['objectId']}/indicators/{indicator['id']}/data",
                params={
                    "startTime": data_start_time_milliseconds,
                    "endTime": data_end_time_milliseconds,
                },
                headers={"content-type": "application/json", "X-AUTH-TOKEN": api_token},
                ssl=ssl,
            ) as resp:
                response = await resp.text()
                response = json.loads(response)
                if response and response[0]["value"] is not None:
                    datum = {
                        "deviceId": indicator["deviceId"],
                        "deviceName": indicator["deviceName"],
                        "deviceAlternateName": indicator["deviceAlternateName"],
                        "deviceDescription": indicator["deviceDescription"],
                        "deviceIp": indicator["deviceIp"],
                        "objectId": indicator["objectId"],
                        "objectName": indicator["objectName"],
                        "objectDescription": indicator["objectDescription"],
                        "objectAlternateName": indicator["objectAlternateName"],
                        "indicatorId": indicator["id"],
                        "indicatorName": indicator["name"],
                        "indicatorDescription": indicator["description"],
                        "dataUnits": indicator["dataUnits"],
                        "data": response,
                    }
                    # Output data in Wavefront Format
                    tags = ""
                    tags = f'deviceId="{datum["deviceId"]}"'
                    if datum["deviceAlternateName"]:
                        tags = f'{tags} deviceAlternateName="{datum["deviceAlternateName"]}"'
                    if datum["deviceDescription"]:
                        tags = (
                            f'{tags} deviceDescription="{datum["deviceDescription"]}"'
                        )
                    if datum["objectId"]:
                        tags = f'{tags} objectId="{datum["objectId"]}"'
                    if datum["objectName"]:
                        tags = f'{tags} objectName="{datum["objectName"]}"'
                    if datum["objectDescription"]:
                        tags = (
                            f'{tags} objectDescription="{datum["objectDescription"]}"'
                        )
                    if datum["objectAlternateName"]:
                        tags = f'{tags} objectAlternateName="{datum["objectAlternateName"]}"'
                    if datum["indicatorId"]:
                        tags = f'{tags} indicatorId="{datum["indicatorId"]}"'
                    if datum["indicatorName"]:
                        tags = f'{tags} indicatorName="{datum["indicatorName"]}"'
                    if datum["indicatorDescription"]:
                        tags = f'{tags} indicatorDescription="{datum["indicatorDescription"]}"'
                    if datum["dataUnits"]:
                        tags = f'{tags} dataUnits="{datum["dataUnits"]}"'
                    metricName = f'"sevone.{datum["deviceName"].replace(".","_")}.{datum["objectName"].replace(".","_")}.{datum["indicatorName"].replace(".","_")}"'.replace(
                        " ", "_"
                    )
                    output = f'{metricName} {datum["data"][0]["value"]} {data_end_time_milliseconds} source="SevOne" {tags}'
                    print(output)

        tasks.append(
            asyncio.create_task(
                get_data_from_indicator(
                    session,
                    url,
                    api_token,
                    indicator,
                    data_start_time_milliseconds,
                    data_end_time_milliseconds,
                )
            )
        )
    await asyncio.gather(*tasks)


async def main():
    start_time = time.time()
    data_end_time_milliseconds = int(int(time.time()) * 1000)
    data_start_time_milliseconds = int(
        data_end_time_milliseconds - (time_interval * 1000)
    )
    # Create aiohttp session
    timeout = aiohttp.ClientTimeout(total=total_timeout)
    connector = aiohttp.TCPConnector(force_close=tcp_force_close)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # Get API token using username/password
        api_token = await api_login(session, url)
        # Get list of all devices
        devices = await get_request(session, f"{url}/devices", api_token)

        # Get list of all objects
        objects = await get_objects(session, url, api_token, devices)

        # Get list of all indicators
        indicators = await get_indicators(session, url, api_token, objects)

        await get_data(
            session,
            url,
            api_token,
            indicators,
            data_start_time_milliseconds,
            data_end_time_milliseconds,
        )

    # print("--- %s seconds to run ---" % (time.time() - start_time))


asyncio.run(main())
