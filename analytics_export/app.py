from aiohttp import web, ClientSession, MultipartWriter, streamer
import asyncio
import csv
from envparse import env
from pprint import pp
import json
from tempfile import NamedTemporaryFile

routes = web.RouteTableDef()


async def get_caluma_data(request):
    data = await request.content.read()
    data_object = json.loads(data)
    data_object["query"] = data_object["query"].replace("\n", "")

    async with ClientSession() as session:
        async with session.post(
            env.str("CALUMA_ENDPOINT", default="http://caluma:8000/graphql"),
            "http://localhost:8000/graphql",
            json=data_object,
            headers=request.headers,
            ssl=env.bool("ENABLE_SSL", default=None),
        ) as response:
            if not response.ok:
                raise web.HTTPBadRequest(reason=(await response.text()))

            response_value = await response.json()

            if not response_value:
                raise web.HTTPBadRequest(reason="Empty response from caluma")
            elif "errors" in response_value:
                raise web.HTTPBadRequest(reason=(await response.text()))

            return response_value


@routes.post("/export-csv")
async def export_csv_handler(request):
    data = await get_caluma_data(request)

    with MultipartWriter("mixed") as mpwriter:
        for i, node in enumerate(
            data["data"]["analyticsTable"]["resultData"]["records"]["edges"]
        ):
            headers = []
            values = []
            for row in node["node"]["edges"]:
                value = row["node"]["value"]
                values.append("" if value is None else value)

                if i == 0:
                    headers.append(row["node"]["alias"])

            if i == 0:
                part = mpwriter.append(",".join(headers), {"Content-Type": "text/csv"})
                part.set_content_disposition("attachment", filename="export.csv")

            part = mpwriter.append(",".join(values), {"Content-Type": "text/csv"})
            part.set_content_disposition("attachment", filename="export.csv")

        return web.Response(body=mpwriter)


def app():
    app = web.Application()
    app.add_routes(routes)

    return app
