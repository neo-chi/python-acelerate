import asyncio
# from abc import ABC, abstractmethod
# from dataclasses import dataclass
# from enum import Enum, auto
from pathlib import Path
from typing import Optional, Union
from entities import (
    Query,
    QueryTimestamp,
    QuerySelect,
    QueryOptions,
    ResponseData,
)

import aiohttp
# import numpy as np
import pandas as pd
import ruamel.yaml

# from .functions import logit


class AuthorizationError(Exception):
    pass


class Application:
    """Represents the acelerate data query application.
    API
    ---
        API URL:
            - project name
        API Services:
            authentication:
                header:
                    - email
                    - password
            data (query):
                header:
                    - access token
                query:
                    - start timestamp
                    - end timestamp
                    - device id
                    - field address
                    - limit
                    - skip
                    - sort
    """

    conf: Union[list[dict], dict]
    access_token: str = ""

    def __init__(self, config: Path = None, verbose: bool = False):
        yaml = ruamel.yaml.YAML()
        print(f"Loading {config.name} ...") if verbose else None

        with config.open("r") as config_file:
            try:
                configuration = yaml.load(config_file)
            except ruamel.YAMLError as e:
                print(f"Failed to load {config.name}: {e}")
            else:
                self.conf = configuration
        print(f"Success: loaded {config.name} ...") if verbose else None

    async def authorize_user(
            self,
            credentials_file: Optional[Path] = None
            ) -> bool:
        """Execute user authorization for API access.

        Returns:
            True if authorization is successful.

        Example:
            >>> app = Application(config="/path/to/config.yml")
            >>> is_authorized = app.authorize_user()
            >>> is_authorized
                True

        """
        # Retrieve user email and password from file.
        user = self.conf["user"]
        email = user["email"]
        password = user["password"]

        # Run authorization routine.
        url = Application.auth_url(project=self.conf["project"])
        if access_token := await Application.get_access_token(
            url=url, email=email, password=password
        ):
            self.access_token = access_token
            return True
        raise AuthorizationError(f"Failed to authorize user {email}")

    async def get_access_token(*, url: str, email: str, password: str) -> str:
        """Returns the user's authorization token, required to access API
        services.

        Args:
            email: The user's account email.
            password: The user's account password.

        Returns:
            access_token: The user's API service access token.

        Raises:
            AuthorizationError: if the email/password combination is
            invalid.

        """
        data = {"strategy": "local", "email": email, "password": password}
        async with aiohttp.ClientSession() as session:
            async with session.post(url=url, data=data) as response:
                content: dict = await response.json()
                await session.close()
                return content["accessToken"]

    async def query_device_register(self):
        project: str = self.conf["project"]
        # api_url: str = self.conf["api_url"]
        q = self.conf
        # url: str = '/'.join([api_url, 'data'])

        field_addresses = q["field_address"]

        query: Query = Query(
            timestamp=QueryTimestamp(start=q["t_start"], end=q["t_end"]),
            selection=QuerySelect(
                device_id=q["device_id"],
                field_address=field_addresses[0],
                # WARN:: Must loop through all fields in list
            ),
            options=QueryOptions(),
            )

        # prepare URL
        url = Application.data_url(project=project, query=query)
        print(url)

        # Open authorized session
        session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.access_token}"}
                )
        content: list[dict] = await Application.get_device_data(url, session)
        await session.close()
        return content

    async def get_device_data(self, url: str, session: aiohttp.ClientSession):
        async with session.get(url) as response:
            content = await response.json()
            await session.close()
            return content

    @staticmethod
    def data_url(*, project, query: Query):
        return Application.build_url(
            host=f"{project.lower()}.sr.dispatcher.acelerex.com/api",  # WARN: duplication with `authentication_url`
            path="data",
            query=query.to_string(),
        )

    def auth_url(project: str):
        """Returns the API authentication path url.

        Args:
            project: The API host

        Returns:
            url: Web URL path to the hosts authentication API

        Note:
            Send this ```authentication_url`` with a header containing
            authentication information to recieve authorization.
        """
        return Application.build_url(
            host=f"{project.lower()}.sr.dispatcher.acelerex.com/api",
            path="authentication",
        )

    def build_url(host: str, path: str, query: str = None, scheme: str = "https://"):
        """Returns the url ``{scheme}{host}/{path}?{query}``"""
        if query is not None:
            return "".join([scheme, host, "/", path, "?", query])
        return "".join([scheme, host, "/", path])


async def main():
    PROJECT = "NICE"

    # Step 1: Authentication
    # Returns access_token
    access_token = await Application.get_access_token(
        url=Application.auth_url(project=PROJECT),
        email="rchimento@southernresearch.org",
        password="Qmu8bp@h9w6tPj9$",
    )
    # print(access_token)

    # Step 2: Query preparation
    # Returns prepared query
    query = Query(
        time_start="2021-08-10T02:00:00",
        time_end="2021-08-10T17:30:00",
        device_id=8355,
        field_address=3219,
        limit=10000,
        skip=0,
        sort=1,
    )

    # Step 3: Open database session with authentication headers
    # Returns authorizaed http session
    session = aiohttp.ClientSession(headers={"Authorization": f"Bearer {access_token}"})

    # Step 4: Execute query
    # Returns dataset and open session
    data, session, limit, total, skip = await Application.get_device_data(
        url=Aplication.data_url(project=PROJECT, query=query), session=session
    )
    print(f"{total=}")
    print(f"{limit=}")
    print(f"{skip=}")

    df: pd.DataFrame = pd.DataFrame(data=data)
    print(df.timestamp)
    df = df.set_index("timestamp")
    print(df)
    df.to_csv("./data/queried.csv", na_rep="n/a", float_format="%0.3f", header=True)
    # df.to_excel(
    #         excel_writer=pd.ExcelWriter(engine="openpyxl", datetime_format="%Y-%M-%D %h:%m:%s", date_format="%Y-%M-%D"),
    #         sheet_name=df.timestamp[0],
    #         float_format="%0.3f",
    #         )
    df.to_excel("./data/queried.xlsx")

    writer = pd.ExcelWriter(
        "pandas_datetime.xlsx",
        engine="xlsxwriter",
        datetime_format="mmm d yyyy hh:mm:ss",
        date_format="mmmm dd yyyy",
    )
    df.to_excel(writer, sheet_name="Sheet 24")
    workbook = writer.book
    worksheet = writer.sheets["Sheet 24"]
    worksheet.set_column("A:C", 20)

    writer.save()
    return 0


if __name__ == "__main__":
    asyncio.run(main())


async def main():
    pass


if __name__ == "__main__":
    asyncio.run(main())
