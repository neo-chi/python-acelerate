from dataclasses import dataclass
from datetime import datetime
import dateutil.parser
# from enum import Enum, auto
from typing import Union
# from pprint import pprint


@dataclass
class QueryTimestamp:
    start: Union[str, datetime]
    end: Union[str, datetime]

    def __post_init__(self):
        self.str_to_datetime()

    def query_string_tuple(self):
        return self.start.isoformat(), self.end.isoformat()

    def str_to_datetime(self):
        try:
            self.start = dateutil.parser.parse(self.start)
        except TypeError as e:
            print(f"QueryTimestamp is already in datetime format {e}")
        try:
            self.end = dateutil.parser.parse(self.end)
        except TypeError as e:
            print(f"QueryTimestamp is already in datetime format {e}")

    def to_string(self):
        return f"{self.start.isoformat()} - {self.end.isoformat()}"


@dataclass
class QuerySelect:
    device_id: int
    field_address: int

    def to_string(self):
        string = (
            f"|DEVICE_ID: {self.device_id}"
            f"|FIELD_ADDRESS: {self.field_address}|"
        )
        return string


@dataclass
class QueryOptions:
    limit: int = 10000
    skip: int = 0
    sort: int = 1

    def to_string(self):
        string = (
            f"|LIMIT: {self.limit}"
            f"|SKIP: {self.skip}"
            f"|SORT: {self.sort}|"
            )
        return string


@dataclass
class ResponseData:
    timestamp: str
    device_id: int
    field_name: str
    field_table: str
    field_address: int
    value: int
    unit: str
    field_tag: str

    def to_dict(self):
        data_dictionary: dict = {
            'timestamp': self.timestamp,
            'device_id': self.device_id,
            'field_name': self.field_name,
            'field_table': self.field_table,
            'field_address': self.field_address,
            'value': self.value,
            'unit': self.unit,
            'field_tag': self.field_tag,
            }
        return data_dictionary

    def to_string(self):
        string = (
            f"|{self.timestamp}"
            f"|{self.device_id}"
            f"|{self.field_name}"
            f"|{self.field_table}"
            f"|{self.field_address}"
            f"|{self.value}"
            f"|{self.unit}"
            f"|{self.field_tag}|"
            )
        return string


@dataclass
class QueryProgress:
    total: int
    limit: int
    skip: int

    def status(self) -> dict:
        stat: dict = {
            'total_samples': self.total,
            'retrieved_samples': self.skip,
            'progress': self.to_string(),
            'is_complete': self.is_complete(),
        }
        return stat

    def is_complete(self) -> bool:
        # NOTE: Use this method to determine when a query must be repeated
        return True if self.skip >= self.total else False

    def percent_completion(self) -> int:
        percent_complete: int = int((self.skip / self.total)*100)
        return percent_complete if percent_complete < 100 else 100

    def to_string(self):
        string = ''.join([str(self.percent_completion()), '%'])
        return string


@dataclass
class QueryResponse:
    status: QueryProgress
    data: ResponseData

    def is_complete(self):
        return self.status.is_complete()


@dataclass
class Query:
    timestamp: QueryTimestamp
    selection: QuerySelect
    options: QueryOptions

    def to_string(self):
        ts = self.timestamp.query_string_tuple()
        string = (
                f"timestamp[$gt]={ts[0]}"
                f"&timestamp[$lt]={ts[-1]}"
                f"&device_id={self.selection.device_id}"
                f"&field_address={self.selection.field_address}"
                f"&$limit={self.options.limit}"
                f"&$skip={self.options.skip}"
                f"&$sort[timestamp]={self.options.sort}"
                )
        return string


if __name__ == "__main__":
    ts = QueryTimestamp(datetime(2021, 8, 14, 14, 30, 0), "2021-08-15 00:12")
    print(ts)
    print(ts.to_string())
    print(ts.query_string_tuple())
    q = Query(
        timestamp=QueryTimestamp(
            start="2021-08-14 09:12",
            end="2021-08-15 00:12",
            ),
        selection=QuerySelect(
            device_id=8355,
            field_address=3219,
            ),
        options=QueryOptions(),
        )
    print(q.to_string())
