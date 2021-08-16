"""
acelerate.api
"""


# from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
from ruamel.yaml import YAML


class Configuration(ABC):

    path: Path

    @abstractmethod
    def apply_settings(self):
        pass


class Authorizer(ABC):

    is_authorized: bool = False

    @abstractmethod
    def authorize(self):
        pass


@dataclass
class Connection(ABC):

    config: Configuration

    @abstractmethod
    def open(self, authorizer: Authorizer, config_path: Path):
        pass


@dataclass
class APIConnection(Connection):

    def open(self, authorizer: Authorizer):




def login(authorizer: Authorizer):
    # authorize user
    # retain access token
    # return authorized session
    ...


def query(configuration: Configuration):
    ...
