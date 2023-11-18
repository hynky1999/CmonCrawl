import importlib.util
import os
import re
import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import ModuleType
from typing import Dict, List, Union

from cmoncrawl.common.loggers import (
    all_purpose_logger,
    metadata_logger,
)
from cmoncrawl.common.types import (
    PipeMetadata,
    RoutesConfig,
)
from cmoncrawl.processor.pipeline.extractor import IExtractor


@dataclass
class Route:
    name: str
    regexes: List[re.Pattern[str]]
    since: datetime
    to: datetime


class IRouter(ABC):
    """
    Base class for all routers
    """

    @abstractmethod
    def route(
        self, url: str | None, time: datetime | None, metadata: PipeMetadata
    ) -> IExtractor:
        """
        Routes the url to the correct extractor
        """
        raise NotImplementedError()


class Router(IRouter):
    def __init__(self):
        self.registered_routes: List[Route] = []
        self.modules: Dict[str, IExtractor] = {}

    def load_module(self, module_path: Path):
        module_name = os.path.splitext(os.path.basename(module_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            raise Exception("Failed to load module: " + module_name)

        module: ModuleType = importlib.util.module_from_spec(spec)
        sys.modules[f"rc__{module_name}"] = module
        if spec.loader is None:
            raise Exception("Failed to load module: " + module_name)

        spec.loader.exec_module(module)
        return module, module_name

    def load_module_as_extractor(self, module_path: Path):
        """
        Loads a module and returns its extractor
        """
        module, module_name = self.load_module(module_path)
        name: str = getattr(module, "NAME", module_name)
        extractor: IExtractor | None = getattr(module, "extractor", None)
        if extractor is None:
            raise ValueError("Missing extractor variable in module: " + module_name)
        self.modules[name] = extractor
        all_purpose_logger.debug(f"Loaded module: {name}")
        return extractor

    def load_modules(self, folder: Path):
        extractors: List[IExtractor] = []
        for root, _, files in os.walk(folder):
            for file in files:
                if not file.endswith(".py"):
                    continue

                if file == "__init__.py":
                    self.load_module(Path(root) / file)
                else:
                    extractors.append(self.load_module_as_extractor(Path(root) / file))
        all_purpose_logger.info(f"Loaded {len(extractors)} extractors")

    def load_extractor(self, name: str, extractor: IExtractor):
        self.modules[name] = extractor

    def register_route(
        self,
        name: str,
        regex: Union[str, List[str]],
        since: datetime | None = None,
        to: datetime | None = None,
    ):
        """
        Registers a route for a given extractor name and regex

        Args:
            name (str): The name of the extractor
            regex (Union[str, List[str]]): The regex to match against
            since (datetime | None, optional): The earliest time to route to this extractor. Defaults to None.
            to (datetime | None, optional): The latest time to route to this extractor. Defaults to None.

        """
        if isinstance(regex, str):
            regex = [regex]
        regex_compiled = [re.compile(regex) for regex in regex]

        extractor = self.modules.get(name)
        if extractor is None:
            raise ValueError(f'No extractor named: "{name}" found')

        since = self._as_offset_aware(datetime.min if since is None else since)
        to = self._as_offset_aware(datetime.max if to is None else to)

        self.registered_routes.append(Route(name, regex_compiled, since, to))

    def register_routes(self, config: List[RoutesConfig]):
        for route in config:
            regex = route.regexes
            for extractor in route.extractors:
                self.register_route(
                    extractor.name,
                    regex,
                    extractor.since,
                    extractor.to,
                )
                all_purpose_logger.debug(f"Registered route: {extractor} {regex}")

    def _as_offset_aware(self, time: datetime) -> datetime:
        if time.tzinfo is None:
            return time.replace(tzinfo=timezone.utc)
        return time

    def route(
        self, url: str | None, time: datetime | None, metadata: PipeMetadata
    ) -> IExtractor:
        """
        Routes the url to the correct extractor based on the url and time

        Args:
            url (str | None): The url to route
            time (datetime | None): The time to route
            metadata (PipeMetadata): The metadata for the current pipeline
        """
        # check if offset naive datetime if so then convert to utc
        if url is None:
            all_purpose_logger.warn("No url provided, using empty string")
            url = ""

        time = self._as_offset_aware(time) if time is not None else None
        for route in self.registered_routes:
            for regex in route.regexes:
                if regex.match(url) and (
                    time is None or (route.since <= time and time < route.to)
                ):
                    metadata_logger.debug(
                        f"Routed {url} to {route.name}",
                        extra={"domain_record": metadata.domain_record},
                    )
                    return self.modules[route.name]
        raise ValueError("No route found for url: " + url)
