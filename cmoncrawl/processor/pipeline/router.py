from abc import ABC, abstractmethod
from datetime import datetime, timezone
import importlib.util
import os
from pathlib import Path
import sys
import re
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, List, Union

from cmoncrawl.processor.pipeline.extractor import IExtractor
from cmoncrawl.common.types import (
    RoutesConfig,
    PipeMetadata,
)
from cmoncrawl.common.loggers import (
    metadata_logger,
    all_purpose_logger,
)


@dataclass
class Route:
    name: str
    regexes: List[re.Pattern[str]]
    since: datetime
    to: datetime


class IRouter(ABC):
    @abstractmethod
    def route(
        self, url: str | None, time: datetime | None, metadata: PipeMetadata
    ) -> IExtractor:
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
        # check if offset naive datetime if so then convert to utc
        if url is None:
            raise ValueError("Url must not be None")
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
