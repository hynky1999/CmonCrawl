from datetime import datetime
import importlib.util
import logging
import os
import sys
import re
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, List, Union

from Extractor.extractor import BaseExtractor


@dataclass
class Route:
    name: str
    regexes: List[re.Pattern[str]]
    since: datetime
    to: datetime


class Router:
    def __init__(self):
        self.registered_routes: List[Route] = []
        self.modules: Dict[str, BaseExtractor] = {}

    def load_module(self, module_path: str):
        module_name = os.path.splitext(os.path.basename(module_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            raise Exception("Failed to load module: " + module_name)

        module: ModuleType = importlib.util.module_from_spec(spec)
        sys.modules[f"rc__{module_name}"] = module
        if spec.loader is None:
            raise Exception("Failed to load module: " + module_name)

        spec.loader.exec_module(module)

        name: str = getattr(module, "NAME", module_name)
        extractor: BaseExtractor | None = getattr(module, "extractor", None)
        if extractor is None:
            raise ValueError("Missing extractor variable in module: " + module_name)
        self.modules[name] = extractor

    def load_modules(self, folder: str):
        for path in os.listdir(folder):
            if not path.endswith(".py"):
                continue

            self.load_module(os.path.join(folder, path))

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
            raise ValueError(f"No extractor found for route: {name}")

        if since is None:
            since = extractor.SINCE if extractor.SINCE is not None else datetime.min
        if to is None:
            to = extractor.TO if extractor.TO is not None else datetime.max

        self.registered_routes.append(Route(name, regex_compiled, since, to))

    def route(self, url: str, time: datetime) -> BaseExtractor:
        for route in self.registered_routes:
            for regex in route.regexes:
                if regex.match(url) and route.since <= time and time < route.to:
                    logging.debug(f"Routed {url} to {route.name}")
                    return self.modules[route.name]
        raise ValueError("No route found for url: " + url)
