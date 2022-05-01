import importlib.util
import os
import sys
import re
from dataclasses import dataclass
from types import ModuleType
from typing import Dict, List, Union


@dataclass
class Route:
    name: str
    regexes: List[re.Pattern[str]]


class Router:
    def __init__(self):
        self.__registered_routes: List[Route] = []
        self.__modules: Dict[str, ModuleType] = {}

    def load_module(self, module_path: str):
        module_name = os.path.splitext(os.path.basename(module_path))[0]
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        if spec is None:
            raise Exception("Failed to load module: " + module_name)

        module: ModuleType = importlib.util.module_from_spec(spec)
        sys.modules[f"rc__{module_name}"] = module
        spec.loader.exec_module(module)

        name: str = getattr(module, "NAME", module_name)
        self.__modules[name] = module

    def load_modules(self, folder: str):
        for path in os.listdir(folder):
            if not path.endswith(".py"):
                continue

            self.load_module(os.path.join(folder, path))

    def register_route(self, name: str, regex: Union[str, List[str]]):
        if isinstance(regex, str):
            regex = [regex]
        regex_compiled = [re.compile(regex) for regex in regex]
        self.__registered_routes.append(Route(name, regex_compiled))

    def route(
        self, url: str, pipe_params: Dict[str, object] = {}
    ) -> Union[ModuleType, None]:
        for route in self.__registered_routes:
            for regex in route.regexes:
                if regex.match(url):
                    pipe_params["proccessor_cls"] = self.__modules[route.name]
                    return self.__modules[route.name]
        return None

