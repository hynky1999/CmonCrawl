import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


def get_str_env(name: str, default: str | None) -> str | None:
    env = os.getenv(name, default)
    if not env:
        return None

    return env


@dataclass
class Config:
    AWS_PROFILE: str | None = get_str_env("AWS_PROFILE", None)

    def update_from_cli(self, args: argparse.Namespace):
        self.AWS_PROFILE = args.aws_profile or self.AWS_PROFILE  # type: ignore


CONFIG = Config()
