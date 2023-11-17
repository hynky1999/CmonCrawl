import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    AWS_PROFILE: str | None = os.getenv("AWS_PROFILE")

    def update_from_cli(self, args: argparse.Namespace):
        self.AWS_PROFILE = args.aws_profile or self.AWS_PROFILE  # type: ignore


CONFIG = Config()
