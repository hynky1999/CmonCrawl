import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import logging
import os
from pathlib import Path
import random
from typing import Any, Dict, List, Set, Tuple

from stomp import Connection, ConnectionListener
from stomp.utils import Frame
from stomp.exception import StompException
from Processor.App.Downloader.downloader import DownloaderFull
from Processor.App.OutStreamer.stream_to_file import OutStreamerFileJSON
from Processor.App.Pipeline.pipeline import ProcessorPipeline
from Processor.App.processor_utils import (
    DomainRecord,
    all_purpose_logger,
    metadata_logger,
)
from Processor.App.Router.router import Router

all_purpose_logger.setLevel(logging.INFO)
metadata_logger.setLevel(logging.WARN)


@dataclass
class Message:
    dr: DomainRecord
    headers: Dict[str, str]


@dataclass
class ListnerStats:
    messages: int = 0
    last_message_time: datetime = datetime.now()


def init_pipeline(
    downloader: DownloaderFull,
    extractors_path: Path,
    output_path: Path,
    config: Dict[str, Any],
):
    router = Router()
    router.load_modules(extractors_path)
    router.register_routes(config["routes"])
    outstreamer = OutStreamerFileJSON(Path(output_path))
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    return pipeline


def init_connection(conn: Connection, config: Dict[str, Any]):
    conn.connect(login="consumer", passcode="consumer", wait=True)
    for address in config["addresses"]:
        conn.subscribe(address, id=address, ack="client-individual")
    conn.subscribe("topic.poisson_pill.#", id="poisson_pill", ack="auto")
    all_purpose_logger.info("Connected to queue")
    return conn


class Listener(ConnectionListener):
    def __init__(self, messages: asyncio.Queue[Message], listener_stats: ListnerStats):
        self.messages = messages
        self.pills = 0
        self.listener_stats = listener_stats

    def on_message(self, frame: Frame):
        if frame.headers.get("type") == "poisson_pill":
            self.pills += 1
        else:
            msg_json = json.loads(frame.body)
            try:
                msg_json["timestamp"] = datetime.fromisoformat(
                    msg_json.get("timestamp")
                )
                domain_record = DomainRecord(**msg_json)
                self.messages.put_nowait(Message(domain_record, frame.headers))
                self.listener_stats.messages += 1
                self.listener_stats.last_message_time = datetime.now()
            except ValueError:
                pass


async def call_pipeline_with_ack(
    pipeline: ProcessorPipeline, msg: Message, client: Connection
):
    # Make sure no exception is thrown from this function
    # So that we can nack it if needed
    paths = []
    try:
        paths = await pipeline.process_domain_record(msg.dr)
        # Ack at any result
        client.ack(msg.headers.get("message-id"), msg.headers.get("subscription"))

    except KeyboardInterrupt:
        raise KeyboardInterrupt

    except Exception as e:
        client.nack(msg.headers.get("message-id"), msg.headers.get("subscription"))
        all_purpose_logger.error(f"Error in pipeline: {e}", exc_info=True)
    return (msg, paths)


def get_hostname_output_path(output_path: Path):
    hostname = os.environ["HOSTNAME"]
    if hostname == "":
        hostname = f"default{random.randint(0, 100)}"
        all_purpose_logger.warning(f"HOSTNAME is not set using hostname: {hostname}")

    return output_path / hostname


async def processor(
    output_path: Path,
    use_hostname_output: bool,
    queue_host: str,
    queue_port: int,
    pills_to_die: int,
    queue_size: int,
    timeout: int,
    config_path: Path,
    extractors_path: Path,
    max_retry: int,
    sleep_step: int
):
    timeout_delta = timedelta(minutes=timeout)
    with open(Path(config_path)) as f:
        config = json.load(f)
    pending_extracts: Set[asyncio.Task[Tuple[Message, List[Path]]]] = set()
    conn = Connection(
        [(queue_host, queue_port)], reconnect_attempts_max=-1, heartbeats=(10000, 10000)
    )
    listener_stats = ListnerStats()
    listener = Listener(asyncio.Queue(0), listener_stats)
    conn.set_listener("", listener)
    all_purpose_logger.debug("Connecting to queue")
    extracted_num = 0
    async with DownloaderFull(max_retry=max_retry, sleep_step=sleep_step) as downloader:
        try:
            if use_hostname_output:
                output_path = get_hostname_output_path(output_path)
            pipeline = init_pipeline(downloader, extractors_path, output_path, config)
        except Exception as e:
            all_purpose_logger.error(f"{e}", exc_info=True)
            return

        while True:
            if (
                listener.messages.empty()
                and (pills_to_die is None or listener.pills >= pills_to_die)
                and datetime.now() - listener.listener_stats.last_message_time
                >= timeout_delta
            ):
                all_purpose_logger.info(
                    f"No new messages in {timeout} minutes, exiting"
                )
                break
            try:
                # Auto reconnect if queue disconnects
                if not conn.is_connected():
                    conn = init_connection(conn, config)

                if len(pending_extracts) > 0:
                    done, pending_extracts = await asyncio.wait(
                        pending_extracts, return_when="FIRST_COMPLETED"
                    )
                    for task in done:
                        message, paths = task.result()
                        if len(paths) > 0:
                            for path in paths:
                                all_purpose_logger.info(
                                    f"Downloaded {message.dr.url} to {path}"
                                )
                                extracted_num += 1
                        else:
                            all_purpose_logger.info(
                                f"Failed to extract {message.dr.url}"
                            )
                while (
                    len(pending_extracts) < queue_size and not listener.messages.empty()
                ):
                    pending_extracts.add(
                        asyncio.create_task(
                            call_pipeline_with_ack(
                                pipeline, listener.messages.get_nowait(), conn
                            )
                        )
                    )
            except StompException as e:
                all_purpose_logger.error(e, exc_info=True)
                continue

            except Exception as e:
                all_purpose_logger.error(e, exc_info=True)
                break

        # Process reamining stuff in queue
        gathered = await asyncio.gather(*pending_extracts, return_exceptions=True)
        for task in gathered:
            if isinstance(task, Exception):
                continue
            message, paths = task
            if len(paths) > 0:
                for path in paths:
                    all_purpose_logger.info(f"Downloaded {message.dr.url} to {path}")
                    extracted_num += 1
            else:
                all_purpose_logger.info(f"Failed to extract {message.dr.url}")

    all_purpose_logger.info(
        f"Extracted {extracted_num}/{listener.listener_stats.messages} articles"
    )
    conn.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processor")
    parser.add_argument("--queue_host", type=str, default="artemis")
    parser.add_argument("--queue_port", type=int, default=61616)
    parser.add_argument("--queue_size", type=int, default=80)
    parser.add_argument("--pills_to_die", type=int, default=None)
    parser.add_argument("--output_path", type=Path, default=Path("output"))
    parser.add_argument("--use_hostname_output", action="store_true")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max_retry", type=int, default=20)
    parser.add_argument("--sleep_step", type=int, default=5)
    parser.add_argument(
        "--config_path",
        type=Path,
        default=Path(__file__).parent / "App" / "config.json",
    )
    parser.add_argument(
        "--extractors_path",
        type=Path,
        default=Path(__file__).parent / "App" / "DoneExtractors",
    )
    asyncio.run(processor(**vars(parser.parse_args())))
