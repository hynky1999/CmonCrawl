import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime
import json
from pathlib import Path
from typing import Dict, Set, Tuple

from stomp import Connection, ConnectionListener
from stomp.utils import Frame
from stomp.exception import StompException
from Downloader.download import Downloader
from OutStreamer.stream_to_file import OutStreamerFileJSON
from Pipeline.pipeline import ProcessorPipeline
from utils import DomainRecord, all_purpose_logger
from Router.router import Router


@dataclass
class Message:
    dr: DomainRecord
    headers: Dict[str, str]


def init_pipeline(downloader: Downloader, output_path: Path):
    router = Router()
    router.load_modules(str(Path("UserDefinedExtractors").absolute()))
    router.register_route("aktualne_cz", [r".*aktualne\.cz.*"])
    router.register_route("idnes_cz", [r".*idnes\.cz.*"])
    router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
    outstreamer = OutStreamerFileJSON(Path(output_path))
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    return pipeline


def init_connection(listener: ConnectionListener, conn: Connection):
    conn.set_listener("", listener)
    conn.connect(login="consumer", passcode="consumer", wait=True)
    conn.subscribe("/queue/articles.#", id="articles", ack="client-individual")
    conn.subscribe("/topic/poisson_pill.#", id="poisson_pill", ack="auto")
    return conn


class Listener(ConnectionListener):
    def __init__(self, messages: asyncio.Queue[Message]):
        self.messages = messages
        self.pills = 0

    def on_message(self, frame: Frame):
        if frame.headers.get("type") == "pill":
            self.pills += 1
        else:
            msg_json = json.loads(frame.body)
            try:
                msg_json["timestamp"] = datetime.fromisoformat(
                    msg_json.get("timestamp")
                )
                domain_record = DomainRecord(**msg_json)
                self.messages.put_nowait(Message(domain_record, frame.headers))
            except ValueError:
                pass


async def call_pipeline_with_ack(
    pipeline: ProcessorPipeline, msg: Message, client: Connection
):
    path = await pipeline.process_domain_record(msg.dr)
    # Ack at any result
    client.ack(msg.headers.get("message-id"), msg.headers.get("subscription"))
    return (msg, path)


async def processor(
    output_path: Path,
    queue_host: str,
    queue_port: int,
    pills_to_die: int,
    queue_size: int,
):
    pending_extracts: Set[asyncio.Task[Tuple[Message, Path] | None]] = set()
    conn = Connection(
        [(queue_host, queue_port)], reconnect_attempts_max=-1, heartbeats=(10000, 10000)
    )
    listener = Listener(asyncio.Queue(0))
    async with Downloader() as downloader:
        try:
            pipeline = init_pipeline(downloader, output_path)
            while not listener.messages.empty() or listener.pills < pills_to_die:
                try:
                    # Auto reconnect if queue disconnects
                    if not conn.is_connected():
                        conn = init_connection(listener, conn)

                    if len(pending_extracts) > 0:
                        done, pending_extracts = await asyncio.wait(
                            pending_extracts, return_when="FIRST_COMPLETED"
                        )
                        for task in done:
                            message, path = task.result()
                            if path is not None:
                                all_purpose_logger.info(
                                    f"Downloaded {message.dr.url} to {path}"
                                )

                    while (
                        len(pending_extracts) < queue_size
                        and not listener.messages.empty()
                    ):
                        pending_extracts.add(
                            asyncio.create_task(
                                call_pipeline_with_ack(
                                    pipeline, listener.messages.get_nowait(), conn
                                )
                            )
                        )
                except (KeyboardInterrupt, StompException, Exception) as e:
                    all_purpose_logger.error(e)
                    break
        except Exception as e:
            # Needed because async with would implicitly catch it and doesn't say anything about it
            all_purpose_logger.error(e)

    await asyncio.gather(*pending_extracts)
    conn.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processor")
    parser.add_argument("--queue_host", type=str, default="artemis")
    parser.add_argument("--queue_port", type=int, default=61613)
    parser.add_argument("--queue_size", type=int, default=80)
    parser.add_argument("--pills_to_die", type=int, default=1)
    parser.add_argument("output_path", type=Path)
    asyncio.run(processor(**vars(parser.parse_args())))
