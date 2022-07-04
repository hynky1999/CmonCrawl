import argparse
import asyncio
from datetime import datetime
import json
import logging
from pathlib import Path
from xmlrpc.client import Boolean

from aiostomp.aiostomp import Frame, AioStomp
from Downloader.download import Downloader
from OutStreamer.stream_to_file import OutStreamerFileJSON
from Pipeline.pipeline import ProcessorPipeline
from utils import DomainRecord
from Router.router import Router

# logging.basicConfig(
#     format="%(asctime)s - %(filename)s:%(lineno)d - " "%(levelname)s - %(message)s",
#     level="DEBUG",
# )

TASK_MAX_SIZE = 100


async def report_error(error: str):
    print("report_error:", error)


def on_poison_pill_wrapper(end_feature: asyncio.Future[Boolean], pills_to_die: int):
    pills = 0

    async def inner(frame: Frame, message: str | bytes | None):
        nonlocal pills
        pills += 1
        if pills == pills_to_die:
            end_feature.set_result(True)
        return True

    return inner


def on_message_wrapper(messages: asyncio.Queue[DomainRecord]):
    async def on_message(frame: Frame, message: str | bytes | None):
        if isinstance(message, bytes) and message is not None:
            message = message.decode()

        if message is None:
            return True

        msg_json = json.loads(message)
        try:
            msg_json["timestamp"] = datetime.fromisoformat(msg_json.get("timestamp"))
            domain_record = DomainRecord(**msg_json)
            messages.put_nowait(domain_record)
        except ValueError:
            pass
        return True

    return on_message


def init_pipeline(downloader: Downloader):
    router = Router()
    router.load_modules(str(Path("UserDefinedExtractors").absolute()))
    router.register_route("aktualne_cz", [r".*aktualne\.cz.*"])
    router.register_route("idnes_cz", [r".*idnes\.cz.*"])
    router.register_route("seznamzpravy_cz", [r".*seznamzpravy\.cz.*"])
    outstreamer = OutStreamerFileJSON(Path("output/"))
    pipeline = ProcessorPipeline(router, downloader, outstreamer)
    return pipeline


async def processor(queue_host: str, queue_port: int, pills_to_die: int):
    downloader = await Downloader().aopen()
    # async with Downloader() as downloader:
    pipeline = init_pipeline(downloader)
    messages = asyncio.Queue(0)
    finished: asyncio.Future[Boolean] = asyncio.get_event_loop().create_future()
    client = AioStomp(
        host=queue_host,
        port=queue_port,
        error_handler=report_error,
        reconnect_max_attempts=30,
        heartbeat_interval_cx=0,
        heartbeat_interval_cy=0,
    )
    await client.connect(username="consumer", password="consumer")
    client.subscribe("queue/articles.idnes.cz", handler=on_message_wrapper(messages))
    client.subscribe(
        "topic/poisson_pill.#",
        handler=on_poison_pill_wrapper(finished, pills_to_die),
    )
    pending_extracts = set()
    while not messages.empty() or not finished.done():
        if len(pending_extracts) > 0:
            _, pending_extracts = await asyncio.wait(
                pending_extracts, return_when="FIRST_COMPLETED"
            )

        while len(pending_extracts) < TASK_MAX_SIZE:
            try:
                dr = await asyncio.wait_for(messages.get(), 4)
                pending_extracts.add(
                    asyncio.create_task(pipeline.process_domain_record(dr))
                )
            except TimeoutError | asyncio.CancelledError:
                break

    await asyncio.gather(*pending_extracts)
    client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processor")
    parser.add_argument("--queue_host", type=str, default="localhost")
    parser.add_argument("--queue_port", type=int, default=61613)
    parser.add_argument("--pills_to_die", type=int, default=1)
    asyncio.run(processor(**vars(parser.parse_args())))
