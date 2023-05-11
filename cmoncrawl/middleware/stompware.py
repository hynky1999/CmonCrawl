from datetime import datetime
import json
from typing import List

from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.common.loggers import all_purpose_logger
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Tuple
from cmoncrawl.aggregator.utils.helpers import unify_url_id


from stomp import Connection, ConnectionListener
from stomp.utils import Frame
from stomp.exception import StompException
from cmoncrawl.common.types import DomainRecord
from cmoncrawl.processor.pipeline.pipeline import ProcessorPipeline


DUPL_ID_HEADER = "_AMQ_DUPL_ID"


@dataclass
class Message:
    dr: DomainRecord
    headers: Dict[str, str]


@dataclass
class ListnerStats:
    messages: int = 0
    last_message_time: datetime = datetime.now()


class ArtemisAggregator:
    def __init__(
        self,
        queue_host: str,
        queue_port: int,
        url: str,
        index_agg: IndexAggregator,
        heartbeat: int = 10000,
    ):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.index_agg = index_agg
        self.url = url
        self.heartbeat = heartbeat

    def _init_connection(self):
        conn = Connection(
            [(self.queue_host, self.queue_port)],
            heartbeats=(self.heartbeat, self.heartbeat),
        )
        conn.connect(login="producer", passcode="producer", wait=True)
        all_purpose_logger.info(f"Connected to queue")
        return conn

    async def aggregate(self, filter_duplicates: bool = True):
        while True:
            try:
                conn = self._init_connection()
                break
            except StompException as e:
                all_purpose_logger.error(e, exc_info=True)
                await asyncio.sleep(5)
                continue
        i = 0
        async with self.index_agg as aggregator:
            async for domain_record in aggregator:
                try:
                    while not conn.is_connected():
                        conn = self._init_connection()

                    json_str = json.dumps(domain_record.__dict__, default=str)
                    headers = {}
                    id = unify_url_id(domain_record.url)
                    if filter_duplicates:
                        headers[DUPL_ID_HEADER] = id
                    conn.send(
                        f"queue.{self.url}",
                        json_str,
                        headers=headers,
                    )
                    all_purpose_logger.debug(
                        f"Sent url: {domain_record.url} with id: {id}"
                    )
                    i += 1
                except (StompException, OSError) as e:
                    all_purpose_logger.error(e, exc_info=True)
                    continue

                except Exception as e:
                    all_purpose_logger.error(e, exc_info=True)
                    break

        all_purpose_logger.info(f"Sent {i} messages")
        conn.send(
            f"topic.poisson_pill.{self.url}", "", headers={"type": "poisson_pill"}
        )
        conn.disconnect()


class ArtemisProcessor:
    def __init__(
        self,
        queue_host: str,
        queue_port: int,
        pills_to_die: int | None,
        queue_size: int,
        timeout: int,
        addresses: List[str],
        pipeline: ProcessorPipeline,
        heartbeat: int = 10000,
    ):
        self.queue_host = queue_host
        self.queue_port = queue_port
        self.pills_to_die = pills_to_die
        self.queue_size = queue_size
        self.timeout = timeout
        self.pipeline = pipeline
        self.addresses = addresses
        self.heartbeat = heartbeat

    class Listener(ConnectionListener):
        def __init__(
            self,
            messages: asyncio.Queue[Message],
            listener_stats: ListnerStats,
        ):
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

    def _init_connection(self, addresses: List[str]):
        conn = Connection(
            [(self.queue_host, self.queue_port)],
            reconnect_attempts_max=-1,
            heartbeats=(self.heartbeat, self.heartbeat),
        )
        conn.connect(login="consumer", passcode="consumer", wait=True)
        for address in addresses:
            conn.subscribe(address, id=address, ack="client-individual")
        conn.subscribe("topic.poisson_pill.#", id="poisson_pill", ack="auto")
        listener_stats = ListnerStats()
        listener = self.Listener(asyncio.Queue(0), listener_stats)
        conn.set_listener("", listener)
        all_purpose_logger.info("Connected to queue")
        return conn, listener

    async def _call_pipeline_with_ack(
        self,
        pipeline: ProcessorPipeline,
        msg: Message,
        client: Connection,
    ):
        # Make sure no exception is thrown from this function
        # So that we can nack it if needed
        paths = []
        try:
            paths = await pipeline.process_domain_record(msg.dr, {})
            # Ack at any result
            client.ack(msg.headers.get("message-id"), msg.headers.get("subscription"))

        except KeyboardInterrupt:
            raise KeyboardInterrupt

        except Exception as e:
            client.nack(msg.headers.get("message-id"), msg.headers.get("subscription"))
            all_purpose_logger.error(f"Error in pipeline: {e}", exc_info=True)
        return (msg, paths)

    async def process(self):
        timeout_delta = timedelta(minutes=self.timeout)
        # Set's extractor path based on config
        pending_extracts: Set[asyncio.Task[Tuple[Message, List[Path]]]] = set()
        while True:
            try:
                conn, listener = self._init_connection(self.addresses)
                break
            except StompException as e:
                all_purpose_logger.error(e, exc_info=True)
                await asyncio.sleep(5)
                continue
        all_purpose_logger.debug("Connecting to queue")
        extracted_num = 0
        try:
            if hasattr(self.pipeline.downloader, "__aenter__"):
                await self.pipeline.downloader.__aenter__()

            while True:
                if (
                    listener.messages.empty()
                    and (
                        self.pills_to_die is None or listener.pills >= self.pills_to_die
                    )
                    and datetime.now() - listener.listener_stats.last_message_time
                    >= timeout_delta
                ):
                    all_purpose_logger.info(
                        f"No new messages in {self.timeout} minutes, exiting"
                    )
                    break
                try:
                    # Auto reconnect if queue disconnects
                    if not conn.is_connected():
                        conn, listener = self._init_connection(self.addresses)

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
                        len(pending_extracts) < self.queue_size
                        and not listener.messages.empty()
                    ):
                        pending_extracts.add(
                            asyncio.create_task(
                                self._call_pipeline_with_ack(
                                    self.pipeline, listener.messages.get_nowait(), conn
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
                        all_purpose_logger.info(
                            f"Downloaded {message.dr.url} to {path}"
                        )
                        extracted_num += 1
                else:
                    all_purpose_logger.info(f"Failed to extract {message.dr.url}")
        except Exception as e:
            pass

        finally:
            if hasattr(self.pipeline.downloader, "__aexit__"):
                await self.pipeline.downloader.__aexit__(None, None, None)

        all_purpose_logger.info(
            f"Extracted {extracted_num}/{listener.listener_stats.messages} articles"
        )
        conn.disconnect()
