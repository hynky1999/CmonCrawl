import asyncio
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple

from stomp import Connection, ConnectionListener
from stomp.exception import StompException
from stomp.utils import Frame

from cmoncrawl.aggregator.index_query import IndexAggregator
from cmoncrawl.aggregator.utils.helpers import unify_url_id
from cmoncrawl.common.loggers import all_purpose_logger
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


class StompAggregator:
    """
    Aggregator that listens queries the common crawl index and sends the results to a queue
    using the stomp protocol. It the creates a queue
    with name `queue.{url}` and sends the results to it.
    It also creates a topic with name `topic.poisson_pill.{url}`
    and sends a message with type `poisson_pill` to it when it finishes.

    Args:
        queue_host (str): The host of the queue
        queue_port (int): The port of the queue
        url (str): The url of the queue
        index_agg (IndexAggregator): The index aggregator
        heartbeat (int, optional): The heartbeat of the connection. Defaults to 10000.


    """

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
        conn.connect(login="producer", passcode="producer", wait=True)  # type: ignore
        all_purpose_logger.info("Connected to queue")
        return conn

    async def aggregate(self, filter_duplicates: bool = True):
        """
        Aggregates the results of the index aggregator and sends them to the queue.
        If `filter_duplicates` is True, it will use the `DUPL_ID_HEADER` header,
        which Artemis uses to filter duplicates.
        """
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
                    id = unify_url_id(domain_record.url or "")
                    if filter_duplicates:
                        headers[DUPL_ID_HEADER] = id
                    conn.send(  # type: ignore
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
        conn.send(  # type: ignore
            f"topic.poisson_pill.{self.url}", "", headers={"type": "poisson_pill"}
        )
        conn.disconnect()  # type: ignore


class StompProcessor:
    """
    Processor that listens to a queues and processes the messages using a pipeline.
    When it receives a message with type enough `poisson_pill` messages, it will
    stop listening if it doesn't receive any messages for `timeout` minutes.


    Args:
        queue_host (str): The host of the queue
        queue_port (int): The port of the queue
        pills_to_die (int, optional): The number of `poisson_pill` messages to receive before dying. Defaults to None.
        queue_size (int): The size of the queue
        timeout (int): The timeout in minutes
        addresses (List[str]): The addresses of the queues
        pipeline (ProcessorPipeline): The pipeline to use for processing
        heartbeat (int, optional): The heartbeat of the connection. Defaults to 10000.

    """

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
            if frame.headers.get("type") == "poisson_pill":  # type: ignore
                self.pills += 1
            else:
                msg_json = json.loads(frame.body)  # type: ignore
                try:
                    msg_json["timestamp"] = datetime.fromisoformat(
                        msg_json.get("timestamp")
                    )
                    domain_record = DomainRecord(**msg_json)
                    self.messages.put_nowait(Message(domain_record, frame.headers))  # type: ignore
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
        conn.connect(login="consumer", passcode="consumer", wait=True)  # type: ignore
        for address in addresses:
            conn.subscribe(address, id=address, ack="client-individual")  # type: ignore
        conn.subscribe("topic.poisson_pill.#", id="poisson_pill", ack="auto")  # type: ignore
        listener_stats = ListnerStats()
        listener = self.Listener(asyncio.Queue(0), listener_stats)
        conn.set_listener("", listener)  # type: ignore
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
        pending_extracts: Set[asyncio.Task[Tuple[Message, List[str]]]] = set()
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
                await self.pipeline.downloader.__aenter__()  # type: ignore

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
        except Exception:
            pass

        finally:
            if hasattr(self.pipeline.downloader, "__aexit__"):
                await self.pipeline.downloader.__aexit__(None, None, None)  # type: ignore

        all_purpose_logger.info(
            f"Extracted {extracted_num}/{listener.listener_stats.messages} articles"
        )
        conn.disconnect()  # type: ignore
