import textwrap
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlparse

from cmoncrawl.aggregator.utils.helpers import crawl_to_year
from cmoncrawl.common.types import MatchType


def url_query_based_on_match_type(match_type: MatchType, url: str):
    # Given www.arxiv.org/abs/1905.00075 following will match
    parsed_url = urlparse(url) if url.startswith("http") else urlparse(f"http://{url}")
    host = (
        parsed_url.netloc
        if not parsed_url.netloc.startswith("www.")
        else parsed_url.netloc[4:]
    )
    path = parsed_url.path
    match match_type:
        case MatchType.PREFIX:
            # (www.)?arxiv.org/abs/1905.00075(/.*)?
            parsed_url = urlparse(url)
            return f"(cc.url_host_name = '{host}' OR cc.url_host_name = 'www.{host}') AND (cc.url_path = '{path}' OR cc.url_path LIKE '{path}/%')"
        case MatchType.HOST:
            # (www.)?arxiv.org(/.*)?
            return f"cc.url_host_name = '{host}' OR cc.url_host_name = 'www.{host}'"
        case MatchType.DOMAIN:
            # (.*\.)arxiv.org(/.*)?
            return f"cc.url_host_name LIKE '%.{host}' OR cc.url_host_name = '{host}'"
        case MatchType.EXACT:
            # www.arxiv.org/abs/1905.00075
            return f"cc.url = '{url}'"
    raise ValueError("Invalid match type")


def date_to_sql_format(date: datetime):
    return date.strftime("%Y-%m-%d %H:%M:%S")


def url_query_date_range(since: Optional[datetime], to: Optional[datetime]):
    if since is None and to is None:
        return ""
    elif since is not None and to is not None:
        return f"cc.fetch_time BETWEEN CAST('{date_to_sql_format(since)}' AS TIMESTAMP) AND CAST('{date_to_sql_format(to)}' AS TIMESTAMP)"
    elif to is not None:
        return f"cc.fetch_time <= CAST('{date_to_sql_format(to)}' AS TIMESTAMP)"
    elif since is not None:
        return f"cc.fetch_time >= CAST('{date_to_sql_format(since)}' AS TIMESTAMP)"

    raise ValueError("Invalid date range")


def crawl_url_to_name(crawl_url: str):
    crawl = crawl_url.split("/")[-1]
    without_index = crawl.split("-index")[0]
    return without_index


def crawl_query(
    crawl_urls: List[str], since: Optional[datetime], to: Optional[datetime]
):
    allowed_crawls = [
        crawl_url_to_name(crawl)
        for crawl in crawl_urls
        if (since is None or crawl_to_year(crawl) >= since.year)
        and (to is None or crawl_to_year(crawl) <= to.year)
    ]
    allowed_crawls_query = " OR ".join(
        f"cc.crawl = '{crawl}'" for crawl in allowed_crawls
    )
    return allowed_crawls_query


def prepare_athena_where_conditions(
    urls: List[str],
    since: Optional[datetime],
    to: Optional[datetime],
    crawl_urls: List[str],
    match_type: MatchType = MatchType.EXACT,
):
    urls_with_type_query = [
        f"({url_query_based_on_match_type(match_type, url)})" for url in urls
    ]
    url_query = " OR ".join(urls_with_type_query)
    allowed_crawls_query = crawl_query(crawl_urls, since, to)
    date_query = url_query_date_range(since, to)
    where_conditions = [
        date_query,
        allowed_crawls_query,
        "cc.fetch_status = 200",
        "cc.subset = 'warc'",
        url_query,
    ]
    where_conditions = [condition for condition in where_conditions if condition]
    return where_conditions


def prepare_athena_sql_query(
    urls: List[str],
    since: Optional[datetime],
    to: Optional[datetime],
    crawl_urls: List[str],
    database: str,
    table: str,
    match_type: MatchType = MatchType.EXACT,
    extra_sql_where_clause: str | None = None,
):
    where_conditions = prepare_athena_where_conditions(
        urls, since, to, crawl_urls, match_type
    )
    where_conditions += (
        [extra_sql_where_clause] if extra_sql_where_clause is not None else []
    )
    where_conditions_query = " AND ".join(
        f"({condition})" for condition in where_conditions
    )
    query = textwrap.dedent(
        f"""\
        SELECT cc.url,
                cc.fetch_time,
                cc.warc_filename,
                cc.warc_record_offset,
                cc.warc_record_length
        FROM "{database}"."{table}" AS cc
        WHERE {where_conditions_query};"""
    )
    return query


def get_name(
    since: datetime,
    until: datetime,
    urls: List[str],
    match_type: MatchType = MatchType.EXACT,
):
    return f"{'-'.join(urls)}-{since.strftime('%Y%m%d')}-{until.strftime('%Y%m%d')}-{match_type.name}"


def to_timestamp_format(date: datetime):
    return date.strftime("%Y%m%d%H%M%S")
