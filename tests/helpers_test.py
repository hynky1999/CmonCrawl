import unittest
from unittest.mock import AsyncMock, MagicMock
from aiohttp import ClientSession
from cmoncrawl.aggregator.utils.helpers import retrieve, all_purpose_logger


class TestRetrieve(unittest.IsolatedAsyncioTestCase):
    async def test_logging(self):
        # Arrange
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.ok = True
        mock_response.json = AsyncMock(return_value={})
        mock_client.get.return_value.__aenter__.return_value = mock_response

        cdx_server = "http://test.com"
        params = {"param1": "value1"}
        content_type = "application/json"
        max_retry = 3
        sleep_base = 1.0

        # Act
        with self.assertLogs(all_purpose_logger, level="DEBUG") as cm:
            await retrieve(
                mock_client,
                cdx_server,
                params,
                content_type,
                max_retry,
                sleep_base,
            )

        # Assert
        expected_log_message = (
            f"Sending request to {cdx_server} with params: {params}"
        )
        self.assertEqual(expected_log_message, cm.records[0].message)

    async def test_failure_logging(self):
        # Arrange
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status = 500
        mock_response.ok = False
        mock_response.json = AsyncMock(return_value={})
        mock_response.reason = "Fail"
        mock_client.get.return_value.__aenter__.return_value = mock_response

        cdx_server = "http://test.com"
        params = {"param1": "value1"}
        content_type = "application/json"
        max_retry = 11
        sleep_base = 0

        # Act and Assert
        with self.assertLogs(all_purpose_logger, level="ERROR") as cm:
            try:
                await retrieve(
                    mock_client,
                    cdx_server,
                    params,
                    content_type,
                    max_retry,
                    sleep_base,
                    log_additional_info={"test": "test"},
                )
            except Exception:
                expected_log_message = f"Failed to retrieve from {cdx_server} with reason: 'DownloadError: Fail 500', retry: 10"
                expect_additional_info = f"additional info: {{'test': 'test'}}"
                self.assertEqual(len(cm.records), 3)
                self.assertIn(expected_log_message, cm.records[2].message)
                self.assertIn(
                    str(expect_additional_info), cm.records[2].message
                )
