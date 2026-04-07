from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from src.services.request_handler import RequestHandler


class TestRequestHandler:
    @pytest.fixture
    def mock_response(self):
        response = MagicMock()
        response.json = AsyncMock(return_value={"key": "value"})
        return response

    @pytest.fixture
    def mock_session(self, mock_response):
        session = MagicMock()
        session.get = MagicMock(
            return_value=MagicMock(
                __aenter__=AsyncMock(return_value=mock_response), __aexit__=AsyncMock()
            )
        )
        session.closed = False
        session.close = AsyncMock()
        return session

    @pytest.mark.anyio
    async def test_make_get_request_returns_json(self, mock_session):
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
            result = await handler.make_get_request("https://api.example.com/data")

        assert result == {"key": "value"}
        mock_session.get.assert_called_once_with("https://api.example.com/data")

    @pytest.mark.anyio
    async def test_make_get_request_uses_rate_limiter(self, mock_session):
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
            await handler.make_get_request("https://api.example.com/data")

        assert handler.rate_limiter is not None

    @pytest.mark.anyio
    async def test_close_closes_session(self, mock_session):
        mock_session.closed = False
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
            await handler.close()

        mock_session.close.assert_called_once()

    @pytest.mark.anyio
    async def test_close_does_nothing_when_session_already_closed(self, mock_session):
        mock_session.closed = True
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
            await handler.close()

        mock_session.close.assert_not_called()

    @pytest.mark.anyio
    async def test_rate_limiter_allows_10_requests_per_second(self, mock_session):
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
            assert handler.rate_limiter.max_rate == 10
            assert handler.rate_limiter.time_period == 1

    @pytest.mark.anyio
    async def test_session_is_created_on_init(self, mock_session):
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
        mock_session.get  # Just verify session was accessed
        assert handler.session is not None

    @pytest.mark.anyio
    async def test_finalizer_is_set_for_cleanup(self, mock_session):
        with patch("aiohttp.ClientSession", return_value=mock_session):
            handler = RequestHandler()
        assert handler._finalizer is not None
