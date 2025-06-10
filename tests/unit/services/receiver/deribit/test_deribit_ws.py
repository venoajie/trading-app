import pytest
from unittest.mock import AsyncMock, MagicMock
from src.services.receiver.deribit import deribit_ws


@pytest.mark.asyncio
async def test_authentication_flow(mocker):
    # Mock WebSocket connection
    mock_ws = AsyncMock()
    mocker.patch("websockets.connect", return_value=mock_ws)

    # Create instance
    stream = deribit_ws.StreamingAccountData(
        sub_account_id="test", client_id="test_id", client_secret="test_secret"
    )

    # Test authentication
    await stream.ws_auth(AsyncMock())
    mock_ws.send.assert_called_with(
        '{"jsonrpc": "2.0", "id": 9929, "method": "public/auth", '
        '"params": {"grant_type": "client_credentials", '
        '"client_id": "test_id", "client_secret": "test_secret"}}'
    )
