import pytest
from unittest.mock import AsyncMock


@pytest.fixture
def mock_redis():
    redis = AsyncMock()
    redis.pubsub.return_value = AsyncMock()
    redis.publish = AsyncMock()
    return redis


@pytest.fixture
def mock_queue():
    queue = AsyncMock()
    queue.get = AsyncMock()
    queue.put = AsyncMock()
    return queue
