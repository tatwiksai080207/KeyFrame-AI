"""Tests for RabbitMQ message serialization, schemas, and topology declaration."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from common.broker import (
    DLX_EXCHANGE_NAME,
    EXCHANGE_NAME,
    QUEUE_DLQ,
    QUEUE_FRAME_EXTRACT,
    QUEUE_PUBLISH,
    QUEUE_SUMMARIZE,
    QUEUE_TRANSCRIBE,
    ROUTING_KEY_DLQ,
    ROUTING_KEY_FRAME_EXTRACT,
    ROUTING_KEY_PUBLISH,
    ROUTING_KEY_SUMMARIZE,
    ROUTING_KEY_TRANSCRIBE,
    STAGE_ROUTING_KEYS,
    StageMessage,
    publish_stage_task,
    setup_topology,
)


def test_stage_message_serialization():
    """Verify StageMessage encodes to JSON bytes and decodes back accurately."""
    msg = StageMessage(
        job_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
        stage="transcribe",
        video_id="dQw4w9WgXcQ",
        youtube_url="https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        notion_token="secret_notion_token",
        notion_parent_id="test_parent_id",
        retry_count=1,
        last_error={"code": "TIMEOUT", "message": "Connection dropped"},
    )

    serialized = msg.serialize()
    assert isinstance(serialized, bytes)

    deserialized = StageMessage.deserialize(serialized)
    assert deserialized.job_id == msg.job_id
    assert deserialized.stage == "transcribe"
    assert deserialized.video_id == "dQw4w9WgXcQ"
    assert deserialized.retry_count == 1
    assert deserialized.last_error["code"] == "TIMEOUT"


def test_stage_routing_keys():
    """Verify routing key mappings match expected queue bindings."""
    assert STAGE_ROUTING_KEYS["transcribe"] == ROUTING_KEY_TRANSCRIBE
    assert STAGE_ROUTING_KEYS["summarize"] == ROUTING_KEY_SUMMARIZE
    assert STAGE_ROUTING_KEYS["frame_extract"] == ROUTING_KEY_FRAME_EXTRACT
    assert STAGE_ROUTING_KEYS["publish"] == ROUTING_KEY_PUBLISH
    assert STAGE_ROUTING_KEYS["dlq"] == ROUTING_KEY_DLQ


@pytest.mark.asyncio
async def test_setup_topology_mock():
    """Verify declare_exchange and declare_queue calls configure durable queues with DLX."""
    mock_channel = AsyncMock()
    mock_direct_exchange = AsyncMock()
    mock_dlx_exchange = AsyncMock()
    mock_queue = AsyncMock()

    mock_channel.declare_exchange.side_effect = [
        mock_direct_exchange,
        mock_dlx_exchange,
    ]
    mock_channel.declare_queue.return_value = mock_queue

    queues = await setup_topology(mock_channel)

    assert QUEUE_DLQ in queues
    assert QUEUE_TRANSCRIBE in queues
    assert QUEUE_SUMMARIZE in queues
    assert QUEUE_FRAME_EXTRACT in queues
    assert QUEUE_PUBLISH in queues

    assert mock_channel.declare_exchange.call_count == 2
    assert mock_channel.declare_queue.call_count == 5


@pytest.mark.asyncio
async def test_publish_stage_task_mock():
    """Verify publish_stage_task packages message with persistent delivery mode."""
    mock_conn = AsyncMock()
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()

    mock_conn.channel.return_value = mock_channel
    mock_channel.get_exchange.return_value = mock_exchange

    msg = StageMessage(
        job_id="test_job_123",
        stage="summarize",
        video_id="vid_abc",
        youtube_url="https://www.youtube.com/watch?v=vid_abc",
        notion_token="secret_key",
    )

    await publish_stage_task(msg, connection=mock_conn)

    mock_channel.get_exchange.assert_called_once_with(EXCHANGE_NAME)
    mock_exchange.publish.assert_called_once()
    call_args = mock_exchange.publish.call_args
    published_msg, = call_args[0]
    routing_key = call_args[1]["routing_key"]

    assert routing_key == ROUTING_KEY_SUMMARIZE
    assert b"test_job_123" in published_msg.body

