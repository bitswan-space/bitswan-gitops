"""
Event broadcaster for SSE push updates.
Manages connected SSE clients and broadcasts Docker/image events.
"""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class EventBroadcaster:
    def __init__(self):
        self._subscribers: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(maxsize=32)
        self._subscribers.add(queue)
        logger.info("SSE client subscribed (total: %d)", len(self._subscribers))
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        self._subscribers.discard(queue)
        logger.info("SSE client unsubscribed (total: %d)", len(self._subscribers))

    async def broadcast(self, event_type: str, data: Any):
        dead: list[asyncio.Queue] = []
        for queue in self._subscribers:
            try:
                queue.put_nowait({"event": event_type, "data": data})
            except asyncio.QueueFull:
                dead.append(queue)
        for q in dead:
            self._subscribers.discard(q)
            logger.warning(
                "Dropped slow SSE client (total: %d)", len(self._subscribers)
            )


# Singleton
event_broadcaster = EventBroadcaster()
