"""
SSE endpoint that pushes automation/image state changes to connected editors.
"""

import asyncio
import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app.dependencies import get_automation_service, get_image_service
from app.event_broadcaster import event_broadcaster

router = APIRouter(tags=["events"])


@router.get("/events/stream")
async def stream_events():
    """SSE endpoint that pushes automation/image state changes."""

    async def event_generator():
        queue = event_broadcaster.subscribe()
        try:
            # Send current state immediately on connect
            automations = await get_automation_service().get_automations()
            data = [
                a.model_dump(mode="json") if hasattr(a, "model_dump") else a
                for a in automations
            ]
            yield f"event: automations\ndata: {json.dumps(data)}\n\n"

            images = await get_image_service().get_images()
            yield f"event: images\ndata: {json.dumps(images)}\n\n"

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=30)
                    yield f"event: {msg['event']}\ndata: {json.dumps(msg['data'])}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            event_broadcaster.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
