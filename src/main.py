"""Configuration for fast api"""

import asyncio
from typing import Optional, Dict, Any, List, Tuple
from logging import info
from abc import ABC, abstractmethod
from threading import Thread
from json.decoder import JSONDecodeError
from fastapi import FastAPI, WebSocket, BackgroundTasks
from starlette.websockets import WebSocketDisconnect
from pydantic import BaseModel, ValidationError


app = FastAPI(debug=True)

# Clasess
class ResponseWS(BaseModel):
    code: int
    version: float
    message: str
    description: Optional[str]


class ResponseOkWS(ResponseWS):
    attributes: Dict[str, Any]


class ErrorResponseWS(ResponseWS):
    error_message: str


class RequestWS(BaseModel):
    event: str
    kwargs: Optional[Dict[str, Any]]


class RequestWSPublisher(RequestWS):
    channels: List[str]


class ChannelNotValid(Exception):
    message: str = "Channel not Valid"


class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.channels: Dict[str, List[WebSocket]] = {}

    async def connect(self, websocket: WebSocket, channels: Optional[List[str]] = []):
        await websocket.accept()
        self.active_connections.append(websocket)
        if channels:
            channels = list(filter(lambda channel: channel !=  "", channels))
            for channel in channels:
                if channel not in self.channels:
                    self.channels[channel] = []
                self.channels[channel].append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        for key, consumers in self.channels.items():
            if websocket in consumers:
                self.consumer[key].remove(websocket)

    async def unicast(self, message: ResponseWS, websocket: WebSocket):
        await websocket.send_json(message.dict())

    async def multicast(self, channel_name: str, message: ResponseWS):
        if not channel_name in self.channels:
            raise ChannelNotValid()
        channel = self.channels[channel_name]
        for connection in channel:
            await connection.send_json(message.dict())

    async def broadcast(self, message: ResponseWS):
        for connection in self.active_connections:
            await connection.send_json(message.dict())


class Event(ABC):
    def __init__(self, event: str, manager: ConnectionManager):
        self.name: str =  event
        self.manager: ConnectionManager = manager

    @abstractmethod
    async def execute(self, *args, **kwargs):
        raise NotImplementedError()


class TheEvent(Event):
    def __init__(self, manager: ConnectionManager):
        name = "THE_EVENT"
        super().__init__(name, manager)

    async def execute(self, *args, **kwargs):
        channels: str = kwargs["channels"]
        response = ResponseOkWS(
            code=200,
            version=1.0,
            message="event",
            description="sended pub/sub event",
            attributes={"event": self.name, **kwargs}
        )
        for channel in channels:
            await self.manager.multicast(channel, response)


event_listener: List[Event] = [
    TheEvent,
]


manager = ConnectionManager()

# Exceptions
class InvalidRequest(Exception):
    pass

class EventNotFound(Exception):
    pass


def to_request(data: dict) -> RequestWS:
    """
    This convert to Request valid
    if this is failure throw Exception -> InvalidRequest
    """
    try:
        conversion = RequestWS(**data)
    except ValidationError as exc:
        raise InvalidRequest(exc)
    return conversion

def to_request_publisher(data: dict) -> RequestWSPublisher:
    try:
        conversion = RequestWSPublisher(**data)
    except ValidationError as exc:
        raise InvalidRequest(exc)
    return conversion


def handle_event(request: RequestWSPublisher, manager: ConnectionManager):
    """Control of events
    if this is failure throw Exception -> EventNotFound
    """
    event = list(filter(lambda event: event(manager).name == request.event, event_listener))
    if not event:
        raise EventNotFound()
    emiter = event[0](manager)
    kwargs = request.kwargs or {}
    asyncio.ensure_future(asyncio.create_task(emiter.execute(channels=request.channels,**kwargs)))

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.websocket("/publisher")
async def websocket(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data_dict: dict = await websocket.receive_json()
            request = to_request_publisher(data_dict)
            handle_event(request, manager)
    except ChannelNotValid:
        error_response = ErrorResponseWS(
            code=423,
            version=1.0,
            message="channel invalid",
            description="the channel not found",
            error_message="channel",
        )
        await websocket.send_json(error_response.dict())
    except EventNotFound:
        error_response = ErrorResponseWS(
            code=423,
            version=1.0,
            message="event invalid",
            description="the sended event not found",
            error_message="event",
        )
        await websocket.send_json(error_response.dict())
    except InvalidRequest as exc:
        error_response = ErrorResponseWS(
            code=423,
            version=1.0,
            message="json invalid",
            description="a json invalid is sended",
            error_message=str(exc),
        )
        await websocket.send_json(error_response.dict())
    except JSONDecodeError:
        error_response = ErrorResponseWS(
            code=423,
            version=1.0,
            message="json invalid",
            description="a json invalid is sended",
            error_message="json",
        )
        await websocket.send_json(error_response.dict())
    except WebSocketDisconnect:
        manager.disconnect(websocket)


@app.websocket("/subscriber")
async def websocket(websocket: WebSocket, channels: Optional[str] = ""):
    await manager.connect(websocket, channels.split(","))
    try:
        while True:
            await websocket.receive_text()
    except JSONDecodeError:
        error_response = ErrorResponseWS(
            code=423,
            version=1.0,
            message="json invalid",
            description="a json invalid is sended",
            error_message="json",
        )
        await websocket.send_json(error_response.dict())
    except WebSocketDisconnect:
        manager.disconnect(websocket)
