import asyncio
import logging
import uuid

log = logging.getLogger("IaC:SocketClient")


class SocketBusClient:
    """Eventbus client for talking to the core socket manager."""

    def __init__(self, ctx, timeout_seconds: float = 15.0):
        self.ctx = ctx
        self.timeout_seconds = timeout_seconds
        self._pending: dict[str, asyncio.Future] = {}
        self.ctx.subscribe("socket:response")(self._on_response)

    async def _on_response(self, payload: dict):
        request_id = payload.get("request_id")
        if not request_id:
            return
        future = self._pending.get(request_id)
        if future is None or future.done():
            return
        if payload.get("ok"):
            future.set_result(payload.get("result"))
        else:
            future.set_exception(RuntimeError(payload.get("error") or "Socket request failed"))

    async def request(self, operation: str, provider: str = "docker", args: dict | None = None, timeout: float | None = None):
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        self.ctx.emit(
            "socket:request",
            {
                "request_id": request_id,
                "provider": provider,
                "operation": operation,
                "args": args or {},
            },
        )
        try:
            return await asyncio.wait_for(future, timeout or self.timeout_seconds)
        finally:
            self._pending.pop(request_id, None)

    async def resolve_runtime_mounts(self, required_targets: list[str] | None = None):
        result = await self.request(
            "docker:resolve_mounts",
            args={"required_targets": required_targets or []},
        )
        return result.get("mounts") if isinstance(result, dict) else {}

    async def spawn_runner(self, *, image: str, name: str, env_vars: list[dict] | None = None, mounts: list[dict] | None = None, command: list[str] | None = None, remove: bool = True, networks: list[str] | None = None):
        return await self.request(
            "docker:spawn",
            args={
                "image": image,
                "name": name,
                "env_vars": env_vars or [],
                "mounts": mounts or [],
                "command": command,
                "remove": remove,
                "networks": networks or [],
            },
        )
