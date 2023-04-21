from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
# from app.extensions.logger import backend_logger
from fastapi import status


# class AccessMiddleware(BaseHTTPMiddleware):
#     async def dispatch(self, request: Request, call_next):
#         backend_logger.info(f"{request.client.host} {request.method} {request.url}  [I]")
#         try:
#             response = await call_next(request)
#         except Exception as exc:
#             backend_logger.exception(exc)
#             raise exc
#         backend_logger.info(f"{request.client.host} {request.method} {request.url}  [0]")
#         return response


class SuppressNoResponseReturnedMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            return await call_next(request)
        except RuntimeError as exc:
            if str(exc) == 'No response returned.' and await request.is_disconnected():
                return Response(status_code=status.HTTP_204_NO_CONTENT)
            raise
