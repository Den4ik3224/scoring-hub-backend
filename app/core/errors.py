from dataclasses import dataclass

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette import status


@dataclass
class ApiError(Exception):
    code: str
    message: str
    http_status: int = status.HTTP_400_BAD_REQUEST


class NotFoundError(ApiError):
    def __init__(self, message: str, code: str = "not_found") -> None:
        super().__init__(code=code, message=message, http_status=status.HTTP_404_NOT_FOUND)


class ConflictError(ApiError):
    def __init__(self, message: str, code: str = "conflict") -> None:
        super().__init__(code=code, message=message, http_status=status.HTTP_409_CONFLICT)


class ValidationError(ApiError):
    def __init__(self, message: str, code: str = "validation_error") -> None:
        super().__init__(code=code, message=message, http_status=status.HTTP_422_UNPROCESSABLE_ENTITY)


class AuthError(ApiError):
    def __init__(self, message: str = "Authentication required", code: str = "unauthorized") -> None:
        super().__init__(code=code, message=message, http_status=status.HTTP_401_UNAUTHORIZED)


class PermissionError(ApiError):
    def __init__(self, message: str = "Insufficient permissions", code: str = "forbidden") -> None:
        super().__init__(code=code, message=message, http_status=status.HTTP_403_FORBIDDEN)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def api_error_handler(_: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content={"error": {"code": exc.code, "message": exc.message}})

    @app.exception_handler(HTTPException)
    async def http_error_handler(_: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": "http_error", "message": str(exc.detail)}},
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(_: Request, exc: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": {"code": "internal_error", "message": "Unexpected error"}},
        )
