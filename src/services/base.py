from collections.abc import Sequence
from typing import (
    Generic,
    TypeVar,
)

from sqlalchemy.sql.expression import Executable

from db.session import AsyncSession


class SessionMixin:
    """Provides instance of database session."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session


T = TypeVar("T")


class BaseService(SessionMixin, Generic[T]):  # noqa: UP046
    """Base class for application services. Owns the transaction boundary."""


class BaseDataManager(SessionMixin, Generic[T]):  # noqa: UP046
    """Database operations for one model. Flushes, never commits."""

    async def add_one(self, model: T) -> T:
        self.session.add(model)
        await self.session.flush()
        await self.session.refresh(model)
        return model

    async def add_all(self, models: Sequence[T]) -> None:
        self.session.add_all(models)
        await self.session.flush()

    async def get_one(self, select_stmt: Executable) -> T | None:
        return await self.session.scalar(select_stmt)

    async def get_all(self, select_stmt: Executable) -> list[T]:
        return list((await self.session.scalars(select_stmt)).all())

    async def update(self, model: T) -> T:
        await self.session.flush()
        await self.session.refresh(model)
        return model
