from typing import List, Optional, Type

from aioarangodb import CursorCloseError
from aioarangodb.cursor import Cursor

from arangodantic.exceptions import CursorError, CursorNotFoundError


class ArangodanticCursor:
    """
    Wrapper for the aioarangodb.cursor.Cursor that will give back instances of the
    defined class rather than dictionaries.
    """

    __slots__ = [
        "cls",
        "cursor",
    ]

    def __init__(self, cls, cursor: Cursor):
        from arangodantic.models import Model

        self.cls: Type[Model] = cls
        self.cursor: Cursor = cursor

    def __aiter__(self):
        return self

    async def __anext__(self):  # pragma: no cover
        return await self.next()

    async def __aenter__(self):
        return self

    def __len__(self):
        return len(self.cursor)

    async def __aexit__(self, *_):
        await self.close(ignore_missing=True)

    def __repr__(self):
        cursor_id_str = ""
        if self.cursor.id:
            cursor_id_str = f" (Cursor: {self.cursor.id})"
        return f"<ArangodanticCursor ({self.cls.__name__}){cursor_id_str}>"

    async def close(self, ignore_missing: bool = False) -> Optional[bool]:
        """
        Close the cursor to free server side resources.

        :param ignore_missing: Do not raise an exception if the cursor is missing on the
        server side.
        :return: True if cursor was closed successfully, False if cursor was missing on
        the server side and **ignore_missing** was True, None if there were no cursors
        to close server-side.
        :raise CursorNotFoundError: If the cursor was missing and **ignore_missing** was
        False.
        """
        try:
            result: Optional[bool] = await self.cursor.close(
                ignore_missing=ignore_missing
            )
        except CursorCloseError as ex:
            if ex.error_code == 404:
                raise CursorNotFoundError(ex.error_message)
            raise

        return result

    async def next(self):
        return self.cls(**(await self.cursor.next()))

    async def to_list(self) -> List:
        """
        Convert the cursor to a list.
        """
        async with self as cursor:
            return [i async for i in cursor]

    @property
    def full_count(self) -> int:
        """
        Get the full count.

        :return: The full count.
        :raise CursorError: If the cursor statistics do not contain the full count.
        """
        stats = self.cursor.statistics()
        try:
            full_count: int = stats["fullCount"]
        except KeyError as e:
            raise CursorError(
                "Cursor statistics has no full count, did you use full_count=True?"
            ) from e

        return full_count
