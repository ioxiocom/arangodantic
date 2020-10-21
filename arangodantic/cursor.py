from typing import Type

from aioarangodb.cursor import Cursor


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
        return self.cls(**(await self.cursor.next()))

    async def __aenter__(self):
        return self

    def __len__(self):
        return len(self.cursor)

    async def __aexit__(self, *args):
        await self.cursor.__aexit__(*args)

    def __repr__(self):
        cursor_id_str = ""
        if self.cursor.id:
            cursor_id_str = f" (Cursor: {self.cursor.id})"
        return f"<ArangodanticCursor ({self.cls.__name__}){cursor_id_str}>"

    async def close(self, ignore_missing=False):
        await self.cursor.close(ignore_missing=ignore_missing)

    async def next(self):
        return self.cls(**(await self.cursor.next()))
