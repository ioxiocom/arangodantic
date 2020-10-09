import random
import string
from typing import Optional
from uuid import uuid4

import pydantic
import pytest
from aioarangodb import ArangoClient
from shylock import AsyncLock as Lock
from shylock import ShylockAioArangoDBBackend
from shylock import configure as configure_shylock

from arangodantic import DocumentModel, EdgeModel, configure

HOSTS = "http://localhost:8529"
USERNAME = "root"
PASSWORD = ""
DATABASE = "test"


@pytest.fixture
async def configure_db():
    def rand_str(length: int) -> str:
        """
        Generate a random string for collection names.

        :param length: The length of the random string.
        :return: The random prefix string.
        """
        chars = string.ascii_letters + string.digits
        return "".join(random.choice(chars) for _ in range(length))

    prefix = f"test-{rand_str(10)}"

    client = ArangoClient(hosts=HOSTS)
    # Connect to "_system" database and create the actual database if it doesn't exist
    sys_db = await client.db("_system", username=USERNAME, password=PASSWORD)
    if not await sys_db.has_database(DATABASE):
        await sys_db.create_database(DATABASE)

    db = await client.db(DATABASE, username=USERNAME, password=PASSWORD)
    configure_shylock(await ShylockAioArangoDBBackend.create(db, f"{prefix}-shylock"))
    configure(db, prefix=f"{prefix}-", key_gen=uuid4, lock=Lock)

    yield

    await db.delete_collection(f"{prefix}-shylock")
    await client.close()


class Identity(DocumentModel):
    """Dummy identity Arangodantic model."""

    name: str = ""


class SubModel(pydantic.BaseModel):
    """Dummy plain pydantic sub-model."""

    text: str = ""


class ExtendedIdentity(Identity):
    """Dummy extended identity Arangodantic model."""

    extra: Optional[str] = None
    sub: Optional[SubModel] = None

    class ArangodanticConfig:
        collection_name = "ext_identities"

    async def _before_save(self, new: bool, extra: Optional[str] = None, **kwargs):
        self.extra = extra


class Link(EdgeModel):
    """Dummy Arangodantic edge model."""

    type: str


@pytest.fixture
async def identity_collection(configure_db):
    await Identity.ensure_collection()
    yield
    await Identity.delete_collection()


@pytest.fixture
async def extended_identity_collection(configure_db):
    await ExtendedIdentity.ensure_collection()
    yield
    await ExtendedIdentity.delete_collection()


@pytest.fixture
async def link_collection(configure_db):
    await Link.ensure_collection()
    yield
    await Link.delete_collection()
