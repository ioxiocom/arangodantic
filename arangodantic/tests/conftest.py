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

from arangodantic import DocumentModel, EdgeDefinition, EdgeModel, Graph, configure

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

    async def before_save(
        self, new: bool, override_extra: Optional[str] = None, **kwargs
    ):
        if override_extra:
            self.extra = override_extra


class Link(EdgeModel):
    """Dummy Arangodantic edge model."""

    type: str


class Person(DocumentModel):
    """Documents describing persons."""

    name: str


class Relation(EdgeModel):
    """Edge documents describing relation between people."""

    kind: str


class SecondaryRelation(EdgeModel):
    """Edge documents describing a secondary relation between people."""

    kind: str


class RelationGraph(Graph):
    class ArangodanticConfig:
        edge_definitions = [
            EdgeDefinition(
                edge_collection=Relation,
                from_vertex_collections=[Person],
                to_vertex_collections=[Person],
            )
        ]


class SecondaryRelationGraph(Graph):
    class ArangodanticConfig:
        edge_definitions = [
            EdgeDefinition(
                edge_collection=SecondaryRelation,
                from_vertex_collections=[Person],
                to_vertex_collections=[Person],
            )
        ]


@pytest.fixture
async def identity_collection(configure_db):
    await Identity.ensure_collection()
    yield
    await Identity.delete_collection()


@pytest.fixture
async def identity_alice(identity_collection):
    alice = Identity(name="Alice")
    await alice.save()
    yield alice


@pytest.fixture
async def identity_bob(identity_collection):
    bob = Identity(name="Bob")
    await bob.save()
    yield bob


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


@pytest.fixture
async def relation_graph(configure_db):
    await RelationGraph.ensure_graph()
    yield
    await RelationGraph.delete_graph(ignore_missing=True, drop_collections=True)


@pytest.fixture
async def secondary_relation_graph(configure_db):
    await SecondaryRelationGraph.ensure_graph()
    yield
    await SecondaryRelationGraph.delete_graph(
        ignore_missing=True, drop_collections=True
    )
