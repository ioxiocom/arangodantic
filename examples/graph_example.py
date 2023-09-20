import asyncio
from asyncio import gather
from os import getenv
from uuid import uuid4

from arango import ArangoClient
from asyncer import asyncify, create_task_group
from shylock import AsyncLock as Lock
from shylock import configure as configure_shylock

from arangodantic import (
    DocumentModel,
    EdgeDefinition,
    EdgeModel,
    GraphModel,
    ModelNotFoundError,
    configure,
)
from arangodantic.backends.asyncer_python_arango_backend import ShylockAsyncerArangoDBBackend


# Define models
class Person(DocumentModel):
    """Documents describing persons."""

    name: str


class Relation(EdgeModel):
    """Edge documents describing relation between people."""

    kind: str


class SecondaryRelation(EdgeModel):
    """Edge documents describing a secondary relation between people."""

    kind: str


# Define the graph
class RelationGraph(GraphModel):
    class ArangodanticConfig:
        edge_definitions = [
            EdgeDefinition(
                edge_collection=Relation,
                from_vertex_collections=[Person],
                to_vertex_collections=[Person],
            )
        ]


async def main():
    # Configure the database settings
    hosts = getenv("HOSTS")
    username = getenv("USERNAME")
    password = getenv("PASSWORD")
    database = "example"
    prefix = "example-"

    client = ArangoClient(hosts=hosts)
    # Connect to "_system" database and create the actual database if it doesn't exist
    # Only for demo, you likely want to create the database in advance.
    sys_db = await asyncify(client.db)("_system", username=username, password=password)
    if not await asyncify(sys_db.has_database)(database):
        await asyncify(sys_db.create_database)(database)

    # Configure Arangodantic and Shylock
    db = await asyncify(client.db)(database, username=username, password=password)
    configure_shylock(
        await ShylockAsyncerArangoDBBackend.create(db, f"{prefix}shylock")
    )
    configure(db, prefix=prefix, key_gen=uuid4, lock=Lock)

    # Create the graph (it'll also create the collections)
    # Only for demo, you likely want to create the graph in advance.
    async with create_task_group() as tg:
        tg.soonify(Person.ensure_collection)()
        tg.soonify(RelationGraph.ensure_graph)()

    # Let's create some example persons
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    malory = Person(name="Malory")
    await gather(alice.save(), bob.save(), malory.save())

    ab = Relation(_from=alice, _to=bob, kind="BFF")
    # named EdgeModel _key
    # ab.key_ = "1"

    await ab.save()
    am = Relation(_from=alice, _to=malory, kind="hates")
    await am.save()
    #
    # Deleting using the model will tell ArangoDB to delete the item from the collection
    await malory.delete()
    # ... this won't delete any edges to/from the model.
    await am.reload()
    #
    # Deleting through the graph will tell ArangoDB to delete from the graph
    await RelationGraph.delete(bob)
    # ... in this case ArangoDB will delete any edges to/from the model.
    try:
        await ab.reload()
        raise Exception("Should have been deleted, but wasn't")
    except ModelNotFoundError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
