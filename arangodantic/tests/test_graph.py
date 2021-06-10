from asyncio import gather
from uuid import uuid4

import pytest

from arangodantic import GraphNotFoundError, ModelNotFoundError, UniqueConstraintError
from arangodantic.tests.conftest import (
    Person,
    Relation,
    RelationGraph,
    SecondaryRelation,
    SecondaryRelationGraph,
)


@pytest.mark.asyncio
async def test_save_through_graph(relation_graph):
    alice = Person(name="Alice")
    bob = Person(name="Bob")

    await RelationGraph.save(alice)
    assert alice.key_
    assert alice.rev_
    await RelationGraph.save(bob)

    alice.name = "Alice in Wonderland"
    await RelationGraph.save(alice)
    await alice.reload()
    assert alice.name == "Alice in Wonderland"

    ab = Relation(_from=alice, _to=bob, kind="BFF")
    await RelationGraph.save(ab)
    assert ab.key_
    assert ab.rev_
    ab.kind = "BF"
    await RelationGraph.save(ab)
    await ab.reload()
    assert ab.kind == "BF"


@pytest.mark.asyncio
async def test_save_through_graph_model_not_found(relation_graph):
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    cecil = Person(name="Cecil")

    await RelationGraph.save(alice)
    await RelationGraph.save(bob)
    await RelationGraph.save(cecil)

    # Saving a new edge through graph should fail if one of the vertices has been
    # deleted
    bob_id = bob.id_
    await bob.delete()
    ab = Relation(_from=alice, _to=bob_id, kind="BFF")
    with pytest.raises(ModelNotFoundError):
        await RelationGraph.save(ab)

    # Updating an existing edge should fail if one of the vertices has been deleted
    ac = Relation(_from=alice, _to=cecil, kind="BFF")
    await RelationGraph.save(ac)
    await cecil.delete()
    with pytest.raises(ModelNotFoundError):
        await RelationGraph.save(ac)


@pytest.mark.asyncio
async def test_unique_constraint_graph(relation_graph):
    # Create unique index on the "name" field.
    await Person.get_collection().add_hash_index(fields=["name"], unique=True)

    pre_generated_key = str(uuid4())

    person = Person(name="John Doe", _key=pre_generated_key)
    await RelationGraph.save(person)

    assert person.rev_ is not None

    loaded_person = await Person.load(pre_generated_key)
    assert loaded_person.key_ == pre_generated_key

    # Colliding primary key
    person_2 = Person(name="Jane Doe", _key=pre_generated_key)
    with pytest.raises(UniqueConstraintError):
        await RelationGraph.save(person_2)

    person_2.key_ = None
    await person_2.save()

    # Colliding "name"
    person_3 = Person(name="Jane Doe")
    with pytest.raises(UniqueConstraintError):
        await RelationGraph.save(person_3)

    person_3.name = "Jane Jr. Doe"
    await RelationGraph.save(person_3)

    with pytest.raises(UniqueConstraintError):
        person_3.name = "Jane Doe"
        await RelationGraph.save(person_3)


@pytest.mark.asyncio
async def test_deletion_through_graph(relation_graph, secondary_relation_graph):
    # Create some example persons
    alice = Person(name="Alice")
    bob = Person(name="Bob")
    malory = Person(name="Malory")
    await gather(alice.save(), bob.save(), malory.save())

    # Create some edges between them in the primary graph
    ab = Relation(_from=alice, _to=bob, kind="BFF")
    await ab.save()
    am = Relation(_from=alice, _to=malory, kind="hates")
    await am.save()

    # Create some edges between them in the secondary graph
    ab2 = SecondaryRelation(_from=alice, _to=bob, kind="knows")
    await ab2.save()
    am2 = SecondaryRelation(_from=alice, _to=malory, kind="hates")
    await am2.save()

    # Delete document normally (not using graph) and check edges were left in place
    assert await malory.delete()
    await am.reload()
    await am2.reload()
    # Clean up orphaned edges manually
    assert await RelationGraph.delete(am)
    assert await SecondaryRelationGraph.delete(am2)
    assert not await RelationGraph.delete(am, ignore_missing=True)
    assert not await SecondaryRelationGraph.delete(am2, ignore_missing=True)

    # Delete document through graph and verify edges were deleted from both graphs.
    assert await RelationGraph.delete(bob)
    with pytest.raises(ModelNotFoundError):
        await ab.reload()
    with pytest.raises(ModelNotFoundError):
        await ab2.reload()
    with pytest.raises(ModelNotFoundError):
        await RelationGraph.delete(ab)
    with pytest.raises(ModelNotFoundError):
        await SecondaryRelationGraph.delete(ab2)
    assert not await RelationGraph.delete(ab, ignore_missing=True)
    assert not await SecondaryRelationGraph.delete(ab2, ignore_missing=True)

    assert not await RelationGraph.delete(bob, ignore_missing=True)
    with pytest.raises(ModelNotFoundError):
        await RelationGraph.delete(bob)


@pytest.mark.asyncio
async def test_delete_graph(relation_graph):
    assert await RelationGraph.delete_graph()
    with pytest.raises(GraphNotFoundError):
        assert not await RelationGraph.delete_graph()
    assert not await RelationGraph.delete_graph(ignore_missing=True)

    assert await RelationGraph.get_db().has_collection(Person.get_collection_name())
    assert await RelationGraph.get_db().has_collection(Relation.get_collection_name())

    await RelationGraph.ensure_graph()
    assert await RelationGraph.delete_graph(drop_collections=True)
    with pytest.raises(GraphNotFoundError):
        assert not await RelationGraph.delete_graph()
    assert not await RelationGraph.get_db().has_collection(Person.get_collection_name())
    assert not await RelationGraph.get_db().has_collection(
        Relation.get_collection_name()
    )
