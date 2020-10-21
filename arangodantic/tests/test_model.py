from asyncio import gather
from uuid import uuid4

import pytest
from aioarangodb import CursorCountError

from arangodantic import ModelNotFoundError, UniqueConstraintError
from arangodantic.tests.conftest import ExtendedIdentity, Identity, Link, SubModel


@pytest.mark.asyncio
async def test_save_and_load_model(identity_collection):
    identity = Identity(name="John Doe")
    await identity.save()

    assert identity.key_ is not None
    assert identity.rev_ is not None

    loaded_identity = await Identity.load(identity.key_)
    assert identity.key_ == loaded_identity.key_


@pytest.mark.asyncio
async def test_unique_constraint(identity_collection):
    # Create unique index on the "name" field.
    await Identity.get_collection().add_hash_index(fields=["name"], unique=True)

    pre_generated_key = str(uuid4())

    identity = Identity(name="John Doe", _key=pre_generated_key)
    await identity.save()

    assert identity.rev_ is not None

    loaded_identity = await Identity.load(pre_generated_key)
    assert loaded_identity.key_ == pre_generated_key

    # Colliding primary key
    identity_2 = Identity(name="Jane Doe", _key=pre_generated_key)
    with pytest.raises(UniqueConstraintError):
        await identity_2.save()

    identity_2.key_ = None
    await identity_2.save()

    # Colliding "name"
    identity_3 = Identity(name="Jane Doe")
    with pytest.raises(UniqueConstraintError):
        await identity_3.save()

    identity_3.name = "Jane Jr. Doe"
    await identity_3.save()

    with pytest.raises(UniqueConstraintError):
        identity_3.name = "Jane Doe"
        await identity_3.save()


@pytest.mark.asyncio
async def test_delete_model(identity_collection):
    identity = Identity(name="Jane Doe")
    await identity.save()
    assert await identity.delete() is True

    with pytest.raises(ModelNotFoundError):
        await identity.delete()

    assert await identity.delete(ignore_missing=True) is False


@pytest.mark.asyncio
async def test_reload(identity_collection):
    identity = Identity(name="Jane Doe")
    with pytest.raises(ModelNotFoundError):
        await identity.reload()

    await identity.save()

    loaded_identity = await Identity.load(identity.key_)
    assert identity.key_ == loaded_identity.key_

    identity.name = "Jane Austen"
    await identity.save()

    assert loaded_identity.name == "Jane Doe"
    await loaded_identity.reload()
    assert loaded_identity.name == "Jane Austen"


@pytest.mark.asyncio
async def test_locking(identity_collection):
    identity = Identity(name="James Doe")
    await identity.save()

    second_lock = Identity.get_lock(identity.key_)

    # Lock and reload the identity
    async with identity.lock_and_reload():
        # Verify the lock is held
        assert await second_lock.acquire(block=False) is False

    # Verify the lock is not held any longer
    try:
        assert await second_lock.acquire(block=False) is True
    finally:
        await second_lock.release()

    # Lock and load as new identity
    async with Identity.lock_and_load(identity.key_) as _:
        # Verify the lock is held
        assert await second_lock.acquire(block=False) is False

    # Verify the lock is not held any longer
    try:
        assert await second_lock.acquire(block=False) is True
    finally:
        await second_lock.release()


@pytest.mark.asyncio
async def test_find(identity_collection):
    i_x = Identity(name="Do not find me")
    await i_x.save()

    i_1 = Identity(name="John Doe")
    await i_1.save()

    i_y = Identity(name="Do not find me either")
    await i_y.save()

    i_2 = Identity(name="James Doe")
    await i_2.save()

    i_3 = Identity(name="James Doe")
    await i_3.save()

    async with (await Identity.find({"name": "John Doe"})) as cursor:
        results = [i async for i in cursor]

    assert len(results) == 1
    assert i_1.key_ == results[0].key_
    assert i_1.name == results[0].name

    async with (await Identity.find({"name": "James Doe"})) as cursor:
        results = [i async for i in cursor]

    assert len(results) == 2
    for r in results:
        assert r.name == "James Doe"

    with pytest.raises(CursorCountError):
        len(cursor)

    async with (await Identity.find({"name": "James Doe"}, count=True)) as cursor:
        assert len(cursor) == 2


@pytest.mark.asyncio
async def test_find_with_comparisons(identity_collection):
    i_a = Identity(name="a")
    i_a2 = Identity(name="a")
    i_b = Identity(name="b")
    i_c = Identity(name="c")

    await gather(i_a.save(), i_a2.save(), i_b.save(), i_c.save())

    cursor = await (Identity.find({"name": "a"}, count=True))
    async with cursor:
        assert len(cursor) == 2
        async for i in cursor:
            assert i.name == "a"

    cursor = await (Identity.find({"name": {"<": "a"}}, count=True))
    async with cursor:
        assert len(cursor) == 0

    cursor = await (Identity.find({"name": {"<=": "a"}}, count=True))
    async with cursor:
        assert len(cursor) == 2
        async for i in cursor:
            assert i.name == "a"

    cursor = await (Identity.find({"name": {">": "c"}}, count=True))
    async with cursor:
        assert len(cursor) == 0

    cursor = await (Identity.find({"name": {">": "b"}}, count=True))
    async with cursor:
        assert len(cursor) == 1
        async for i in cursor:
            assert i.name == "c"

    cursor = await (Identity.find({"name": {">=": "b"}}, count=True))
    async with cursor:
        assert len(cursor) == 2
        async for i in cursor:
            assert i.name in {"b", "c"}

    cursor = await (Identity.find({"name": {">": "a", "<": "c"}}, count=True))
    async with cursor:
        assert len(cursor) == 1
        async for i in cursor:
            assert i.name == "b"

    cursor = await (Identity.find({"name": "a", "_id": {"!=": i_a}}, count=True))
    async with cursor:
        assert len(cursor) == 1
        async for i in cursor:
            assert i.id_ == i_a2.id_


@pytest.mark.asyncio
async def test_find_one(identity_collection):
    i = Identity(name="John Doe")
    await i.save()

    i_found = await Identity.find_one({"name": "John Doe"})

    assert i.key_ == i_found.key_

    horribly_bad_str = "'`´ \"$&=?+._"

    with pytest.raises(ModelNotFoundError):
        await Identity.find_one({"name": horribly_bad_str})

    with pytest.raises(ModelNotFoundError):
        await Identity.find_one({horribly_bad_str: "John Doe"})


@pytest.mark.asyncio
async def test__before_save(extended_identity_collection):
    identity = ExtendedIdentity(name="John Doe")
    await identity.save(extra="foo")
    identity = await ExtendedIdentity.load(identity.key_)
    assert identity.extra == "foo"


@pytest.mark.asyncio
async def test_sub_models(extended_identity_collection):
    sub = SubModel(text="foo")
    identity = ExtendedIdentity(name="John Doe", sub=sub)
    await identity.save()

    identity = await ExtendedIdentity.load(identity.key_)
    assert isinstance(identity.sub, SubModel)
    assert identity.sub.text == "foo"

    identity.sub = None
    await identity.reload()
    assert isinstance(identity.sub, SubModel)
    assert identity.sub.text == "foo"


@pytest.mark.asyncio
async def test_find_with_sub_models(extended_identity_collection):
    sub_1 = SubModel(text="foo")
    identity_1 = ExtendedIdentity(name="John Doe", sub=sub_1)
    await identity_1.save()

    sub_2 = SubModel(text="bar")
    identity_2 = ExtendedIdentity(name="John Doe", sub=sub_2)
    await identity_2.save()

    async with (await ExtendedIdentity.find({"sub.text": "foo"}, count=True)) as cursor:
        assert len(cursor) == 1
        found = await cursor.next()
        assert found.key_ == identity_1.key_


@pytest.mark.asyncio
async def test_edge_model(
    identity_collection, link_collection, identity_alice, identity_bob
):
    link = Link(_from=identity_alice, _to=identity_bob, type="Knows")
    await link.save()

    await link.reload()
    assert link.from_ == identity_alice.id_
    assert link.to_ == identity_bob.id_


@pytest.mark.asyncio
async def test_find_one_edge_model(
    identity_collection, link_collection, identity_alice, identity_bob
):
    ab = Link(_from=identity_alice, _to=identity_bob, type="Knows")
    await ab.save()

    with pytest.raises(ModelNotFoundError):
        await Link.find_one({"_from": identity_bob, "_to": identity_alice})

    assert (
        ab.key_
        == (await Link.find_one({"_from": identity_alice, "_to": identity_bob})).key_
    )
