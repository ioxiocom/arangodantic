import pytest

from arangodantic.tests.conftest import Identity


@pytest.mark.asyncio
async def test_to_list(identity_collection, identity_alice, identity_bob):
    identities = await (await Identity.find()).to_list()

    assert len(identities) == 2
    assert any(i.name == "Alice" for i in identities)
    assert any(i.name == "Bob" for i in identities)

    # Can also be expressed like this:
    cursor = await Identity.find()
    identities = await cursor.to_list()

    assert len(identities) == 2
    assert any(i.name == "Alice" for i in identities)
    assert any(i.name == "Bob" for i in identities)
