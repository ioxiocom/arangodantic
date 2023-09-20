import asyncio
from os import getenv
from uuid import uuid4

import asyncer
from arango import ArangoClient
from asyncer import asyncify
from orjson import orjson
from pydantic import BaseModel
from shylock import AsyncLock as Lock
from shylock import configure as configure_shylock

from arangodantic import ASCENDING, DocumentModel, EdgeModel, configure
from arangodantic.backends.asyncer_python_arango_backend import ShylockAsyncerArangoDBBackend


# Define models
class Owner(BaseModel):
    """Dummy owner Pydantic model."""

    first_name: str
    last_name: str


class Company(DocumentModel):
    """Dummy company Arangodantic model."""

    company_id: str
    owner: Owner


class Link(EdgeModel):
    """Dummy Link Arangodantic model."""

    key_: str = None
    type: str

    # @field_serializer("key_")
    # def serialize_key(self, key, _info):
    #     if key is None:
    #         return
    #     elif isinstance(key, uuid.UUID):
    #         return str(uuid)
    #     else:
    #         return key


async def main():
    # Configure the database settings
    hosts = getenv("HOSTS")
    username = getenv("USERNAME")
    password = getenv("PASSWORD")
    database = "example"
    prefix = "example-"
    # Create the ArangoDB client with orjson as serializer and deserializer
    client = ArangoClient(
        hosts=hosts, serializer=orjson.dumps, deserializer=orjson.loads
    )
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

    # Create collections if they don't yet exist
    # Only for demo, you likely want to create the collections in advance.
    async with asyncer.create_task_group() as tg:
        tg.soonify(Company.ensure_collection)()
        tg.soonify(Link.ensure_collection)()

    # Let's create some example entries
    owner = Owner(first_name="John", last_name="Doe")
    company = Company(company_id="1234567-8", owner=owner)
    await company.save()
    print(f"Company saved with key: {company.key_}")

    second_owner = Owner(first_name="Jane", last_name="Doe")
    second_company = Company(company_id="2345678-9", owner=second_owner)
    await second_company.save()
    print(f"Second company saved with key: {second_company.key_}")

    link = Link(_from=company, _to=second_company, type="CustomerOf")
    print(link)
    await link.save()
    print(f"Link saved with key: {link.key_}")

    # Hold named locks while loading and doing changes
    async with Company.lock_and_load(company.key_) as c:
        assert c.owner == owner
        c.owner.first_name = "Joanne"
        await c.save()

    await company.reload()
    print(f"Updated owner of company to '{company.owner!r}'")

    # Let's explore the find functionality
    # Note: You likely want to add indexes to support the queries
    print("Finding companies owned by a person with last name 'Doe'")
    # async with asyncer.create_task_group() as tg:
    #     soon = tg.soonify(Company.find)({"owner.last_name": "Doe"}, count=True)

    companies = await (
        await Company.find({"owner.last_name": "Doe"}, count=True)
    ).to_list()
    print(f"Found {len(companies)} companies")
    # Cave return as dict at the moment
    # Todo return as list of models
    print(f"Found companies\n {companies}")

    # Supported operators include: "==", "!=", "<", "<=", ">", ">="
    found_company = await Company.find_one(
        {"owner.last_name": "Doe", "_id": {"!=": company}}
    )
    print(f"Found the company {found_company.key_}")
    print(f"Found the company {type(found_company)}")

    # Find also supports sorting and the cursor can easily be converted to a list

    companies = await (
        await Company.find(
            sort=[("owner.last_name", ASCENDING), ("owner.first_name", ASCENDING)]
        )
    ).to_list()
    print(f"comps {companies}")
    print(f"type {type(companies)}")
    print("Companies sorted by owner:")
    for c in companies:
        company = Company(**c)
        print(f"Company {company.company_id}, owner: {company.owner!r}")


if __name__ == "__main__":
    asyncio.run(main())
