# Arangodantic

[![Build Status](https://travis-ci.com/ioxiocom/arangodantic.svg?branch=main)](https://travis-ci.com/ioxiocom/arangodantic)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PyPI](https://img.shields.io/pypi/v/arangodantic)](https://pypi.org/project/arangodantic/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/arangodantic)](https://pypi.org/project/arangodantic/)
[![License: BSD 3-Clause](https://img.shields.io/badge/License-BSD%203--Clause-blue.svg)](https://opensource.org/licenses/BSD-3-Clause)

Database models for ArangoDB using Pydantic base models.

## Installation

The package is available on PyPI:

```bash
pip install arangodantic

# Or with Shylock
pip install arangodantic[shylock]
```

## Usage

Define your database models by extending either `DocumentModel` or `EdgeModel`. Nested
structures can be created by extending pydantic.BaseModel.

Configure Arangodantic. You can optionally define a collection name prefix, a key
generation function and a lock (needed if you want to use the locking functionality;
[Shylock](https://github.com/lietu/shylock) is supported out of the box, any other
locking service such as [Sherlock](https://pypi.org/project/sherlock/) should at least
in theory also work).

Ensure you have an ArangoDB server available with known credentials

```bash
docker run --rm -p 8529:8529 -e ARANGO_ROOT_PASSWORD="" arangodb/arangodb:3.7.2.1
```

```python
import asyncio
from uuid import uuid4

from aioarangodb import ArangoClient
from pydantic import BaseModel
from shylock import AsyncLock as Lock
from shylock import ShylockAioArangoDBBackend
from shylock import configure as configure_shylock

from arangodantic import ASCENDING, DocumentModel, EdgeModel, configure


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

    type: str


async def main():
    # Configure the database settings
    hosts = "http://localhost:8529"
    username = "root"
    password = ""
    database = "example"
    prefix = "example-"

    client = ArangoClient(hosts=hosts)
    # Connect to "_system" database and create the actual database if it doesn't exist
    # Only for demo, you likely want to create the database in advance.
    sys_db = await client.db("_system", username=username, password=password)
    if not await sys_db.has_database(database):
        await sys_db.create_database(database)

    # Configure Arangodantic and Shylock
    db = await client.db(database, username=username, password=password)
    configure_shylock(await ShylockAioArangoDBBackend.create(db, f"{prefix}shylock"))
    configure(db, prefix=prefix, key_gen=uuid4, lock=Lock)

    # Create collections if they don't yet exist
    # Only for demo, you likely want to create the collections in advance.
    await Company.ensure_collection()
    await Link.ensure_collection()

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
    async with (await Company.find({"owner.last_name": "Doe"}, count=True)) as cursor:
        print(f"Found {len(cursor)} companies")
        async for found_company in cursor:
            print(f"Company: {found_company.company_id}")

    # Supported operators include: "==", "!=", "<", "<=", ">", ">="
    found_company = await Company.find_one(
        {"owner.last_name": "Doe", "_id": {"!=": company}}
    )
    print(f"Found the company {found_company.key_}")

    # Find also supports sorting and the cursor can easily be converted to a list
    companies = await (
        await Company.find(
            sort=[
                ("owner.last_name", ASCENDING),
                ("owner.first_name", ASCENDING),
            ]
        )
    ).to_list()
    print("Companies sorted by owner:")
    for c in companies:
        print(f"Company {c.company_id}, owner: {c.owner!r}")


if __name__ == "__main__":
    # Starting from Python 3.7 ->
    # asyncio.run(main())

    # Compatible with Python 3.6 ->
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())
```

You might find [migrate-anything](https://github.com/cocreators-ee/migrate-anything)
useful for creating and managing collections and indexes.

## More examples

- The [graph example](examples/graph_example.py) shows how arangodantic can be used with
  graphs. Please note that graph related functionality is at the moment really limited
  and will likely be extended later and might even be restructured, so use with caution.

## License

This code is released under the BSD 3-Clause license. Details in the
[LICENSE](./LICENSE) file.
