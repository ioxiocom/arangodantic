from abc import ABC
from functools import lru_cache
from typing import Type

from arango import (
    DocumentDeleteError,
    DocumentInsertError,
    DocumentReplaceError,
    GraphDeleteError,
)
from arango.database import StandardDatabase
from arango.errno import DOCUMENT_NOT_FOUND, GRAPH_NOT_FOUND, UNIQUE_CONSTRAINT_VIOLATED
from arango.graph import Graph
from asyncer import asyncify
from pydantic import BaseModel, Field

from arangodantic import GraphNotFoundError, ModelNotFoundError, UniqueConstraintError
from arangodantic.configurations import CONF
from arangodantic.models import DocumentModel, EdgeModel, Model


class EdgeDefinition(BaseModel):
    edge_collection: Type[EdgeModel]
    from_vertex_collections: list[Type[DocumentModel]]
    to_vertex_collections: list[Type[DocumentModel]]


class ArangodanticGraphConfig(BaseModel):
    graph_name: str | None = Field(
        None, description="Override the name of the graph to use"
    )
    edge_definitions: list[EdgeDefinition] | None = None
    orphan_collections: list[Model] | None = None


class GraphModel(ABC):
    @classmethod
    @lru_cache()
    def get_graph_name(cls) -> str:
        cls_config: ArangodanticGraphConfig = getattr(
            cls, "ArangodanticConfig", ArangodanticGraphConfig()
        )
        if getattr(cls_config, "graph_name", None):
            graph = cls_config.graph_name
        else:
            graph = CONF.graph_generator(cls)  # type: ignore
        return f"{CONF.prefix}{graph}"

    @classmethod
    def get_db(cls) -> StandardDatabase:
        return CONF.db

    @classmethod
    def get_graph(cls) -> Graph:
        return cls.get_db().graph(cls.get_graph_name())

    @classmethod
    async def save(cls, model: DocumentModel | EdgeModel, **kwargs):
        """
        Save the model through the graph; either creates a new one or updates/replaces
        an existing document.

        Doing this through the graph will make ArangoDB enforce some better validation.
        For more details please see
        https://www.arangodb.com/docs/stable/graphs.html#manipulating-collections-of-named-graphs-with-regular-document-functions

        :raise UniqueConstraintViolated: Raised when there is a unique constraint
        violation.
        :raise ModelNotFoundError: If any of the models are not found.
        """
        graph = cls.get_graph()

        if not model.rev_:
            # insert new document
            if not model.key_ and CONF.key_gen:
                # Use generator to generate new key
                model.key_ = str(CONF.key_gen())
            await model.before_save(new=True, **kwargs)
            data = model.get_arangodb_data()
            if model.key_ is None:
                del data["_key"]
                del data["_id"]
            try:
                collection_name = model.get_collection_name()
                if isinstance(model, EdgeModel):
                    response = await asyncify(graph.insert_edge)(collection_name, data)
                else:
                    response = await asyncify(graph.insert_vertex)(
                        collection_name, data
                    )
            except DocumentInsertError as ex:
                if ex.error_code == UNIQUE_CONSTRAINT_VIOLATED:
                    raise UniqueConstraintError(ex.error_message)
                elif ex.error_code == DOCUMENT_NOT_FOUND:
                    raise ModelNotFoundError(ex.error_message)
                raise
        else:
            # Update existing document
            await model.before_save(new=False, **kwargs)
            data = model.get_arangodb_data()
            try:
                if isinstance(model, EdgeModel):
                    response = await asyncify(graph.replace_edge)(data)
                else:
                    response = await asyncify(graph.replace_vertex)(data)
            except DocumentReplaceError as ex:
                if ex.error_code == UNIQUE_CONSTRAINT_VIOLATED:
                    raise UniqueConstraintError(ex.error_message)
                elif ex.error_code == DOCUMENT_NOT_FOUND:
                    raise ModelNotFoundError(ex.error_message)
                raise

        model.key_ = response["_key"]
        model.rev_ = response["_rev"]

    @classmethod
    async def delete_vertex(cls, document: DocumentModel, ignore_missing=False) -> bool:
        """
        Delete a vertex from the graph. This will also ensure all edges to/from the
        vertex are deleted.

        :param document: The document model to delete.
        :param ignore_missing: Do not raise an exception on missing document.
        :raise ModelNotFoundError: Raised if the document is not found in the database
        and ignore_missing is set to False (the default value).
        :return: True if document was deleted successfully, False if document was
        not found and **ignore_missing** was set to True.
        """
        data = document.get_arangodb_data()
        try:
            result: bool = await asyncify(cls.get_graph().delete_vertex)(
                data, ignore_missing=ignore_missing
            )
        except DocumentDeleteError as ex:
            if ex.error_code == DOCUMENT_NOT_FOUND:
                raise ModelNotFoundError(
                    f"No '{document.__class__.__name__}' found with _key "
                    f"'{document.key_}'"
                )
            raise

        return result

    @classmethod
    async def delete_edge(cls, edge: EdgeModel, ignore_missing=False) -> bool:
        """
        Delete an edge from the graph.

        :param edge: The edge model to delete.
        :param ignore_missing: Do not raise an exception on missing document.
        :raise ModelNotFoundError: Raised if the document is not found in the database
        and ignore_missing is set to False (the default value).
        :return: True if edge was deleted successfully, False if edge was
        not found and **ignore_missing** was set to True.
        """
        data = edge.get_arangodb_data()
        try:
            result: bool = await asyncify(cls.get_graph().delete_edge)(
                data, ignore_missing=ignore_missing
            )
        except DocumentDeleteError as ex:
            if ex.error_code == DOCUMENT_NOT_FOUND:
                raise ModelNotFoundError(
                    f"No '{edge.__class__.__name__}' found with _key '{edge.key_}'"
                )
            raise

        return result

    @classmethod
    async def delete(
        cls, model: DocumentModel | EdgeModel, ignore_missing=False
    ) -> bool:
        """
        Delete a model (edge or vertex) from the graph. This will also ensure all edges
        to/from the vertex are deleted.

        :param model: The model to delete.
        :param ignore_missing: Do not raise an exception on missing document.
        :raise ModelNotFoundError: Raised if the document is not found in the database
        and ignore_missing is set to False (the default value).
        :return: True if model was deleted successfully, False if model was
        not found and **ignore_missing** was set to True.
        """
        if isinstance(model, EdgeModel):
            return await cls.delete_edge(model, ignore_missing=ignore_missing)
        else:
            return await cls.delete_vertex(model, ignore_missing=ignore_missing)

    @classmethod
    async def ensure_graph(cls, **kwargs):
        """
        Ensure the graph exists and create it if needed.
        """
        cls_config: ArangodanticGraphConfig = getattr(
            cls, "ArangodanticConfig", ArangodanticGraphConfig()
        )

        def get_edge_definitions() -> list[dict[str | list[str]]]:
            edge_definitions = getattr(cls_config, "edge_definitions", None)
            if not edge_definitions:
                edge_definitions = []
            # todo check ed as edge_definitions
            return [
                {
                    "edge_collection": ed.edge_collection.get_collection_name(),
                    "from_vertex_collections": [
                        vc.get_collection_name() for vc in ed.from_vertex_collections
                    ],
                    "to_vertex_collections": [
                        vc.get_collection_name() for vc in ed.to_vertex_collections
                    ],
                }
                for ed in edge_definitions
            ]

        def get_orphan_collections() -> list[str]:
            orphan_collections = getattr(cls_config, "orphan_collections", None)
            if not orphan_collections:
                orphan_collections = []
            return [oc.get_collection_name() for oc in orphan_collections]

        name = cls.get_graph_name()
        db = cls.get_db()

        if not await asyncify(db.has_graph)(name):
            await asyncify(db.create_graph)(
                name,
                edge_definitions=get_edge_definitions(),
                orphan_collections=get_orphan_collections(),
                **kwargs,
            )

    @classmethod
    async def delete_graph(cls, ignore_missing: bool = False, drop_collections=None):
        """
        Delete the graph if it exists.

        :param ignore_missing: Do not raise an exception on missing graph.
        :param drop_collections: Drop the collections of the graph also. This
        is only if they are not in use by other graphs.
        :raise GraphNotFoundError: If the graph does not exist and **ignore_missing** is
        False
        """
        name = cls.get_graph_name()
        db = cls.get_db()

        try:
            return await asyncify(db.delete_graph)(
                name, ignore_missing=ignore_missing, drop_collections=drop_collections
            )
        except GraphDeleteError as ex:
            if ex.error_code == GRAPH_NOT_FOUND:
                raise GraphNotFoundError(f"No graph found with name '{name}'")
            raise
