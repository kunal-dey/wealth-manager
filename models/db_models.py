import inspect
from enum import EnumType
from typing import Any

from utils.nr_db import connect_to_collection


def jsonify(dataclass_obj):
    schema: dict = dataclass_obj.schema
    json_data = {}
    for key in schema.keys():
        if isinstance(schema[key], EnumType):
            json_data[key] = getattr(dataclass_obj, key).name
        elif inspect.isclass(schema[key]):
            json_data[key] = jsonify(getattr(dataclass_obj, key))
        else:
            json_data[key] = getattr(dataclass_obj, key)
    return json_data


def objectify(dataclass_obj, data):
    final_obj = dataclass_obj(**data)
    for key in final_obj.schema.keys():
        if inspect.isclass(final_obj.schema[key]):
            if isinstance(final_obj.schema[key], EnumType):
                setattr(final_obj, key, final_obj.schema[key][str(getattr(final_obj, key))])
            else:
                setattr(final_obj, key, final_obj.schema[key](**getattr(final_obj, key)))
    return final_obj


def get_save_to_db(collection_name: str, model_as_self):
    async def save_to_db():
        """
            function to insert the object into the database
        """
        with connect_to_collection(collection_name) as collection:
            await collection.insert_one(jsonify(model_as_self))
    return save_to_db


async def find_by_name(collection_name: str, model_as_cls, search_dict):
    """
        This function is used to find a collection by trade symbol

        Note: Here object id is taken as string and code is written for 2 layers of nesting
    """
    with connect_to_collection(collection_name) as collection:
        data = await collection.find_one(search_dict)
        return objectify(model_as_cls, data) if data else None


def get_delete_from_db(collection_name: str, model_as_self):
    async def delete_from_db():
        """
            This function is used to delete the document from collection
        """
        with connect_to_collection(collection_name) as collection:
            await collection.delete_one({'_id': getattr(model_as_self, "_id")})

    return delete_from_db


def get_update_in_db(collection_name: str, model_as_self):
    async def update_in_db():
        """
            This function is used to update fields of banner
        """
        with connect_to_collection(collection_name) as collection:
            await collection.update_one({'_id': getattr(model_as_self, "_id")}, {'$set': jsonify(model_as_self)})
    return update_in_db


async def retrieve_all_services(collection_name, model_as_cls):
    """
        If limit or skip is provided then it provides that many element
        Otherwise it provides total list of document
    """
    document_list = []
    with connect_to_collection(collection_name) as collection:
        cursor = collection.find({})
        async for document in cursor:
            document_list.append(objectify(model_as_cls, document) if document else None)
        return document_list


