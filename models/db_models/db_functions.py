import inspect
from enum import EnumType
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


async def find_by_name(collection_name: str, model_as_cls, search_dict):
    """
        This function is used to find a collection by trade symbol

        Note: Here object id is taken as string and code is written for 2 layers of nesting
    """
    with connect_to_collection(collection_name) as collection:
        data = await collection.find_one(search_dict)
        return objectify(model_as_cls, data) if data else None


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
