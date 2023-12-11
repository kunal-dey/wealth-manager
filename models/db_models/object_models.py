from models.db_models.db_functions import jsonify
from utils.nr_db import connect_to_collection


def get_save_to_db(collection_name: str, model_as_self):
    async def save_to_db():
        """
            function to insert the object into the database
        """
        with connect_to_collection(collection_name) as collection:
            await collection.insert_one(jsonify(model_as_self))
    return save_to_db


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
            json_data = jsonify(model_as_self)
            del json_data["_id"]  # updating other values and not the id as it is fixed for a holding
            await collection.update_one({'_id': getattr(model_as_self, "_id")}, {'$set': json_data})
    return update_in_db
