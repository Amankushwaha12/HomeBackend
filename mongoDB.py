from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo import MongoClient, ReturnDocument
from datetime import datetime


uri = "mongodb+srv://Aman:@cluster0.qge6pof.mongodb.net/?appName=Cluster0"
# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["ListManager"]
users_collection = db["List"]


def create_user(user_id, username):
    """
    Create a new user document with empty products list.
    `user_id` is required.
    """
    if not user_id:
        raise ValueError("user_id is required")

    user_doc = {
        "user_id": user_id,
        "username": username,
        "products": []
    }
    users_collection.insert_one(user_doc)
    print(f"User '{user_id}' created.")

def put_product(user_id, product):
    """
    Add a product dictionary to the user's products list.
    If product with same name exists, update it.
    product dict must have keys: name, category, mfg, exp
    """
    # Convert mfg and exp strings to datetime if they are string
    for date_key in ["mfg", "exp"]:
        if isinstance(product.get(date_key), str):
            product[date_key] = datetime.strptime(product[date_key], "%Y-%m-%d")

    # Update the product if exists, else add new one
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        raise ValueError("User not found.")

    product_names = [p["name"] for p in user["products"]]
    if product["name"] in product_names:
        # Update existing product
        users_collection.update_one(
            {"user_id": user_id, "products.name": product["name"]},
            {"$set": {"products.$": product}}
        )
        print(f"Product '{product['name']}' updated for user '{user_id}'.")
    else:
        # Add new product
        users_collection.update_one(
            {"user_id": user_id},
            {"$push": {"products": product}}
        )
        print(f"Product '{product['name']}' added for user '{user_id}'.")

def delete_product(user_id, product_name):
    """
    Delete a product by name from user's products list.
    """
    result = users_collection.update_one(
        {"user_id": user_id},
        {"$pull": {"products": {"name": product_name}}}
    )
    if result.modified_count > 0:
        print(f"Product '{product_name}' removed from user '{user_id}'.")
    else:
        print(f"Product '{product_name}' not found for user '{user_id}'.")

def find_products(user_id, name=None, category=None, mfg=None, exp=None):
    """
    Find matching products for a user based on the provided fields.
    Returns a list of product dictionaries.
    Safety we have used status string for checking status of the code execution.(index -1)
    """
    match_query = []
    if name:
        match_query.append({"name": name})
    if category:
        match_query.append({"category": category})
    if mfg:
        if isinstance(mfg, str):
            mfg = datetime.strptime(mfg, "%Y-%m-%d")
        match_query.append({"mfg": mfg})
    if exp:
        if isinstance(exp, str):
            exp = datetime.strptime(exp, "%Y-%m-%d")
        match_query.append({"exp": exp})

    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return [{"status": "User not found."}]

    results = []
    for product in user["products"]:
        matches_all = True
        for q in match_query:
            key, val = list(q.items())[0]
            if product.get(key) != val:
                matches_all = False
                break
        if matches_all:
            results.append(product)

    results.append({"status": "It is working without error."})

    return results

def get_sorted_products(user_id, sort_by="name", reverse=False):
    """
    Fetches and returns products sorted by the specified key.
    sort_by can be: 'name', 'category', 'mfg', 'exp'
    reverse: bool, True for descending order
    Safety we have used status string for checking status of the code execution.(index -1)
    """
    user = users_collection.find_one({"user_id": user_id})
    if not user:
        return [{"status": "User not found."}]

    valid_keys = ["name", "category", "mfg", "exp"]
    if sort_by not in valid_keys:
        return [{"status": f"sort_by must be one of {valid_keys}"}]

    sorted_products = sorted(
        user["products"],
        key=lambda x: x.get(sort_by, ""),
        reverse=reverse
    )
    sorted_products.append({"status": "It is working without error."})

    return sorted_products


# Example usage:
# create_user("user123", "john_doe")
# put_product("user123", {
#      "name": "Advil LIQUI-GELS",
#      "category": "Medicine",
#      "mfg": "2024-10-01",
#      "exp": "2026-10-01"
# })
# delete_product("user123", "Advil LIQUI-GELS")