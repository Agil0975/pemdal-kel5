import requests

COUCHDB_URL = "http://127.0.0.1:5984"
AUTH = ("admin", "admin123")

# res = requests.get(f"{COUCHDB_URL}/users/user_001", auth=AUTH)
# print(res.json())


# res = requests.get(
#     f"{COUCHDB_URL}/users/_design/user_views/_view/by_role?key=\"customer\"&include_docs=true",
#     auth=AUTH
# )
# for row in res.json()["rows"]:
#     print(row["doc"])


# import requests

# COUCHDB_URL = "http://127.0.0.1:5984"
# AUTH = ("admin", "admin123")

query = {
    "selector": {
        "name": "Agil"
    }
}

res = requests.post(f"{COUCHDB_URL}/users/_find", json=query, auth=AUTH)
print(res.json())
