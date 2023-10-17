from pymongo.mongo_client import MongoClient

uri = "mongodb+srv://thomasar676:1234@cluster0.ninaexe.mongodb.net/?retryWrites=true&w=majority"
# Create a new client and connect to the server
client = MongoClient(uri)
db = client['cluster0']
u = db.list_collection_names()
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

