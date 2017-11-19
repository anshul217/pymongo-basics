from datetime import datetime, timedelta
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError


class MongoHandler(object):

	def __init__(self, host, port):
	    """This is the construct of the class.
	    Properties created with the ``@property`` decorator should be documented
	    in the property's getter method.
		This constructor will raise serverSeclectionTimeout error if script is not able to connect to mongo server.
	    Attributes:
	        host (str): It takes ip of the host like `localhost`.
	        port (str): It takes port of the host like `27017`.
		"""
	
		self.host = host
		self.port = port
		self.client_obj = MongoClient(self.host, self.port,serverSelectionTimeoutMS=10)
		info = self.client_obj.server_info()

	def get_existing_databases(self):
		"""
		This function returns all the existing database in pymongo
		"""
		return self.client_obj.database_names()

	def get_exchangedb_collections(self):
		"""
		This function returns all the collections in the Exchanges Database
		"""
		self.exchange_db = self.client_obj.Exchanges
		return self.exchange_db.collection_names()

	def get_trade_history_documents(self):
		"""
		This function returns all the name of documents in Trade_History collection
		"""
		exchange_db = self.client_obj.Exchanges
		names =[]
		for i in exchange_db.Trade_History.find():
			names.append(i['name'])
		return names

	def get_account_docs(self, time_from, time_to):
		"""
		This function retun all the doc between dates.
		"""
		account_db = self.client_obj.Exchanges
		account_docs = account_db.Accounts.find()
		docs_btw = []
		time_from_ = datetime.strptime(time_from, '%m/%d/%Y')
		time_to_ = datetime.strptime(time_to, '%m/%d/%Y')
		for i in account_docs:
			time_ = datetime.strptime(i["Time"], '%m/%d/%Y')
			if time_from_ < time_ <time_to_:
				docs_btw.append(i)
		return docs_btw

	def get_oldest_docs(self):
		"""
		This function return the oldest document in the collection.
		"""
		exchange_db = self.client_obj.Exchanges
		res = exchange_db.Old_Stuff.find().sort('Time_Stamp', -1).limit(1)[0]
		return res

if __name__ == "__main__":
	mongo_handel = MongoHandler('localhost',27017)
	print("Names of all databases in Mongodb : ", mongo_handel.get_existing_databases())
	print("Names of all collections in Exchanges db : ", mongo_handel.get_exchangedb_collections())
	print("Names of all documents in a collection named 'Trade_History' : ", mongo_handel.get_trade_history_documents())
	print("all documents in a collection named ‘Accounts’ occurring after 10/15/2017 and before 10/18/2017' : ", mongo_handel.get_account_docs('10/15/2017','10/18/2017'))
	print("oldest document found in a collection : ", mongo_handel.get_oldest_docs())

