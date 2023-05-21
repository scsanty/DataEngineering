from __init__ import *
from __drivers__ import *

class data_factory(drivers):
	def __init__(self) -> None:
		self.source = None
		self.bucket = None
		self.target = None
	
	def __pre_check(func):
		def wrapper(*args, **kwargs):
			self = args[0]
			if self.source is None or self.target is None:
				raise ValueError("Declare source and target before proceeding")
			return func(*args, **kwargs)
		return wrapper

	@__pre_check
	def copy_data(self):
		try:
			self.bucket = self.source[0](**self.source[1])
			self.target[0](self.bucket, self.target[1])
			return 0
		except Exception as e:
			return e
	
	@__pre_check
	def schema_mapping(self, schema : list | dict):
		if self.bucket is None:
			self.bucket = self.source[0](**self.source[1])
		
		if type(schema) == list:
			if (len(self.bucket.columns) != len(schema)):
				raise ValueError("Missing columns from the schema list, for " +
			 			"partial schema renaming using a dictionary instead")
			else:
				self.bucket.columns = schema

		elif type(schema) == dict:
			self.bucket.rename(columns=schema)
		

		
		







	 