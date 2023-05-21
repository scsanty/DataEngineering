import pandas as pd
import os

class drivers:
	def __init__(self) -> None:
		self.allowed_filestorages = [
			"AzureBlobStorage", 
			"AzureDataLakeStorage", 
			"FileStorage"
			]
		self.allowed_dbstorages = ["AzureSQL", "MSSQL", "MySQL"]
		self.allowed_filetypes = ["csv", "parquet", "text", "excel"]
		self.source_alias = ['source', 'read']
		self.target_alias = ['target', 'write', 'destination', 'sink']

	def __validate_args(func):
		def wrapper(*args, **kwargs):
			self = args[0]
			conditions = lambda var, val: {
				"in_file" 	: os.path.exists(val),
				"file_type"	: val.lower() in self.allowed_filetypes,
				"usage"		: val.lower() in (self.source_alias + 
				   							  self.target_alias),
				"delimiter"	: len(val) == 1
			}.get(var)

			anno = func.__annotations__
			for variable, value in kwargs.items():
				if (variable in anno) and (type(value) != anno[variable]):
					raise TypeError("Wrong type provided for", variable, 
			 						"allowed type", anno[variable])
				if conditions(variable, value) == False:
					raise ValueError("Wrong value provided for", 
			  						(variable, value))
			return func(*args, **kwargs)
		return wrapper

	@__validate_args
	def FileStorage(self, 
		  file : str, 
		  file_type : str, 
		  usage : str,
		  **kwargs
		  ):
		file_type = file_type.lower()
		if file_type in ['csv', 'text']:
			if usage in self.source_alias:
				return self.__csv_reader(in_file=file, **kwargs)
			elif usage in self.target_alias:
				return self.__csv_writer(out_file=file, **kwargs)
		
		elif file_type == 'excel':
			if usage in self.source_alias:
				return self.__excel_reader(in_file=file, **kwargs)
			elif usage in self.target_alias:
				return self.__excel_writer(out_file=file, **kwargs)

		elif file_type == 'parquet':
			if usage in self.source_alias:
				return self.__parquet_reader(in_file=file, **kwargs)
			elif usage in self.target_alias:
				return self.__parquet_writer(out_file=file, **kwargs)

	@__validate_args
	def __csv_reader(self, 
		  in_file : str, 
		  delimiter : str = ',', 
		  first_row_as_header : bool = True,
		  schema : list = []
		  ):
		header = 0 if (first_row_as_header and (not schema)) else None
		schema = schema if schema else None
		kwargs = dict(
			filepath_or_buffer=in_file, 
			delimiter=delimiter, 
			header=header, 
			names=schema
			)
		return pd.read_csv, kwargs

	@__validate_args
	def __excel_reader(self, 
		  in_file : str,
		  first_row_as_header : bool = True,
		  schema : list = [],
		  sheet_name : str = ""
		  ):
		sheet_name = 0 if sheet_name == "" else sheet_name
		header = 0 if (first_row_as_header and (not schema)) else None
		schema = schema if schema else None
		kwargs = dict(
			io=in_file,
			sheet_name=sheet_name,
			header=header, 
			names=schema
			)
		
		return pd.read_excel, kwargs
	
	@__validate_args
	def __parquet_reader(self, in_file : str):
		kwargs = dict(path=in_file)
		return pd.read_parquet, kwargs

	@__validate_args
	def __csv_writer(self, 
		  out_file : str,
		  delimiter : str = ','
		  ):
		to_csv = lambda frame, kwargs: frame.to_csv(**kwargs)
		kwargs = dict(
			path_or_buf=out_file,
			sep=delimiter
		)
		return to_csv, kwargs

	@__validate_args
	def __excel_writer(self,
			out_file : str,
			sheet_name : str = "Sheet1"
			):
		to_excel = lambda frame, kwargs: frame.to_excel(**kwargs)
		kwargs	 = dict(
			excel_writer=out_file,
			sheet_name=sheet_name
		)
		return to_excel, kwargs

	@__validate_args
	def __parquet_writer(self, out_file : str):
		to_parquet = lambda frame, kwargs: frame.to_parquet(**kwargs)
		kwargs	   = dict(path=out_file)
		return to_parquet, kwargs


################################################################################




################################################################################


# def wrap(func):
# 	def wrapper(*args, **kwargs):
# 		print(args, kwargs)
# 		func(*args, **kwargs)
# 	return wrapper
# class A:

	
# 	@wrap
# 	def afunc(self, a):
# 		print('A', a)
# 		self.bfunc(b=a)

# 	@wrap
# 	def bfunc(self, b):
# 		print('B', b)

# a = A()
# a.afunc(a=5)