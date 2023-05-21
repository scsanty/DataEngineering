from data_factory import data_factory

from __drivers__ import *

if __name__ == "__main__":
    df = data_factory()
    source = df.FileStorage(file='testa.csv', file_type='csv', delimiter=';', usage='source')
    target = df.FileStorage(file='testb.parquet', file_type='parquet', usage='sink')