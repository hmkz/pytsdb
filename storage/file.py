# Description: File storage class for storing data in files 
from storage.base import Storage
import os
import pandas as pd
from typing import List
import pyarrow.parquet as pq
import pyarrow as pa

"""
File - class for Time series data stored in files

read - read data from file. Supported file formats: parquet
  Analysis and manipulation, compute with pandas. Supported operations:
  - groupby - group data by time and aggregate
  - filter - filter data by time
    - sort - sort data by time

write - write data to file. Supported file formats: parquet.
    Data is stored in parquet format. Data is stored in partitioned by time every 1 hour.
"""
class File(Storage):
    def __init__(self, datadir) -> None:
        super().__init__()
        self._datadir = datadir


    def read(self, metric: str, start=None, end=None, tags=None) -> List:
        """
        Read data from file.

        :param start: float, start time of data
        :param end: float, end time of data
        :param metrics: string, name of data
        :param tags: dict, tags of data
        :return: list of dictionaries containing the data.
          timestamp: float, timestamp of the data
          metrics: string, name of data
            value: float, value of data
            tags: dict, tags of data
        """
        df = pd.read_parquet(self._datadir)
        df = df[df['metrics.name'] == metric]
        if start:
            df = df[(df['timestamp'] >= start)]
        if end:
            df = df[(df['timestamp'] < end)]
        
        df = df.drop(columns=['metrics.name'])
        df = df.rename(columns={'metrics.value': 'value', 'metrics.tags': 'tags'})
        
        if tags:
            for tag_key, tag_value in tags.items():
                df = df[df['tags'].apply(lambda x: x[tag_key] == tag_value)]
            #df = df.drop(columns=['tags'])
        df = df.set_index('timestamp')
        df = df.sort_index()
        #df = df.reset_index()
        return df.to_dict('records')

    def write(self, data):
        """
        Write given data to parquet format.

        :param data: A list of dictionaries containing the data.
          timestamp: float, timestamp of the data
          metrics: string, name of data
            value: float, value of data
            tags: dict, tags of data
        """
        print(data)
        df = pd.DataFrame(data)
        df = df.set_index('timestamp')
        df = df.sort_index()
        df = df.reset_index()
        df = df.rename(columns={'metrics': 'metrics.name', 'value': 'metrics.value', 'tags': 'metrics.tags'})
        table = pa.Table.from_pandas(df)
        is_error = False
        try:
            pq.write_to_dataset(table,
                                root_path=self._datadir,
                                compression='snappy',
                                write_statistics=True,
                                partition_cols=['metrics.name'])
        except:
            is_error = True
        finally:
            if is_error:
                return False
            return True

    def update(self):
        pass

    def delete(self):
        pass
