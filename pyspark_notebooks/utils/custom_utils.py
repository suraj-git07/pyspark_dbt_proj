from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

class transformations:

    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def dedup(self,df:DataFrame,dedup_cols:List,cdc:str):
        df = df.withColumn("dedupKey",concat_ws("||", *dedup_cols))
        df = df.withColumn("dedupCounts",row_number().over(Window.partitionBy("dedupKey").orderBy(col(cdc).desc())))
        df = df.filter(col("dedupCounts") == 1)
        df = df.drop("dedupKey","dedupCounts")
        return df
    
    def process_timestamp(self,df:DataFrame):
        df = df.withColumn("process_timestamp",current_timestamp())
        return df
    
    def upsert(self,df:DataFrame,key_cols:List,table:str,cdc:str):
        merge_condition = ' AND '.join([f"trg.{k} = src.{k}" for k in key_cols])
        dlt_obj = DeltaTable.forName(self.spark,f"pyspark_proj.silver.{table}")
        dlt_obj.alias("trg").merge(
            df.alias("src"),
            merge_condition
        ).whenMatchedUpdateAll(condition = f"src.{cdc} >= trg.{cdc}")\
        .whenNotMatchedInsertAll()\
        .execute()
    
        return 1

    

        