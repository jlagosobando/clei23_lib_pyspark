from auxiliares.auxi import sel_num_cols
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def sumary(dataframe): 
    num_cols = sel_num_cols(dataframe)
    df_columns = dataframe.select(*num_cols)
    lista_modas= ['moda']
    lista_antimoda = ['antimoda']
    lista_columnas = ['summary']
    for col in num_cols:
        mode_val = dataframe.groupBy(col).count().sort('count', ascending=False).first()[0]
        anti_mode_val = dataframe.groupBy(col).count().sort('count', ascending=True).first()[0]
        lista_modas.append(mode_val)
        lista_antimoda.append(anti_mode_val)
    for valor in num_cols: lista_columnas.append(valor)
    
    lista_modas = tuple(lista_modas)
    lista_antimoda = tuple(lista_antimoda)
    new_row = spark.createDataFrame([lista_modas] , [*lista_columnas])
    new_row_anti_mode = spark.createDataFrame([lista_antimoda] , [*lista_columnas])
    df_columns.summary().unionAll(new_row).unionAll(new_row_anti_mode).show()
