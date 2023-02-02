#######################################
# Imports y asignaciones importantes   
# version: 0.1.8                                     
# fecha: 14-01-2023                         
# ultima modificacion: 23-01-2023
# Gabriel Aillapan                                    
#######################################

from pyspark.sql.functions import col
from pyspark.sql.types import *
from validaciones import is_dataframe 
import re 
def inferSchema(dataframe):
    """
    Esta función recibe un dataframe de PySpark, una columna a ser imputada y una columna auxiliar. 
    La función devuelve el dataframe con la columna de imputación llenada con el promedio de los valores 
    de la columna de imputación agrupados por la columna auxiliar. Si el valor de la columna de imputación
    no es nulo, se mantiene el mismo valor.

    Argumentos:
    dataframe (DataFrame): Dataframe de PySpark que se desea imputar
    columna_imputacion (string): Nombre de la columna que se desea imputar
    columna_auxiliar (string): Nombre de la columna auxiliar para calcular el promedio

    Retorno:
    DataFrame: Dataframe de PySpark con la columna de imputación llenada con el promedio de los valores 
               de la columna de imputación agrupados por la columna auxiliar.
    """
    is_dataframe(dataframe) 
    try:
        dataframe = dataframe.select([col(c).cast("string") for c in dataframe.columns])
        for columna in dataframe.columns: 
            first_value = dataframe.select(col(columna)).first()[0]
            if contains_letter(first_value):
                dataframe = dataframe.withColumn(columna , col(columna).cast(StringType())) 
                print(f'{columna} | string -> string')
            elif '.' in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(DoubleType()))
                print(f'{columna} | string -> double') 
            elif "-"  in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))  
                print(f'{columna} | string -> date')
            elif "/" in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))
            elif columna == "Date":
                dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))
                print(f'{columna} | string -> date')
            elif "True" in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
                print(f'{columna} | string -> bool')
            elif "False" in first_value:
                dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
                print(f'{columna} | string -> bool')
            elif first_value.isnumeric():
                dataframe = dataframe.withColumn(columna , col(columna).cast(LongType()))
                try:
                    dataframe = dataframe.withColumn(columna , col(columna).cast((IntegerType())))
                    print(f'{columna} | string -> integer')
                except:
                    dataframe = dataframe.withColumn(columna , col(columna).cast(LongType()))
                    print(f'{columna} | string -> long')
                    pass
            else:
                dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
                print(f'{columna} | string -> string')
        return dataframe
    except Exception as e :
        print('Ha ocurrido un erro al momento de inferir el schema: ' , e )


def contains_letter(string):
    return bool(re.search("[a-zA-Z]", string))