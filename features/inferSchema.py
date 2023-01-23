#######################################
# Imports y asignaciones importantes   
# version: 0.1.8                                     
# fecha: 14-01-2023                         
# ultima modificacion: 23-01-2023
# Gabriel Aillapan                                    
#######################################

from pyspark.sql.functions import col
from pyspark.sql.types import *

# TODO: hacer mas pruebas y verificaciones | Hacer validaciones de Long to Short 
#La función itera sobre cada columna del DataFrame de entrada y selecciona el primer valor de cada una.
#  Luego, utiliza una serie de condicionales para determinar el tipo de datos de la columna.
#  Si el primer valor contiene un punto, se asume que es un número de tipo DoubleType.
#  Si contiene un guión, se asume que es una fecha de tipo DateType.
#  Si contiene una barra, se asume que es una fecha de tipo DateType.
#  Si el nombre de la columna es "Date", se asume que es una fecha de tipo DateType.
#  Si contiene "True" o "False", se asume que es un valor booleano de tipo BooleanType.
#  Si el primer valor es numérico, se asume que es un número de tipo IntegerType.
#  Si no cumple con ninguna de las condiciones anteriores, se asume que es una cadena de caracteres de tipo StringType.
#Una vez que se ha inferido el tipo de datos para cada columna, se aplica el cast correspondiente y se devuelve el DataFrame con el esquema inferido.
def new_infer_schema_v5(dataframe):
    for columna in dataframe.columns: 
        first_value = dataframe.select(col(columna)).first()[0]
        if "." in first_value: 
            dataframe = dataframe.withColumn(columna , col(columna).cast(DoubleType())) 
        elif "-"  in first_value:
            dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))  
        elif "/" in first_value:
            dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))
        elif columna == "Date":
            dataframe = dataframe.withColumn(columna , col(columna).cast(DateType()))
        elif "True" in first_value:
            dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
        elif "False" in first_value:
            dataframe = dataframe.withColumn(columna , col(columna).cast(BooleanType()))
        elif first_value.isnumeric():
            dataframe = dataframe.withColumn(columna , col(columna).cast(IntegerType()))
        else:
            dataframe = dataframe.withColumn(columna , col(columna).cast(StringType()))   
    return dataframe 