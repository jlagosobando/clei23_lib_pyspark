from pyspark.sql.types import *
from pyspark.sql.functions import *


def imputacion(dataframe , columna , opcion , auxiliar):
    tipos_numericos = [LongType().simpleString(), DoubleType().simpleString(), 
        IntegerType().simpleString() , ShortType().simpleString() ,
        FloatType().simpleString() , DecimalType().simpleString()]
    data_type = dataframe.schema[columna].dataType
    data_type = data_type.simpleString()
    print(data_type)

    if data_type in tipos_numericos:
        print('estoy dentro de el if de datos numericos')
        if opcion == 'mediana':
            print('opcion de mediana')
            mean_val = dataframe.select(mean(columna)).first()[0]
            dataframe = dataframe.fillna(mean_val, [columna])
            return dataframe
        elif opcion == 'media':
            print('opcion de media')
            median_val = dataframe.select(percentile_approx(columna, 0.5).alias("median")).first()[0]
            dataframe = dataframe.fillna(median_val, [columna])
            return dataframe
        elif opcion == 'moda':
            print('opcion de moda')
            mode_val = dataframe.groupBy(columna).count().sort('count', ascending=False).first()[0]
            dataframe = dataframe.fillna(mode_val, [columna])
            return dataframe
        elif opcion == 'similitud':
            df_fill = similitud(dataframe , columna , auxiliar)
            return df_fill
        elif opcion == 'zero':
            dataframe = dataframe.fillna(0, [columna])
            return dataframe
                       
    elif data_type == StringType().simpleString():
        lista_nulls = ['null' , '-' , '' , ' ', 'n' , '\\n' ]        
        if opcion == 'moda': 
            columna_name = "name"
            mode_val = dataframe.filter(dataframe[columna].isNotNull()).groupBy(columna).count().sort('count', ascending=False).first()[0]
            for i, row in enumerate(dataframe.toLocalIterator()):
                if row[columna] in lista_nulls:
                    dataframe = dataframe.withColumn(columna, when(col(columna).isin(lista_nulls), mode_val).otherwise(col(columna)))                    
        return dataframe


def similitud(dataframe , col_name ,ref_col_name):
    average_prices = dataframe.groupBy(ref_col_name).agg(avg(col_name).alias("avg_"+col_name))
    dataframe = dataframe.join(average_prices, ref_col_name, "left") \
    .withColumn(col_name, when(col(col_name).isNull(), average_prices["avg_"+col_name]).otherwise(col(col_name)))
    return dataframe