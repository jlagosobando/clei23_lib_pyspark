from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import count, lag, lit, when , regexp_replace , col



def contar_var (df , tiempo , seguimiento, tolerancia):
    #df = df.withColumn(tiempo, unix_timestamp(col(tiempo)).cast("integer"))
    #df = df.withColumn("date_int", cast(concat(date_format(tiempo, "yyyy"), date_format(tiempo, "MM"),date_format(tiempo, "dd")), "int"))
    #df = df.withColumn(tiempo, unix_timestamp(tiempo))
    df = df.groupBy(tiempo).pivot(seguimiento).agg(count("*"))
    df = df.sort(tiempo)
    df = df.withColumn(tiempo, df[tiempo].cast(IntegerType()))
    #df = df.orderBy(col(tiempo).asc())
    for col in df.columns : df = df.fillna(0 , col)
    for col in df.columns :
        df = df.withColumn('var_neta_'+col , df[col] - lag(df[col]).over(Window.partitionBy().orderBy(tiempo)))
    for col in df.columns:
        if col.startswith("var_neta_"):
            df = df.withColumn('var_porc_'+col, (df[col] / lag(df[col]).over(Window.partitionBy().orderBy(tiempo)) -1)*100)
            df = df.withColumn("tolerancia", lit(tolerancia))
    for col in df.columns:
        if col.startswith("var_porc_"):
            df = df.withColumn('fuera_de_tolerancia_'+col, when(abs(df[col]) > df["tolerancia"], 1).otherwise(0))
    return df


def contar_var_2 (df , tiempo , seguimiento, tolerancia):
    #df = df.withColumn(tiempo, unix_timestamp(col(tiempo)).cast("integer"))
    #df = df.withColumn("date_int", cast(concat(date_format(tiempo, "yyyy"), date_format(tiempo, "MM"),date_format(tiempo, "dd")), "int"))
    #df = df.withColumn(tiempo, unix_timestamp(tiempo))
    data_type = df.schema[tiempo].dataType
    data_type = data_type.simpleString()
    if data_type != 'integer':
        df = df.withColumn(tiempo ,df[tiempo].cast(DateType()))
        df = df.withColumn(tiempo ,df[tiempo].cast(StringType()))
        df_dates_arreglado = df.withColumn(tiempo, regexp_replace(col(tiempo), "-", ""))
        df_dates_arreglado = df_dates_arreglado.withColumn(tiempo ,df_dates_arreglado[tiempo].cast(IntegerType()))

    df_dates_arreglado = df_dates_arreglado.groupBy(tiempo).pivot(seguimiento).agg(count("*"))
    df_dates_arreglado = df_dates_arreglado.sort(tiempo)
    df_dates_arreglado = df_dates_arreglado.withColumn(tiempo, df[tiempo].cast(IntegerType()))
    #df = df.orderBy(col(tiempo).asc())
    for col in df_dates_arreglado.columns : df_dates_arreglado = df_dates_arreglado.fillna(0 , col)
    for col in df_dates_arreglado.columns :
        df_dates_arreglado = df_dates_arreglado.withColumn('var_neta_'+col , df_dates_arreglado[col] - lag(df_dates_arreglado[col]).over(Window.partitionBy().orderBy(tiempo)))
    for col in df.columns:
        if col.startswith("var_neta_"):
            df_dates_arreglado = df_dates_arreglado.withColumn('var_porc_'+col, (df_dates_arreglado[col] / lag(df_dates_arreglado[col]).over(Window.partitionBy().orderBy(tiempo)) -1)*100)
            df_dates_arreglado = df_dates_arreglado.withColumn("tolerancia", lit(tolerancia))
    for col in df_dates_arreglado.columns:
        if col.startswith("var_porc_"):
            df_dates_arreglado = df_dates_arreglado.withColumn('fuera_de_tolerancia_'+col, when(abs(df[col]) > df_dates_arreglado["tolerancia"], 1).otherwise(0))
    return df_dates_arreglado


def castInteger(dataframe): 
    df_ernigs = df_ernigs.withColumn('reportDate' ,df_ernigs['reportDate'].cast(DateType()))
    df_ernigs = df_ernigs.withColumn('reportDate' ,df_ernigs['reportDate'].cast(StringType()))
    df_dates_arreglado = df_ernigs.withColumn("reportDate", regexp_replace(col("reportDate"), "-", ""))
    df_dates_arreglado = df_dates_arreglado.withColumn('reportDate' ,df_dates_arreglado['reportDate'].cast(IntegerType()))
    df_dates_arreglado.show()