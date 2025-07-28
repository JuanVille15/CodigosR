library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KyCo_090625*")

Base_Cruce <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
     fac.GCC_TIPO_ALFABETICO,
     fac.ASO_IDENTIFICACION,
     fac.CON_CODIGO,
     fac.FACT_INTERES_MORA_DIA,
     fac.FACT_VALOR_CUOTA_MES,
     fac.FACT_VALOR_VENCIDO,
     fac.FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS TOTAL,
     fac.FACT_NUMERO_CUOTAS_ATRASADAS,
     fac.FACT_NUMERO_PAGARE,
     fac.COR_NUMERO_CORTE,
     agr.AGR_DESCRIPCION,
     axe.age_nombre
     from grc.stg_gcc_facturacion fac
     LEFT OUTER JOIN grc.gcc_concepto con
       ON fac.con_codigo = con.con_codigo
     LEFT OUTER JOIN grc.gcc_agrupacion agr
       ON con.agr_codigo = agr.agr_codigo
     LEFT OUTER JOIN grc.gcc_agrupacion_empresa axe
       ON agr.age_codigo = axe.age_codigo;"))

dbDisconnect(Connecion_SQL)


Cedulas_Consulta <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Facturación/Cedulas_consulta.xlsx")

# MERGE SIRVE PARA CURZAR LOS DF PERO DEBEN TENER 1 COLUMNA CON EL MISMO NOMBRE, 
# EL RESULATDO SON TODAS LAS COLUMNAS DE AMBOS DF.

Base_Facturacion_Dia <- merge(Base_Cruce,Cedulas_Consulta)        

# Write_xlsx(NOMBRE DEL DF RESULTADO, "RUTA DONDE DESEO GUARDAR EL ARCHIVO *LOS SLASH NORMAL*/NOMBRE DEL DOCUMENTO.XLSX",COL_NAMES=TRUE 
# *COL_NAMES= TRUE*: SIRVE PARA DEFINIR QUE LA PRIMERA FILA SON LOS TITULOS*)

write_xlsx(Base_Facturacion_Dia,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Facturación/Resultado Consulta/Base_Facturacion_Dia.xlsx",col_names=TRUE)
