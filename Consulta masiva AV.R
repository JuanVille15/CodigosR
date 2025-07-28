library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_13",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "Jju3v#A5t93")

Base_Cruce <- dbGetQuery(Connecion_SQL,glue_sql(
  "Select 
  ASO_IDENTIFICACION,
  SUM (FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO) AS TOTAL,
  FACT_NUMERO_PAGARE
  from GRC.STG_GCC_FACTURACION
  where CON_CODIGO in ('APORTES','CALAMID','RECREA','AUX.FUN.','SOLIDAR.','SOLIDARE')
  GROUP BY (ASO_IDENTIFICACION,FACT_NUMERO_PAGARE);"))

Cedulas_Consulta <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Agente Virtual Consulta/Cedula_Consulta/Cedulas_consulta.xlsx")

Pagares <- left_join(Cedulas_Consulta,Base_Cruce, by=c("Cedula" = "ASO_IDENTIFICACION"))

write_xlsx(Pagares,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Agente Virtual Consulta/Cedula_Consulta/Resultado.xlsx")
