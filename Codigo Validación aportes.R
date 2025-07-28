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
  PWD = "Wa4_A#Ro#15")

Base_Cruce <- dbGetQuery(Connecion_SQL,glue_sql("Select GCC_TIPO_ALFABETICO,
                                                ASO_IDENTIFICACION,
                                                CON_CODIGO,
                                                FACT_INTERES_MORA_DIA,
                                                FACT_VALOR_CUOTA_MES,
                                                FACT_VALOR_VENCIDO,
                                                FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO AS TOTAL,
                                                FACT_NUMERO_CUOTAS_ATRASADAS,
                                                FACT_NUMERO_PAGARE,
                                                COR_NUMERO_CORTE
                                                from GRC.STG_GCC_FACTURACION
                                                where CON_CODIGO in ('APORTES');"))


dbDisconnect(Connecion_SQL)

Cedulas_Consulta <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Facturación/Cedulas_consulta.xlsx")
Base_Facturacion_Dia <- merge(Base_Cruce,Cedulas_Consulta)
write_xlsx(Base_Facturacion_Dia,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Facturación/Resultado Consulta/Base_Facturacion_Dia.xlsx",col_names=TRUE)
