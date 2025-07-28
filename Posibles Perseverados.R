library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)
library(dbplyr)
library(lubridate)
library(janitor)
library(readr)
library(Microsoft365R)
library(blastula)
library(janitor)

df_inicial <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Julio/Foto_inicial.xlsx")

df_inicial <- df_inicial %>% filter(Edad_Inicial_Coomeva == 4)

## Conexión ##

Connecion_SQL <- dbConnect( 
    odbc(),
    Driver = "Oracle in instantclient_21_3",
    DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
    SVC = "GRC", 
    encoding = "ISO-8859-1", 
    UID = "JJVT9593", 
    PWD = "KysCoL_140725*")

df_info <- dbGetQuery(Connecion_SQL, glue_sql("SELECT FAC.ASO_IDENTIFICACION,
                                                      CLI.NOMBRE_COMPLETO AS NOMBRE_ASOCIADO,
                                                      CLI.ESTADO_MULTIACTIVA AS ESTADO_HOY,
                                                      SUM(HFR_VALOR_CUOTA_MES_CALCULADO) AS CUOTA_MES,
                                                      MAX(FAC.HFR_CUOTAS_ATRASADAS_INI) AS CUOTAS_VENCIDAS
                                               FROM GRC.GCC_HIST_FACT_RECAUDO FAC
                                               LEFT JOIN GRC.STG_GCC_SAC_INFOCLI CLI
                                                 ON FAC.ASO_IDENTIFICACION = CLI.ASO_IDENTIFICACION
                                               LEFT JOIN GRC.GCC_CONCEPTO CON
                                                 ON FAC.CON_CODIGO = CON.CON_CODIGO
                                               WHERE HFR_PERIODO_FACT = '202507'
                                               AND CLI.EDAD_HOY >= 60
                                               AND CON.AGR_CODIGO = 'SOLIDARIDAD'
                                              GROUP BY FAC.ASO_IDENTIFICACION, CLI.NOMBRE_COMPLETO, CLI.ESTADO_MULTIACTIVA
                                                HAVING SUM(HFR_VALOR_CUOTA_MES_CALCULADO) BETWEEN 12000 AND 14000;"))

df_inicial <- df_inicial %>% select(ASO_IDENTIFICACION, Edad_Inicial_Coomeva)

df_inicial <- df_inicial %>% left_join(df_info, by = c("ASO_IDENTIFICACION" = "ASO_IDENTIFICACION"))

df_filt <- df_inicial %>% filter(ESTADO_HOY != is.na(ESTADO_HOY))

df_inac <- df_info %>% filter(ESTADO_HOY == "Inactivo")

write_xlsx(df_inac,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Analisis_Perseverados_Inactivos.xlsx")
