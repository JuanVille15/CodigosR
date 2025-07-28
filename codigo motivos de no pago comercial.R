### Motivos de No pago Comercial ################################################

library(readxl)
library(dplyr)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(tidyr)
library(lubridate)
library(conflicted)
library(dbplyr)
library(readr)
library(Microsoft365R)
library(blastula)

### CAMBIAR MES DE LAS GESTIONES PARA DESCARAGR EL ARCHIVO CORRECTO

conflict_prefer("filter", "dplyr")

base_gestiones <- read_csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506.csv") %>%
  clean_names(case = "all_caps")
  

motivos_comercial <- base_gestiones %>% 
  filter(MOTIVO_NOPAGO %in% c("VINCULACION","MALA ASESORIA","NEGACION DE PRODUCTOS"))
 

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "dasp6749", 
  PWD = "Ko-Co_130625*")

gcc_asociados <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
     aso.aso_identificacion,
     reg.reg_nombre,
     zon.zon_nombre,
     stm.estm_nombre
     FROM grc.stg_gcc_asociado aso
     LEFT OUTER JOIN grc.GCC_ESTADOS_MULTIACTIVA  stm
        ON aso.estm_codigo = stm.estm_codigo
     LEFT OUTER JOIN grc.GCC_REGIONAL reg
        ON aso.reg_codigo = TRIM(reg.reg_codigo)
     LEFT OUTER JOIN grc.GCC_ZONA zon
        ON aso.zon_codigo = TRIM(zon.zon_codigo);"))

dbDisconnect(Connecion_SQL)

gcc_asociados <- gcc_asociados %>%
  mutate(ASO_IDENTIFICACION = as.double(ASO_IDENTIFICACION))

motivos_comercial <- motivos_comercial %>% 
  left_join(gcc_asociados, by=c("IDENTIFICACION_DEUDOR"="ASO_IDENTIFICACION"))

v_today <- Sys.Date()-months(1)
v_periodo <-ceiling_date(v_today, unit = "month")-1

write_xlsx(motivos_comercial,glue("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Motivos de no pago comercial/Motivos_Comercial_{v_periodo}_USO_INTERNO.xlsx",col_names=TRUE))
