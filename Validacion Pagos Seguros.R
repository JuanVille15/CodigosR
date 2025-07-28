library(dplyr)
library(readxl)
library(writexl)
library(lubridate)
library(glue)
library(tools)
library(janitor)
library(odbc)
library(dbplyr)
library(tidyverse)
library(blastula)
library(janitor)

# ---- VALIDACIÓN PAGOS SEGUROS # -----

## LEEMOS ARCHIVO DE CONTROL ##

Data <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Validación_Pagos_Seguros_Abr_2025.xlsx")


## Conectamos a la bodega de datos ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "TyhU_RtQ3491*")


## Consulta de Pagos ##

Pagos <- dbGetQuery(Connecion_SQL,glue_sql("select aso_identificacion_asociado as Cedula_Asociado,
                                            sum(hrcd_valor_recaudo) as Valor_Recaudo,
                                            rcdo_numero_pagare as Referencia_seguro,
                                            con_codigo as Concepto,
                                            hrcd_tipo_recaudo as Tipo_Recaudo
                                           from grc.gcc_historia_recaudo
                                           where hrcd_fecha_recaudo between to_date('01/04/2025', 'DD/MM/YYYY') and to_date('30/04/2025', 'DD/MM/YYYY')
                                           group by (aso_identificacion_asociado,rcdo_numero_pagare,con_codigo,hrcd_tipo_recaudo);"))


Data <- Data %>% mutate(Cedulas = as.character(Cedulas))

Pagos <- Pagos %>% mutate(CEDULA_ASOCIADO = as.character(CEDULA_ASOCIADO))

### Realizar el cruce ##

Data <- Data %>% left_join(Pagos, by = c("Cedulas" = "CEDULA_ASOCIADO",
                                         "CONCEPTO" = "CONCEPTO",
                                         "PAGARE" = "REFERENCIA_SEGURO"))


## Consultamos la Factura ##

Fact <- dbGetQuery(Connecion_SQL,glue_sql("select aso_identificacion as Cedula,
                                                  con_codigo as Concepto,
                                                  hfr_numero_pagare as Pagare,
                                                  hfr_valor_vencido_calculado as Vencido
                                                  from grc.gcc_hist_fact_recaudo
                                                  where hfr_periodo_fact = '202504';"))
Fact <- Fact %>% mutate(CEDULA = as.character(CEDULA))


## Cruzamos la facturación ##

Data <- Data %>% left_join(Fact, by = c("Cedulas" = "CEDULA",
                                        "CONCEPTO" = "CONCEPTO",
                                        "PAGARE" = "PAGARE"))
Data <- Data %>% distinct()

## Escribir Resultado ##

write_xlsx(Data,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Validación_Pagos_Seguros_Abr_2025.xlsx")
