### Script para generar informacion de pagos y facturacion de los ofrecimientos del alivio MP.

library(readxl)
library(dplyr)
library(readxl)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(tidyr)
library(lubridate)
library(stringr)

SEGUIMIENTO_ALIVIO_MP <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Alivio MP/SEGUIMIENTO APLICACIONES ALIVIO MP.xlsx") %>%
  clean_names(case = "all_caps") %>% 
  filter(ESTADO=="PENDIENTE") %>% 
  transform(HORA_DE_FINALIZACION = as.Date(HORA_DE_FINALIZACION,format = "%d-%m-%Y"))

CONTRATO <- distinct(SEGUIMIENTO_ALIVIO_MP %>% select(IDENTIFICACION_ASOCIADO,
                                                      NUMERO_DE_CONTRATO,
                                                      HORA_DE_FINALIZACION)) %>% 
  transform(NUMERO_DE_CONTRATO=as.character(NUMERO_DE_CONTRATO))

FILTRO <- CONTRATO %>% 
  distinct(IDENTIFICACION_ASOCIADO)

LIMITE <- min(CONTRATO$HORA_DE_FINALIZACION)

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_13",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "Jju3v#A5t93")

PAGOS <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
rec.aso_identificacion_asociado,
rec.rcdo_numero_pagare,
rec.hrcd_fecha_recaudo,
sum(rec.hrcd_valor_recaudo) as RECAUDO
from grc.gcc_historia_recaudo rec
LEFT OUTER JOIN grc.gcc_concepto con
   ON rec.con_codigo = con.con_codigo
LEFT OUTER JOIN grc.gcc_agrupacion agr
   ON con.agr_codigo = agr.agr_codigo
LEFT OUTER JOIN grc.gcc_agrupacion_empresa axe
   ON agr.age_codigo = axe.age_codigo
WHERE axe.age_nombre = 'MEDICINA PREPAGADA'
AND HRCD_FECHA_RECAUDO >= TO_DATE('01/10/24', 'DD/MM/YY')
AND rec.CON_CODIGO NOT IN ('CEMR',
'01CEAB',
'01CEAD',
'01CEAG',
'01CEAM',
'01CEAP',
'01CEAS',
'01CEAT',
'01CEA9',
'01CEB',
'01CEB1',
'01CECA',
'01CECE',
'01CEC1',
'01CEEF',
'01CEE1',
'01CEE2',
'01CEFE',
'01CEFG',
'01CEFI',
'01CEFO',
'01CEGF',
'01CEIN',
'01CEM',
'01CEMA',
'01CEMB',
'01CEMC',
'01CEME',
'01CEMH',
'01CEMJ',
'01CEMO',
'01CEMS',
'01CEMX',
'01CEMZ',
'01CEM1',
'01CEM3',
'01CEM4',
'01CEM7',
'01CEM8',
'01CEOC',
'01CEPD',
'01CEPM',
'01CEPO',
'01CEPR',
'01CEPS',
'01CEP2',
'01CERP',
'01CETE',
'01CET9',
'01CEUS',
'01CEX0',
'01CEX1',
'01CEX3',
'01CE19',
'01CE2P',
'01CE23',
'01CPEM',
'01EVEM',
'04CEAB',
'04CEAD',
'04CEAG',
'04CEAM',
'04CEAP',
'04CEAS',
'04CEAT',
'04CEA9',
'04CEB',
'04CEB1',
'04CECA',
'04CECE',
'04CEC1',
'04CEEF',
'04CEE1',
'04CEE2',
'04CEFE',
'04CEFG',
'04CEFI',
'04CEFO',
'04CEGF',
'04CEIN',
'04CEM',
'04CEMA',
'04CEMB',
'04CEMC',
'04CEME',
'04CEMH',
'04CEMJ',
'04CEMO',
'04CEMS',
'04CEMX',
'04CEMZ',
'04CEM1',
'04CEM3',
'04CEM4',
'04CEM7',
'04CEM8',
'04CEOC',
'04CEPD',
'04CEPM',
'04CEPO',
'04CEPR',
'04CEPS',
'04CEP2',
'04CERP',
'04CETE',
'04CET9',
'04CEUS',
'04CEX0',
'04CEX1',
'04CEX3',
'04CE19',
'04CE2P',
'04CE23',
'04CPEM',
'04EVEM',
'07CEM',
'07CEMC',
'07CEMO',
'07CEMS',
'07CEMZ',
'07CEM1',
'07CEM3',
'07CEM4',
'07CEM7',
'07CEM8',
'08CEM',
'08CEMC',
'08CEMJ',
'08CEMO',
'08CEMS',
'08CEMZ',
'08CEM1',
'08CEM3',
'08CEM4',
'08CEM7',
'08CEM8',
'09CEAG',
'09CEAP',
'09CEAS',
'09CEAT',
'09CEFE',
'09CEFG',
'09CEFO',
'09CEGF',
'09CEM',
'09CEMC',
'09CEMH',
'09CEMO',
'09CEMZ',
'09CEM1',
'09CEM3',
'09CEM4',
'09CEM7',
'09CEM8',
'09CEPD',
'09CEPM',
'09CEPO',
'09CEPR',
'09CEPS',
'09CEP2',
'09CERP',
'09CEX3',
'09CE2P',
'01CEMQ',
'04CEMQ',
'01CE50',
'04CE50',
'01PACE',
'04PACE',
'01TACE',
'04TACE',
'01CUE3',
'04CUE3',
'01CUE6',
'04CUE6',
'01CUE9',
'04CUE9',
'01CU12',
'04CU12',
'01CU23',
'04CU23',
'01CU26',
'04CU26',
'01CU29',
'04CU29',
'01CU22',
'04CU22',
'01CTL3',
'04CTL3',
'01CTL6',
'04CTL6',
'01CTL9',
'04CTL9',
'01CT12',
'04CT12',
'01CT23',
'04CT23',
'01CT26',
'04CT26',
'01CT29',
'04CT29',
'01CT22',
'04CT22')
GROUP BY rec.aso_identificacion_asociado,
rec.rcdo_numero_pagare,rec.hrcd_fecha_recaudo;"))


ESTADOS <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT 
     aso.ASO_IDENTIFICACION,
     stm.ESTM_NOMBRE
     FROM grc.STG_GCC_ASOCIADO aso
     LEFT OUTER JOIN grc.GCC_ESTADOS_MULTIACTIVA  stm
        ON aso.estm_codigo = stm.estm_codigo"))

BASE_FACTURA <- dbGetQuery(Connecion_SQL,glue_sql(
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
   ON agr.age_codigo = axe.age_codigo
WHERE axe.age_nombre = 'MEDICINA PREPAGADA'
AND fac.CON_CODIGO NOT IN ('CEMR',
'01CEAB',
'01CEAD',
'01CEAG',
'01CEAM',
'01CEAP',
'01CEAS',
'01CEAT',
'01CEA9',
'01CEB',
'01CEB1',
'01CECA',
'01CECE',
'01CEC1',
'01CEEF',
'01CEE1',
'01CEE2',
'01CEFE',
'01CEFG',
'01CEFI',
'01CEFO',
'01CEGF',
'01CEIN',
'01CEM',
'01CEMA',
'01CEMB',
'01CEMC',
'01CEME',
'01CEMH',
'01CEMJ',
'01CEMO',
'01CEMS',
'01CEMX',
'01CEMZ',
'01CEM1',
'01CEM3',
'01CEM4',
'01CEM7',
'01CEM8',
'01CEOC',
'01CEPD',
'01CEPM',
'01CEPO',
'01CEPR',
'01CEPS',
'01CEP2',
'01CERP',
'01CETE',
'01CET9',
'01CEUS',
'01CEX0',
'01CEX1',
'01CEX3',
'01CE19',
'01CE2P',
'01CE23',
'01CPEM',
'01EVEM',
'04CEAB',
'04CEAD',
'04CEAG',
'04CEAM',
'04CEAP',
'04CEAS',
'04CEAT',
'04CEA9',
'04CEB',
'04CEB1',
'04CECA',
'04CECE',
'04CEC1',
'04CEEF',
'04CEE1',
'04CEE2',
'04CEFE',
'04CEFG',
'04CEFI',
'04CEFO',
'04CEGF',
'04CEIN',
'04CEM',
'04CEMA',
'04CEMB',
'04CEMC',
'04CEME',
'04CEMH',
'04CEMJ',
'04CEMO',
'04CEMS',
'04CEMX',
'04CEMZ',
'04CEM1',
'04CEM3',
'04CEM4',
'04CEM7',
'04CEM8',
'04CEOC',
'04CEPD',
'04CEPM',
'04CEPO',
'04CEPR',
'04CEPS',
'04CEP2',
'04CERP',
'04CETE',
'04CET9',
'04CEUS',
'04CEX0',
'04CEX1',
'04CEX3',
'04CE19',
'04CE2P',
'04CE23',
'04CPEM',
'04EVEM',
'07CEM',
'07CEMC',
'07CEMO',
'07CEMS',
'07CEMZ',
'07CEM1',
'07CEM3',
'07CEM4',
'07CEM7',
'07CEM8',
'08CEM',
'08CEMC',
'08CEMJ',
'08CEMO',
'08CEMS',
'08CEMZ',
'08CEM1',
'08CEM3',
'08CEM4',
'08CEM7',
'08CEM8',
'09CEAG',
'09CEAP',
'09CEAS',
'09CEAT',
'09CEFE',
'09CEFG',
'09CEFO',
'09CEGF',
'09CEM',
'09CEMC',
'09CEMH',
'09CEMO',
'09CEMZ',
'09CEM1',
'09CEM3',
'09CEM4',
'09CEM7',
'09CEM8',
'09CEPD',
'09CEPM',
'09CEPO',
'09CEPR',
'09CEPS',
'09CEP2',
'09CERP',
'09CEX3',
'09CE2P',
'01CEMQ',
'04CEMQ',
'01CE50',
'04CE50',
'01PACE',
'04PACE',
'01TACE',
'04TACE',
'01CUE3',
'04CUE3',
'01CUE6',
'04CUE6',
'01CUE9',
'04CUE9',
'01CU12',
'04CU12',
'01CU23',
'04CU23',
'01CU26',
'04CU26',
'01CU29',
'04CU29',
'01CU22',
'04CU22',
'01CTL3',
'04CTL3',
'01CTL6',
'04CTL6',
'01CTL9',
'04CTL9',
'01CT12',
'04CT12',
'01CT23',
'04CT23',
'01CT26',
'04CT26',
'01CT29',
'04CT29',
'01CT22',
'04CT22');"))

dbDisconnect(Connecion_SQL)

PAGOS <- PAGOS %>% 
  semi_join(CONTRATO, join_by("HRCD_FECHA_RECAUDO" >= "HORA_DE_FINALIZACION", "ASO_IDENTIFICACION_ASOCIADO" == "IDENTIFICACION_ASOCIADO"))

# Función para verificar la presencia del número de contrato en el número de pagaré
extraer_contrato <- function(RCDO_NUMERO_PAGARE, ASO_IDENTIFICACION_ASOCIADO, CONTRATOS) {
  CONTRATOS <- CONTRATOS[CONTRATO$IDENTIFICACION_ASOCIADO == ASO_IDENTIFICACION_ASOCIADO]  # Filtrar contratos por número de documento
  for (NUMERO_DE_CONTRATO in CONTRATOS) {
    if (str_detect(RCDO_NUMERO_PAGARE, NUMERO_DE_CONTRATO)) {
      return(NUMERO_DE_CONTRATO)
    }
  }
  return(NA)
}

# Aplicar la función a dfB dentro de cada grupo por número de documento
PAGOS <- PAGOS %>%
  group_by(ASO_IDENTIFICACION_ASOCIADO) %>%
  mutate(NUMERO_CONTRATO_EXT = sapply(RCDO_NUMERO_PAGARE, extraer_contrato, ASO_IDENTIFICACION_ASOCIADO = ASO_IDENTIFICACION_ASOCIADO, CONTRATOS = CONTRATO$NUMERO_DE_CONTRATO)) %>%
  ungroup()

# Filtrar filas que tienen un número de contrato extraído
PAGOS_FILTRADO <- PAGOS %>%
  filter(!is.na(NUMERO_CONTRATO_EXT))

ULT_PAGO <- PAGOS_FILTRADO %>% 
  distinct(NUMERO_CONTRATO_EXT,.keep_all = TRUE) %>% 
  transform(NUMERO_CONTRATO_EXT = as.numeric(NUMERO_CONTRATO_EXT))

# Sumar los valores facturados en BASE_FACTURA agrupados por el número de contrato y número de documento
PAGOS_SUM <- PAGOS_FILTRADO %>%
  group_by(NUMERO_CONTRATO_EXT, ASO_IDENTIFICACION_ASOCIADO) %>%
  summarise(SUMA_PAGO_CONTRATO = sum(RECAUDO)) %>% 
  transform(NUMERO_CONTRATO_EXT=as.numeric(NUMERO_CONTRATO_EXT)) %>% 
  left_join(ESTADOS, by = c("ASO_IDENTIFICACION_ASOCIADO"="ASO_IDENTIFICACION")) %>% 
  left_join(ULT_PAGO %>% select(NUMERO_CONTRATO_EXT,
                                HRCD_FECHA_RECAUDO), by = c("NUMERO_CONTRATO_EXT"="NUMERO_CONTRATO_EXT"))

BASE_FACTURA <- BASE_FACTURA %>%
  semi_join(CONTRATO, by=c("ASO_IDENTIFICACION"="IDENTIFICACION_ASOCIADO")) %>% 
  transform(FACT_NUMERO_PAGARE=as.character(FACT_NUMERO_PAGARE))

# Función para verificar la presencia del número de contrato en el número de pagaré
extraer_contrato <- function(FACT_NUMERO_PAGARE, ASO_IDENTIFICACION, CONTRATOS) {
  CONTRATOS <- CONTRATOS[CONTRATO$IDENTIFICACION_ASOCIADO == ASO_IDENTIFICACION]  # Filtrar contratos por número de documento
  for (NUMERO_DE_CONTRATO in CONTRATOS) {
    if (str_detect(FACT_NUMERO_PAGARE, NUMERO_DE_CONTRATO)) {
      return(NUMERO_DE_CONTRATO)
    }
  }
  return(NA)
}

# Aplicar la función a dfB dentro de cada grupo por número de documento
BASE_FACTURA <- BASE_FACTURA %>%
  group_by(ASO_IDENTIFICACION) %>%
  mutate(NUMERO_CONTRATO = sapply(FACT_NUMERO_PAGARE, extraer_contrato, ASO_IDENTIFICACION = ASO_IDENTIFICACION, CONTRATOS = CONTRATO$NUMERO_DE_CONTRATO)) %>%
  ungroup()

# Filtrar filas que tienen un número de contrato extraído
BASE_FACTURA_filtrado <- BASE_FACTURA %>%
  filter(!is.na(NUMERO_CONTRATO))

# Sumar los valores facturados en BASE_FACTURA agrupados por el número de contrato y número de documento
BASE_FACTURA_sum <- BASE_FACTURA_filtrado %>%
  group_by(NUMERO_CONTRATO, ASO_IDENTIFICACION) %>%
  summarise(SUMA_VENCIDO_CONTRATO = sum(FACT_VALOR_VENCIDO)) %>% 
  transform(NUMERO_CONTRATO= as.numeric(NUMERO_CONTRATO))

BASE_FACTURA_sum1 <- BASE_FACTURA_sum %>% 
  left_join(ESTADOS, by = c("ASO_IDENTIFICACION"="ASO_IDENTIFICACION")) %>%
  left_join(PAGOS_SUM %>% select(ASO_IDENTIFICACION_ASOCIADO,
                                 NUMERO_CONTRATO_EXT,
                                 SUMA_PAGO_CONTRATO,
                                 HRCD_FECHA_RECAUDO),by = c("NUMERO_CONTRATO"="NUMERO_CONTRATO_EXT"))

PAGOS_SUM <- PAGOS_SUM %>% 
  left_join(BASE_FACTURA_sum %>% select(NUMERO_CONTRATO,
                                        SUMA_VENCIDO_CONTRATO),by = c("NUMERO_CONTRATO_EXT"="NUMERO_CONTRATO"))

TODAY <- Sys.Date()

write_xlsx(BASE_FACTURA_sum1,glue("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Alivio MP/PAGO ALIVIO/Pago_{TODAY}.xlsx"))

