library(DBI)
library(odbc)
library(writexl)
library(glue)
library(magrittr)
library(dplyr)
library(readxl)
library(conflicted)
library(dbplyr)
library(lubridate)
library(janitor)
library(readr)
library(Microsoft365R)
library(blastula)
library(janitor)

conflict_prefer("filter", "dplyr")

## Inicio de Mes ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KyCo_090625*")

## Cedulas Facturadas ##

Base_Inicial <- dbGetQuery(Connecion_SQL,glue_sql("SELECT ASO_IDENTIFICACION,
                                                        MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY_TOTAL,
                                                           SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
                                                          SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY
                                                          FROM GRC.STG_GCC_FACTURACION FAC
                                                          LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
                                                          ON FAC.CON_CODIGO = CON.CON_CODIGO
                                                          LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
                                                          ON CON.AGR_CODIGO = EMP.AGR_CODIGO
                                                          GROUP BY (FAC.ASO_IDENTIFICACION)"))

## Validacion Coomeva ##

Fac_Coomeva <- dbGetQuery(Connecion_SQL,glue_sql(
  "Select
     ASO_IDENTIFICACION,
    MAX (hfr_numero_cuotas_atrasadas) AS EDAD_MORA_HOY,
    SUM (HFR_VALOR_CUOTA_MES) AS VALOR_TOTAL,
    SUM( HFR_VALOR_VENCIDO_CALCULADO) AS VALOR_VENCIDO,
    MAX(HFR_CUOTAS_ATRASADAS_INI) AS EDAD_INICIAL
     from GRC.GCC_HIST_FACT_RECAUDO
    WHERE hfr_periodo_fact IN ('202507')
    AND AGE_NOMBRE = 'COOMEVA'
   GROUP BY (ASO_IDENTIFICACION)"))

Base_Inicial <- Base_Inicial %>% mutate(Validacion_Coomeva = if_else(Base_Inicial$ASO_IDENTIFICACION %in% Fac_Coomeva$ASO_IDENTIFICACION, "SI","NO"))

Base_Inicial <- Base_Inicial %>% select(ASO_IDENTIFICACION,Validacion_Coomeva)


## Edad, Valor vencido y cuota mes Inicial - Coomeva ##

Coomeva_hoy <- dbGetQuery(Connecion_SQL,glue_sql("SELECT ASO_IDENTIFICACION,
MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES + FAC.FACT_VALOR_VENCIDO) TOTAL_FACTURADO
FROM GRC.STG_GCC_FACTURACION FAC
LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
ON FAC.CON_CODIGO = CON.CON_CODIGO
LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
ON CON.AGR_CODIGO = EMP.AGR_CODIGO
WHERE emp.age_codigo = 3
GROUP BY (FAC.ASO_IDENTIFICACION)"))

Base_Inicial <- Base_Inicial %>% left_join(Coomeva_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY,TOTAL_FACTURADO), by = "ASO_IDENTIFICACION")

Base_Inicial <- Base_Inicial %>% rename(Edad_Inicial_Coomeva = EDAD_CARTERA_HOY)

Base_Inicial <- Base_Inicial %>% rename(Valor_Vencido_Periodo_Coomeva = VALOR_VENCIDO_HOY)

Base_Inicial <- Base_Inicial %>% rename(Total_Fac_Coomeva = TOTAL_FACTURADO)

Base_Inicial <- Base_Inicial %>% mutate(Edad_Inicial_Coomeva = if_else(Validacion_Coomeva == "SI" & is.na(Edad_Inicial_Coomeva),0,Edad_Inicial_Coomeva ))

Base_Inicial <- Base_Inicial %>% mutate(Valor_Vencido_Periodo_Coomeva = if_else(Validacion_Coomeva == "SI" & is.na(Valor_Vencido_Periodo_Coomeva),0,Valor_Vencido_Periodo_Coomeva))

Base_Inicial <- Base_Inicial %>% mutate(Total_Fac_Coomeva = if_else(Validacion_Coomeva == "SI" & is.na(Total_Fac_Coomeva),0,Total_Fac_Coomeva))


### VALIDACION MP ###

Fac_MP <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT
    fac.ASO_IDENTIFICACION,
    MAX(hfr_numero_cuotas_atrasadas) AS EDAD_MORA_HOY,
    SUM(hfr_valor_vencido) AS TOTAL_VENCIDO,
    SUM(hfr_valor_vencido_calculado) AS VALOR_VENCIDO,
    MAX(hfr_cuotas_atrasadas_ini) AS EDAD_INICIAL
FROM
    GRC.GCC_HIST_FACT_RECAUDO fac
WHERE
    age_nombre = 'MEDICINA PREPAGADA'
    AND hfr_periodo_fact IN ('202507')
    AND con_codigo NOT IN (
        'CEMR', '01CEAB', '01CEAD', '01CEAG', '01CEAM', '01CEAP', '01CEAS', '01CEAT', '01CEA9',
        '01CEB', '01CEB1', '01CECA', '01CECE', '01CEC1', '01CEEF', '01CEE1', '01CEE2', '01CEFE',
        '01CEFG', '01CEFI', '01CEFO', '01CEGF', '01CEIN', '01CEM', '01CEMA', '01CEMB', '01CEMC',
        '01CEME', '01CEMH', '01CEMJ', '01CEMO', '01CEMS', '01CEMX', '01CEMZ', '01CEM1', '01CEM3',
        '01CEM4', '01CEM7', '01CEM8', '01CEOC', '01CEPD', '01CEPM', '01CEPO', '01CEPR', '01CEPS',
        '01CEP2', '01CERP', '01CETE', '01CET9', '01CEUS', '01CEX0', '01CEX1', '01CEX3', '01CE19',
        '01CE2P', '01CE23', '01CPEM', '01EVEM', '04CEAB', '04CEAD', '04CEAG', '04CEAM', '04CEAP',
        '04CEAS', '04CEAT', '04CEA9', '04CEB', '04CEB1', '04CECA', '04CECE', '04CEC1', '04CEEF',
        '04CEE1', '04CEE2', '04CEFE', '04CEFG', '04CEFI', '04CEFO', '04CEGF', '04CEIN', '04CEM',
        '04CEMA', '04CEMB', '04CEMC', '04CEME', '04CEMH', '04CEMJ', '04CEMO', '04CEMS', '04CEMX',
        '04CEMZ', '04CEM1', '04CEM3', '04CEM4', '04CEM7', '04CEM8', '04CEOC', '04CEPD', '04CEPM',
        '04CEPO', '04CEPR', '04CEPS', '04CEP2', '04CERP', '04CETE', '04CET9', '04CEUS', '04CEX0',
        '04CEX1', '04CEX3', '04CE19', '04CE2P', '04CE23', '04CPEM', '04EVEM', '07CEM', '07CEMC',
        '07CEMO', '07CEMS', '07CEMZ', '07CEM1', '07CEM3', '07CEM4', '07CEM7', '07CEM8', '08CEM',
        '08CEMC', '08CEMJ', '08CEMO', '08CEMS', '08CEMZ', '08CEM1', '08CEM3', '08CEM4', '08CEM7',
        '08CEM8', '09CEAG', '09CEAP', '09CEAS', '09CEAT', '09CEFE', '09CEFG', '09CEFO', '09CEGF',
        '09CEM', '09CEMC', '09CEMH', '09CEMO', '09CEMZ', '09CEM1', '09CEM3', '09CEM4', '09CEM7',
        '09CEM8', '09CEPD', '09CEPM', '09CEPO', '09CEPR', '09CEPS', '09CEP2', '09CERP', '09CEX3',
        '09CE2P', '01CEMQ', '04CEMQ', '01CE50', '04CE50', '01PACE', '04PACE', '01TACE', '04TACE',
        '01CUE3', '04CUE3', '01CUE6', '04CUE6', '01CUE9', '04CUE9', '01CU12', '04CU12', '01CU23',
        '04CU23', '01CU26', '04CU26', '01CU29', '04CU29', '01CU22', '04CU22', '01CTL3', '04CTL3',
        '01CTL6', '04CTL6', '01CTL9', '04CTL9', '01CT12', '04CT12', '01CT23', '04CT23', '01CT26',
        '04CT26', '01CT29', '04CT29', '01CT22', '04CT22', '01FULL', '04FULL'
    )
GROUP BY
    fac.ASO_IDENTIFICACION;"))


Base_Inicial <- Base_Inicial %>% mutate(Validacion_Mp = if_else(Base_Inicial$ASO_IDENTIFICACION %in% Fac_MP$ASO_IDENTIFICACION, "SI", "NO"))

## Edad, Vencido y facturado inicial. 

Mp_hoy <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT ASO_IDENTIFICACION,
MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES + FAC.FACT_VALOR_VENCIDO) TOTAL_FACTURADO
FROM GRC.STG_GCC_FACTURACION FAC
LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
ON FAC.CON_CODIGO = CON.CON_CODIGO
LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
ON CON.AGR_CODIGO = EMP.AGR_CODIGO
WHERE emp.age_codigo = 2 
AND FAC.CON_CODIGO NOT IN ('CEMR',
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
'04CT22',
'01FULL',
'04FULL')
GROUP BY (FAC.ASO_IDENTIFICACION);"))

Base_Inicial <- Base_Inicial %>% left_join(Mp_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY,TOTAL_FACTURADO), by = "ASO_IDENTIFICACION")

Base_Inicial <- Base_Inicial %>% rename(Edad_Inicial_MP = EDAD_CARTERA_HOY)

Base_Inicial <- Base_Inicial %>% rename(Valor_Vencido_Periodo_MP = VALOR_VENCIDO_HOY)

Base_Inicial <- Base_Inicial %>% rename(Total_Fac_MP = TOTAL_FACTURADO)

Base_Inicial <- Base_Inicial %>% mutate(Edad_Inicial_MP = if_else(Validacion_Mp == "SI" & is.na(Edad_Inicial_MP),0,Edad_Inicial_MP ))

Base_Inicial <- Base_Inicial %>% mutate(Valor_Vencido_Periodo_MP = if_else(Validacion_Mp == "SI" & is.na(Valor_Vencido_Periodo_MP),0,Valor_Vencido_Periodo_MP))

Base_Inicial <- Base_Inicial %>% mutate(Total_Fac_MP = if_else(Validacion_Mp == "SI" & is.na(Total_Fac_MP),0,Total_Fac_MP))

### VALIDACION CEM ###

Fac_CEM <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT
    fac.ASO_IDENTIFICACION,
    MAX(hfr_numero_cuotas_atrasadas) AS EDAD_MORA_HOY,
    SUM(hfr_valor_vencido) AS TOTAL_VENCIDO,
    SUM(hfr_valor_vencido_calculado) AS VALOR_VENCIDO,
    MAX(hfr_cuotas_atrasadas_ini) AS EDAD_INICIAL
FROM
    GRC.GCC_HIST_FACT_RECAUDO fac
WHERE
    age_nombre = 'MEDICINA PREPAGADA'
    AND hfr_periodo_fact IN ('202507')
    AND con_codigo IN (
        'CEMR', '01CEAB', '01CEAD', '01CEAG', '01CEAM', '01CEAP', '01CEAS', '01CEAT', '01CEA9',
        '01CEB', '01CEB1', '01CECA', '01CECE', '01CEC1', '01CEEF', '01CEE1', '01CEE2', '01CEFE',
        '01CEFG', '01CEFI', '01CEFO', '01CEGF', '01CEIN', '01CEM', '01CEMA', '01CEMB', '01CEMC',
        '01CEME', '01CEMH', '01CEMJ', '01CEMO', '01CEMS', '01CEMX', '01CEMZ', '01CEM1', '01CEM3',
        '01CEM4', '01CEM7', '01CEM8', '01CEOC', '01CEPD', '01CEPM', '01CEPO', '01CEPR', '01CEPS',
        '01CEP2', '01CERP', '01CETE', '01CET9', '01CEUS', '01CEX0', '01CEX1', '01CEX3', '01CE19',
        '01CE2P', '01CE23', '01CPEM', '01EVEM', '04CEAB', '04CEAD', '04CEAG', '04CEAM', '04CEAP',
        '04CEAS', '04CEAT', '04CEA9', '04CEB', '04CEB1', '04CECA', '04CECE', '04CEC1', '04CEEF',
        '04CEE1', '04CEE2', '04CEFE', '04CEFG', '04CEFI', '04CEFO', '04CEGF', '04CEIN', '04CEM',
        '04CEMA', '04CEMB', '04CEMC', '04CEME', '04CEMH', '04CEMJ', '04CEMO', '04CEMS', '04CEMX',
        '04CEMZ', '04CEM1', '04CEM3', '04CEM4', '04CEM7', '04CEM8', '04CEOC', '04CEPD', '04CEPM',
        '04CEPO', '04CEPR', '04CEPS', '04CEP2', '04CERP', '04CETE', '04CET9', '04CEUS', '04CEX0',
        '04CEX1', '04CEX3', '04CE19', '04CE2P', '04CE23', '04CPEM', '04EVEM', '07CEM', '07CEMC',
        '07CEMO', '07CEMS', '07CEMZ', '07CEM1', '07CEM3', '07CEM4', '07CEM7', '07CEM8', '08CEM',
        '08CEMC', '08CEMJ', '08CEMO', '08CEMS', '08CEMZ', '08CEM1', '08CEM3', '08CEM4', '08CEM7',
        '08CEM8', '09CEAG', '09CEAP', '09CEAS', '09CEAT', '09CEFE', '09CEFG', '09CEFO', '09CEGF',
        '09CEM', '09CEMC', '09CEMH', '09CEMO', '09CEMZ', '09CEM1', '09CEM3', '09CEM4', '09CEM7',
        '09CEM8', '09CEPD', '09CEPM', '09CEPO', '09CEPR', '09CEPS', '09CEP2', '09CERP', '09CEX3',
        '09CE2P', '01CEMQ', '04CEMQ', '01CE50', '04CE50', '01PACE', '04PACE', '01TACE', '04TACE',
        '01CUE3', '04CUE3', '01CUE6', '04CUE6', '01CUE9', '04CUE9', '01CU12', '04CU12', '01CU23',
        '04CU23', '01CU26', '04CU26', '01CU29', '04CU29', '01CU22', '04CU22', '01CTL3', '04CTL3',
        '01CTL6', '04CTL6', '01CTL9', '04CTL9', '01CT12', '04CT12', '01CT23', '04CT23', '01CT26',
        '04CT26', '01CT29', '04CT29', '01CT22', '04CT22', '01FULL', '04FULL'
    )
GROUP BY
    fac.ASO_IDENTIFICACION;"))

Base_Inicial <- Base_Inicial %>% mutate(Validacion_CEM = if_else(Base_Inicial$ASO_IDENTIFICACION %in% Fac_CEM$ASO_IDENTIFICACION, "SI", "NO"))


## Edad, Vencido, facturado hoy ##

CEM_hoy <- dbGetQuery(Connecion_SQL,glue_sql(
  "SELECT ASO_IDENTIFICACION,
MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY,
SUM(FAC.FACT_VALOR_CUOTA_MES + FAC.FACT_VALOR_VENCIDO) TOTAL_FACTURADO
FROM GRC.STG_GCC_FACTURACION FAC
LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
ON FAC.CON_CODIGO = CON.CON_CODIGO
LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
ON CON.AGR_CODIGO = EMP.AGR_CODIGO
WHERE emp.age_codigo = 2 
AND FAC.CON_CODIGO IN ('CEMR',
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
'04CT22',
'01FULL',
'04FULL')
GROUP BY (FAC.ASO_IDENTIFICACION);"))

Base_Inicial <- Base_Inicial %>% left_join(CEM_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY,TOTAL_FACTURADO), by = "ASO_IDENTIFICACION")

Base_Inicial <- Base_Inicial %>% rename(Edad_Inicial_CEM = EDAD_CARTERA_HOY)

Base_Inicial <- Base_Inicial %>% rename(Valor_Vencido_Periodo_CEM = VALOR_VENCIDO_HOY)

Base_Inicial <- Base_Inicial %>% rename(Total_Fac_CEM = TOTAL_FACTURADO)

Base_Inicial <- Base_Inicial %>% mutate(Edad_Inicial_CEM = if_else(Validacion_CEM == "SI" & is.na(Edad_Inicial_CEM),0,Edad_Inicial_CEM ))

Base_Inicial <- Base_Inicial %>% mutate(Valor_Vencido_Periodo_CEM = if_else(Validacion_CEM == "SI" & is.na(Valor_Vencido_Periodo_CEM),0,Valor_Vencido_Periodo_CEM))

Base_Inicial <- Base_Inicial %>% mutate(Total_Fac_CEM = if_else(Validacion_CEM == "SI" & is.na(Total_Fac_CEM),0,Total_Fac_CEM))

## VALIDACIÓN SEGUROS ## 

Fac_Seguros <- dbGetQuery(Connecion_SQL,glue_sql(
  "Select
    ASO_IDENTIFICACION,
    MAX (hfr_numero_cuotas_atrasadas) AS EDAD_MORA_HOY,
    SUM (hfr_valor_vencido) AS TOTAL_VENCIDO,
    SUM( HFR_VALOR_VENCIDO_CALCULADO) AS VALOR_VENCIDO,
    MAX( HFR_CUOTAS_ATRASADAS_INI) AS EDAD_INICIAL
     from GRC.GCC_HIST_FACT_RECAUDO 
    WHERE CON_CODIGO IN('SEGAUTOC',
'DESEMPLE',
'PYMEBANC',
'SEG ROTU',
'SEGSOAT',
'ASISTAFA',
'VIDACOOM',
'SEG AUTO',
'HOGMASBA',
'SEGINCBA',
'INCCLIBA',
'SEGAUTMD',
'SEGAUTBO',
'AUTOAMPB',
'AUT PESA',
'BOLBILLE',
'CLINIMAS',
'CLINIMA2',
'SEG CUBR',
'EVCRICOO',
'EVCRIMP',
'ECMPFTF',
'FLE.HOG+',
'SEG HOGA',
'HOMEEJEC',
'SEG INCE',
'AUTLIV',
'AUTLIB',
'Seg+Prot',
'SEG MOTO',
'SEGMASCO',
'SEGMOCHI',
'PAGPROTN',
'PAGO SEG',
'PENPRVOL',
'SEG PLAN',
'VIDASOLI',
'SEG.A.EJ',
'SEG GAP',
'AUTOLIV3',
'PROT.EDU',
'SEG PAGP',
'PLAN100L',
'SEGAUTOS',
'SEGMYCAR',
'MOVIBIC',
'AUTOLICF',
'AUTOLIVN',
'LIVESECF',
'LIVESEN',
'MASPRONU',
'VIDAEG',
'VIDARENH',
'VIDA',
'LIVFULL',
'LIVFULCF',
'INCORIGI',
'INCORICF',
'RENTA HG',
'RENTHONU',
'SEG CIVI',
'S.RC.Exc',
'SEG RESC',
'S.RC.CLI',
'RCMEDLIV',
'ORIVIVI',
'RC MED',
'ARCEPT',
'ARCEBA',
'RUNT',
'SEGRUNT',
'EQELECBA',
'TAXIBANC',
'SEG ACCI',
'SEG ELEC',
'SEG.SOAT',
'SEG RENT',
'SEG.VEH.',
'SEGVICBA',
'SEGVIGC',
'SEGMOTBA',
'SEGVEHBA',
'SEGVEHCB',
'VEHPESBA',
'SEG SUST',
'SEG PYME',
'SEGMPSTO',
'CREDASOC',
'SEGVIDVA',
'SEGVGFS',
'SEG TAMP',
'SEG.TAXI',
'TOHOMLIF',
'TOHOMLIV',
'TMKBSEG',
'VGORIGCF')
AND hfr_periodo_fact IN ('202507')
GROUP BY (ASO_IDENTIFICACION)"))

Base_Inicial <- Base_Inicial %>% mutate(Validacion_Seguros = if_else(Base_Inicial$ASO_IDENTIFICACION %in% Fac_Seguros$ASO_IDENTIFICACION, "SI", "NO"))


## Edad, Vencido y facturado Inicial ## 

Seguros_hoy <- dbGetQuery(Connecion_SQL,glue_sql("SELECT ASO_IDENTIFICACION,
                                                 MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY,
                                                 SUM(FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
                                                 SUM(FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY,
                                                 SUM(FACT_VALOR_CUOTA_MES + FACT_VALOR_VENCIDO) TOTAL_FACTURADO
                                                 FROM GRC.STG_GCC_FACTURACION
                                                 WHERE CON_CODIGO IN ('SEGAUTOC',
                                                                      'DESEMPLE',
                                                                      'PYMEBANC',
                                                                      'SEG ROTU',
                                                                      'SEGSOAT',
                                                                      'ASISTAFA',
                                                                      'VIDACOOM',
                                                                      'SEG AUTO',
                                                                      'HOGMASBA',
                                                                      'SEGINCBA',
                                                                      'INCCLIBA',
                                                                      'SEGAUTMD',
                                                                      'SEGAUTBO',
                                                                      'AUTOAMPB',
                                                                      'AUT PESA',
                                                                      'BOLBILLE',
                                                                      'CLINIMAS',
                                                                      'CLINIMA2',
                                                                      'SEG CUBR',
                                                                      'EVCRICOO',
                                                                      'EVCRIMP',
                                                                      'ECMPFTF',
                                                                      'FLE.HOG+',
                                                                      'SEG HOGA',
                                                                      'HOMEEJEC',
                                                                      'SEG INCE',
                                                                      'AUTLIV',
                                                                      'AUTLIB',
                                                                      'Seg+Prot',
                                                                      'SEG MOTO',
                                                                      'SEGMASCO',
                                                                      'SEGMOCHI',
                                                                      'PAGPROTN',
                                                                      'PAGO SEG',
                                                                      'PENPRVOL',
                                                                      'SEG PLAN',
                                                                      'VIDASOLI',
                                                                      'SEG.A.EJ',
                                                                      'SEG GAP',
                                                                      'AUTOLIV3',
                                                                      'PROT.EDU',
                                                                      'SEG PAGP',
                                                                      'PLAN100L',
                                                                      'SEGAUTOS',
                                                                      'SEGMYCAR',
                                                                      'MOVIBIC',
                                                                      'AUTOLICF',
                                                                      'AUTOLIVN',
                                                                      'LIVESECF',
                                                                      'LIVESEN',
                                                                      'MASPRONU',
                                                                      'VIDAEG',
                                                                      'VIDARENH',
                                                                      'VIDA',
                                                                      'LIVFULL',
                                                                      'LIVFULCF',
                                                                      'INCORIGI',
                                                                      'INCORICF',
                                                                      'RENTA HG',
                                                                      'RENTHONU',
                                                                      'SEG CIVI',
                                                                      'S.RC.Exc',
                                                                      'SEG RESC',
                                                                      'S.RC.CLI',
                                                                      'RCMEDLIV',
                                                                      'ORIVIVI',
                                                                      'RC MED',
                                                                      'ARCEPT',
                                                                      'ARCEBA',
                                                                      'RUNT',
                                                                      'SEGRUNT',
                                                                      'EQELECBA',
                                                                      'TAXIBANC',
                                                                      'SEG ACCI',
                                                                      'SEG ELEC',
                                                                      'SEG.SOAT',
                                                                      'SEG RENT',
                                                                      'SEG.VEH.',
                                                                      'SEGVICBA',
                                                                      'SEGVIGC',
                                                                      'SEGMOTBA',
                                                                      'SEGVEHBA',
                                                                      'SEGVEHCB',
                                                                      'VEHPESBA',
                                                                      'SEG SUST',
                                                                      'SEG PYME',
                                                                      'SEGMPSTO',
                                                                      'CREDASOC',
                                                                      'SEGVIDVA',
                                                                      'SEGVGFS',
                                                                      'SEG TAMP',
                                                                      'SEG.TAXI',
                                                                      'TOHOMLIF',
                                                                      'TOHOMLIV',
                                                                      'TMKBSEG',
                                                                      'VGORIGCF')
                                                 GROUP BY(ASO_IDENTIFICACION)"))

Base_Inicial <- Base_Inicial %>% left_join(Seguros_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY,TOTAL_FACTURADO), by = "ASO_IDENTIFICACION")

Base_Inicial <- Base_Inicial %>% rename(Edad_Inicial_Seguros = EDAD_CARTERA_HOY)

Base_Inicial <- Base_Inicial %>% rename(Valor_Vencido_Periodo_Seguros = VALOR_VENCIDO_HOY)

Base_Inicial <- Base_Inicial %>% rename(Total_Fac_Seguros = TOTAL_FACTURADO)

Base_Inicial <- Base_Inicial %>% mutate(Edad_Inicial_Seguros = if_else(Validacion_Seguros == "SI" & is.na(Edad_Inicial_Seguros),0,Edad_Inicial_Seguros ))

Base_Inicial <- Base_Inicial %>% mutate(Valor_Vencido_Periodo_Seguros = if_else(Validacion_Seguros == "SI" & is.na(Valor_Vencido_Periodo_Seguros),0,Valor_Vencido_Periodo_Seguros))

Base_Inicial <- Base_Inicial %>% mutate(Total_Fac_Seguros = if_else(Validacion_Seguros == "SI" & is.na(Total_Fac_Seguros),0,Total_Fac_Seguros))

write_xlsx(Base_Inicial,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Julio/Foto_inicial.xlsx")
