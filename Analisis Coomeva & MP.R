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


## Leemos las cedulas ##

reporte <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Cedulas Estados/Cedulas_Consulta.xlsx")

## Consulta Coomeva ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "jjvt9593", 
  PWD = "KyCo_090625*")

## Información Asociado ##

Info_Asociado <- dbGetQuery(Connecion_SQL,glue_sql("SELECT 
  aso_identificacion AS cedula_Asociado,
  aso_primer_nombre ||' '|| aso_segundo_nombre ||' '|| aso_primer_apellido ||' '||aso_segundo_apellido AS Nombre_Asociado,
  aso_fecha_ingreso as Fecha_Vinculacion,
  cort.cor_descripcion as Corte_Actual,
  reg.reg_nombre as Regional,
  est.estm_nombre as Estado_Multiactiva,
  aso_subcampanna_asignada as Canal_R10
  FROM 
  grc.gcc_asociado aso
LEFT JOIN
     grc.gcc_corte_facturacion cort 
on cort.cor_numero_corte = aso.cor_numero_corte
LEFT JOIN
     grc.gcc_regional reg 
on reg.reg_codigo = aso.reg_codigo
LEFT JOIN grc.gcc_estados_multiactiva est
on est.estm_codigo = aso.estm_codigo;"))


## Cruzar Info ##
reporte <- reporte %>% left_join(Info_Asociado, by = c("ASO_IDENTIFICACION" = "CEDULA_ASOCIADO"))

reporte <- reporte %>% rename(CEDULA_ASOCIADO = ASO_IDENTIFICACION)

## Coomeva Hoy ## 

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


## Cruzamos las bases ##

reporte <- reporte %>% left_join(Coomeva_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY), by = c("CEDULA_ASOCIADO" = "ASO_IDENTIFICACION"))

reporte <- reporte %>% rename(Edad_Hoy_Coomeva = EDAD_CARTERA_HOY)

reporte <- reporte %>% rename(Valor_Vencido_Coomeva = VALOR_VENCIDO_HOY)

## MP Hoy ##

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

## Cruzamos las bases ##

reporte <- reporte %>% left_join(Mp_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY,VALOR_VENCIDO_HOY), by = c("CEDULA_ASOCIADO" = "ASO_IDENTIFICACION"))

reporte <- reporte %>% rename(Edad_Hoy_MP = EDAD_CARTERA_HOY)

reporte <- reporte %>% rename(Valor_Vencido_MP = VALOR_VENCIDO_HOY)

## Leemos el seguimiento de hoy ##

Ruta <- ruta_carpeta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Resultado/Seguimientos"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

r10 <- read_excel(ultimo_archivo)

r10 <- r10 %>% mutate(`CEDULA ASOCIADO` = as.double(`CEDULA ASOCIADO`))

reporte <- reporte %>% left_join(r10 %>% select(`CEDULA ASOCIADO`,`EDAD CARTERA INICIAL`,`EDAD CARTERA HOY`,Qgestiones,Respuesta,motivo_nopago,Contacto,`Contacto Directo`,Efectivo), by = c("CEDULA_ASOCIADO" = "CEDULA ASOCIADO"))

reporte <- reporte %>% select(CEDULA_ASOCIADO,NOMBRE_ASOCIADO,FECHA_VINCULACION,CORTE_ACTUAL,REGIONAL, ESTADO_MULTIACTIVA,CANAL_R10,`EDAD CARTERA INICIAL`,`EDAD CARTERA HOY`,
                              Edad_Hoy_Coomeva,Valor_Vencido_Coomeva,Edad_Hoy_MP,Valor_Vencido_MP,Qgestiones,Respuesta,motivo_nopago,Contacto,`Contacto Directo`,Efectivo)

## Extraemos R10 desde GCC ##

r10_gcc <- dbGetQuery(Connecion_SQL, glue_sql("select aso_identificacion,
                                               haso_edad_cartera_ini,
                                               haso_edad_cartera_hoy
                                               from grc.gcc_reporte10;"))

reporte <- reporte %>% left_join(r10_gcc, by = c("CEDULA_ASOCIADO" = "ASO_IDENTIFICACION"))

reporte <- reporte %>% select(-`EDAD CARTERA INICIAL`,-`EDAD CARTERA HOY`)

reporte <- reporte %>% select(CEDULA_ASOCIADO, NOMBRE_ASOCIADO, HASO_EDAD_CARTERA_INI,HASO_EDAD_CARTERA_HOY, everything())

reporte <- reporte %>% rename(Edad_Inicial = HASO_EDAD_CARTERA_INI,
                              Edad_Hoy = HASO_EDAD_CARTERA_HOY,
                              Subcampaña = CANAL_R10)

reporte <- reporte %>% mutate(Edad_Inicial = if_else(is.na(Edad_Inicial),0,Edad_Inicial),
                              Edad_Hoy = if_else(is.na(Edad_Hoy),0,Edad_Hoy))

## ESCRIBIR INFORME ##

write_xlsx(reporte,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Analisis MP cedulas vencidas09072025.xlsx")
