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

conflicts_prefer(dplyr::filter)

### CODIGO REPORTE 10 - PROVISIONAL ###

## Se lee el r-10 mas actual ##

Ruta<- ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

r10 <- read_excel(ultimo_archivo)


## Estado Multiactiva Hoy ## 

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KysCoL_140725*")

Base_Estados <- dbGetQuery(Connecion_SQL,glue_sql(
  "select
     aso.aso_identificacion,
     aso.aso_fecha_nacimiento,
     reg.reg_nombre,
     zon.zon_nombre,
     stm.estm_nombre,
     aso.aso_fecha_cambio_estado_mult,
     aso.aso_fecha_ingreso
     FROM grc.gcc_asociado aso
     LEFT OUTER JOIN grc.GCC_ESTADOS_MULTIACTIVA  stm
        ON aso.estm_codigo = stm.estm_codigo
     LEFT OUTER JOIN grc.GCC_REGIONAL reg
        ON TRIM(aso.reg_codigo) = TRIM(reg.reg_codigo)
     LEFT OUTER JOIN grc.GCC_ZONA zon
        ON TRIM(aso.zon_codigo) = TRIM(zon.zon_codigo);"))

r10 <- r10 %>% 
  left_join(Base_Estados %>% select(ASO_IDENTIFICACION, ESTM_NOMBRE), 
            by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))

r10 <- r10 %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10 <- r10 %>%
  mutate(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` = case_when(
    `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` == ESTM_NOMBRE ~ `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,
    TRUE ~ ESTM_NOMBRE  # Si no es igual, reemplaza con ESTM_NOMBRE
  ))

r10 <- r10 %>% select(-ESTM_NOMBRE)

## Edad de Cartera Hoy y Valor Vencido Hoy ##

Coomeva_hoy <- dbGetQuery(Connecion_SQL,glue_sql("SELECT ASO_IDENTIFICACION,
                                                   MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY_ESTATU,
                                                  SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
                                                  SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY
                                                  FROM GRC.STG_GCC_FACTURACION FAC
                                                  LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
                                                  ON FAC.CON_CODIGO = CON.CON_CODIGO
                                                  LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
                                                  ON CON.AGR_CODIGO = EMP.AGR_CODIGO
                                                  WHERE emp.age_codigo = 3
                                                  AND FAC.CON_CODIGO IN ('APORTES','CALAMID','RECREA','AUX.FUN.','SOLIDAR.','SOLIDARE')
                                                  GROUP BY (FAC.ASO_IDENTIFICACION);"))
                                                  
Estado_cuenta_hoy <- dbGetQuery(Connecion_SQL,glue_sql("SELECT ASO_IDENTIFICACION,
                                                        MAX(CASE WHEN FACT_NUMERO_CUOTAS_ATRASADAS >= 7 THEN 7 ELSE FACT_NUMERO_CUOTAS_ATRASADAS END) EDAD_CARTERA_HOY_TOTAL,
                                                           SUM(FAC.FACT_VALOR_CUOTA_MES) VALOR_CUOTA_MES_HOY,
                                                          SUM(FAC.FACT_VALOR_VENCIDO) VALOR_VENCIDO_HOY
                                                          FROM GRC.STG_GCC_FACTURACION FAC
                                                          LEFT OUTER JOIN GRC.GCC_CONCEPTO CON
                                                          ON FAC.CON_CODIGO = CON.CON_CODIGO
                                                          LEFT OUTER JOIN GRC.GCC_AGRUPACION EMP
                                                          ON CON.AGR_CODIGO = EMP.AGR_CODIGO
                                                          AND FAC.CON_CODIGO NOT IN 'LIBRANZA'
                                                          GROUP BY (FAC.ASO_IDENTIFICACION);"))

## Edad Cartera Hoy ##

Estado_cuenta_hoy <- Estado_cuenta_hoy %>% left_join(Coomeva_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY_ESTATU),by = "ASO_IDENTIFICACION")

Estado_cuenta_hoy <- Estado_cuenta_hoy %>% mutate(EDAD_CARTERA_HOY_ACTUALIZADA = if_else(EDAD_CARTERA_HOY_ESTATU == 0 | is.na(EDAD_CARTERA_HOY_ESTATU), EDAD_CARTERA_HOY_TOTAL, EDAD_CARTERA_HOY_ESTATU))

r10 <- r10 %>% left_join(Estado_cuenta_hoy %>% select(ASO_IDENTIFICACION,EDAD_CARTERA_HOY_ACTUALIZADA), by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))

r10 <- r10 %>% mutate(`EDAD CARTERA HOY` = if_else(`EDAD CARTERA HOY` == EDAD_CARTERA_HOY_ACTUALIZADA,`EDAD CARTERA HOY`,EDAD_CARTERA_HOY_ACTUALIZADA))

r10 <- r10 %>%select(-EDAD_CARTERA_HOY_ACTUALIZADA)

## Valor Vencido Hoy##

r10 <- r10 %>% left_join(Estado_cuenta_hoy %>% select(ASO_IDENTIFICACION,VALOR_VENCIDO_HOY,VALOR_CUOTA_MES_HOY), by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))

r10 <- r10 %>% mutate(`VALOR VENCIDO HOY` = if_else(`VALOR VENCIDO HOY` == VALOR_VENCIDO_HOY,`VALOR VENCIDO HOY`,VALOR_VENCIDO_HOY))

r10 <- r10 %>%select(-VALOR_VENCIDO_HOY)

## Valor cuota mes Hoy ##

r10 <- r10 %>% mutate(`VALOR CUOTA MES HOY` = if_else(`VALOR CUOTA MES HOY` == VALOR_CUOTA_MES_HOY,`VALOR CUOTA MES HOY`,VALOR_CUOTA_MES_HOY))

r10 <- r10 %>%select(-VALOR_CUOTA_MES_HOY)

## Valor recaudo Vencido ##

Pagos_hoy <- dbGetQuery(Connecion_SQL,glue_sql("SELECT 
    REC.ASO_IDENTIFICACION_ASOCIADO, 
    SUM(REC.HRCD_VALOR_RECAUDO) AS RECAUDO_TOTAL,
    SUM(CASE 
            WHEN REC.HRCD_PERIODO_FACTURACION < 202507
            THEN REC.HRCD_VALOR_RECAUDO 
            ELSE 0 
        END) AS RECAUDO_VENCIDO
FROM GRC.GCC_HISTORIA_RECAUDO REC
LEFT OUTER JOIN GRC.GCC_CONCEPTO CON ON REC.CON_CODIGO = CON.CON_CODIGO
LEFT OUTER JOIN GRC.GCC_AGRUPACION_EMPRESA EMP ON CON.AGE_CODIGO = EMP.AGE_CODIGO
WHERE 
    REC.HRCD_NRO_TRANSACCION NOT IN ('71','52','51')
    AND REC.HRCD_FECHA_RECAUDO >= TO_DATE('01/07/2025', 'DD/MM/YYYY')
GROUP BY REC.ASO_IDENTIFICACION_ASOCIADO;"))

r10 <- r10 %>% left_join(Pagos_hoy %>% select(ASO_IDENTIFICACION_ASOCIADO,RECAUDO_VENCIDO,RECAUDO_TOTAL), by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION_ASOCIADO"))

r10 <- r10 %>% mutate(`VALOR RECAUDO VENCIDO` = if_else(`VALOR RECAUDO VENCIDO` >= RECAUDO_VENCIDO,`VALOR RECAUDO VENCIDO`,RECAUDO_VENCIDO))

r10 <- r10 %>% mutate(`RECAUDO TOTAL` = if_else(`RECAUDO TOTAL` >= RECAUDO_TOTAL,`RECAUDO TOTAL`,RECAUDO_TOTAL))


## Valores recaudos vencidos para mora 0 ##

Pagos_Mora_1 <- dbGetQuery(Connecion_SQL,glue_sql("SELECT 
                                                   ASO_IDENTIFICACION,
                                                   SUM(HFR_VALOR_RCDO_VENC_FIN + HFR_VALOR_RCDO_VENC_MULT) AS VALOR_RECAUDO_VENCIDO
                                                   FROM GRC.GCC_HIST_FACT_RECAUDO
                                                   WHERE HFR_PERIODO_FACT = '202508'
                                                   GROUP BY ASO_IDENTIFICACION;"))

r10 <- r10 %>% left_join(Pagos_Mora_1, by = c("CEDULA ASOCIADO" = "ASO_IDENTIFICACION"))

## Valor Vencido Periodo ##

r10 <- r10 %>%
  mutate(`VALOR VENCIDO PERIODO` = case_when(
    (`VALOR VENCIDO PERIODO` == 0 | is.na(`VALOR VENCIDO PERIODO`)) & `EDAD CARTERA INICIAL` == 0 & `VALOR VENCIDO PERIODO` < `VALOR VENCIDO HOY` ~ `VALOR VENCIDO HOY`,
    (`VALOR VENCIDO PERIODO` == 0 | is.na(`VALOR VENCIDO PERIODO`)) & `EDAD CARTERA INICIAL` == 0 ~ `VALOR VENCIDO HOY`,
    TRUE ~ `VALOR VENCIDO PERIODO`
  ))

## Recaudo Vencido - Mora 0 ##

r10 <- r10 %>% mutate(`VALOR RECAUDO VENCIDO` = if_else(`EDAD CARTERA INICIAL` == 0,VALOR_RECAUDO_VENCIDO,`VALOR RECAUDO VENCIDO`))

r10 <- r10[, 1:89]

write_xlsx(r10,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/R-10-Provisional.xlsx")