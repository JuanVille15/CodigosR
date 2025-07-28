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

## Leemos el reporte 10 de Hoy ##

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
r10 <- read_excel(ultimo_archivo)

## Filtramos Regional Caribe ##

r10 <- r10 %>% filter(REGIONAL %in% c("Cali","Caribe"),
                      `TIPO CLIENTE` %in% c("Solo Asociado", "Mixto (Asociado/Cliente)"))

## Query estados historicos (r10) ##

Connecion_SQL <- dbConnect( 
  odbc(),
  Driver = "Oracle in instantclient_21_3",
  DBQ = "bdcdpora23.intracoomeva.com.co:1575/CDPORA23", 
  SVC = "GRC", 
  encoding = "ISO-8859-1", 
  UID = "JJVT9593", 
  PWD = "KysCoL_140725*")

hist <- dbGetQuery(Connecion_SQL, glue_sql("select haso_per_fact as Periodo,
                                                   aso_identificacion as Cedula,
                                                   estm_nombre as Estado
                                            from grc.gcc_reporte10_historico
                                            where haso_per_fact between '202501' and '202506'
                                            order by haso_per_fact desc;"))


r10 <- r10 %>% select(`CEDULA ASOCIADO`,`NOMBRE ASOCIADO`, `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`, `DESCRIPCIÓN DEL CORTE`, REGIONAL,ZONA,`NOMBRE OFICINA`, `EDAD CARTERA HOY`, SUBCAMPAÑA)

r10 <- r10 %>% left_join(hist %>% select(CEDULA,PERIODO,ESTADO), by = c("CEDULA ASOCIADO" = "CEDULA"))

r10 <- r10 %>% distinct(`CEDULA ASOCIADO`,PERIODO,.keep_all = TRUE)

r10 <- r10 %>% group_by(`CEDULA ASOCIADO`) %>% 
                    mutate(Qperiodo = n()) %>% 
                            ungroup()

r10 <- r10 %>% mutate(ESTADO = if_else(ESTADO == "Activo Normal", 1, 0))

r10 <- r10 %>% group_by(`CEDULA ASOCIADO`) %>% 
                    mutate(sumESTADO = sum(ESTADO)) %>% 
                            ungroup()

r10 <- r10 %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE) %>% 
                    mutate(Prob_st10 = sumESTADO/Qperiodo)

r10 <- r10 %>% mutate(Prob_st10 = round(Prob_st10,2))

r10 <- r10 %>% select(-PERIODO,-ESTADO)

## Ahora vamos a consultar como quedaron apenas paso su corte ##

hist_Cortes <- dbGetQuery(Connecion_SQL, glue_sql("SELECT 
                                                  DISTINCT ASO_IDENTIFICACION AS IDENTIFICACION_ASOCIADO,
                                                  CASE 
                                                   WHEN HFR_PERIODO_FACT = 202502 THEN 202501
                                                   WHEN HFR_PERIODO_FACT = 202503 THEN 202502
                                                   WHEN HFR_PERIODO_FACT = 202504 THEN 202503
                                                   WHEN HFR_PERIODO_FACT = 202505 THEN 202504
                                                   WHEN HFR_PERIODO_FACT = 202506 THEN 202505
                                                   WHEN HFR_PERIODO_FACT = 202507 THEN 202506
                                                  ELSE HFR_PERIODO_FACT
                                                  END AS PERIODO_CORTE,
                                                 MAX(HFR_ESTM_CODIGO_INI) AS ESTADO_INICIAL
                                                  FROM GRC.GCC_HIST_FACT_RECAUDO
                                                  WHERE HFR_PERIODO_FACT BETWEEN 202502 AND 202507
                                                  AND AGE_NOMBRE = 'COOMEVA'
                                                  GROUP BY 
                                                    ASO_IDENTIFICACION,
                                                    CASE 
                                                   WHEN HFR_PERIODO_FACT = 202502 THEN 202501
                                                   WHEN HFR_PERIODO_FACT = 202503 THEN 202502
                                                   WHEN HFR_PERIODO_FACT = 202504 THEN 202503
                                                   WHEN HFR_PERIODO_FACT = 202505 THEN 202504
                                                   WHEN HFR_PERIODO_FACT = 202506 THEN 202505
                                                   WHEN HFR_PERIODO_FACT = 202507 THEN 202506
                                                   ELSE HFR_PERIODO_FACT
                                                    END;"))



hist_Cortes <- hist_Cortes %>% mutate(ESTADO_INICIAL = as.double(ESTADO_INICIAL),
                                      ESTADO_INICIAL = if_else(is.na(ESTADO_INICIAL),10, ESTADO_INICIAL))

r10 <- r10 %>% left_join(hist_Cortes %>% select(IDENTIFICACION_ASOCIADO,PERIODO_CORTE,ESTADO_INICIAL), by = c("CEDULA ASOCIADO" = "IDENTIFICACION_ASOCIADO"))

r10_NA <- r10 %>% filter(is.na(ESTADO_INICIAL))

r10 <- r10 %>% mutate(ESTADO_INICIAL = as.double(ESTADO_INICIAL))

r10 <- r10 %>% mutate(ESTADO_INICIAL = if_else(ESTADO_INICIAL == 10, 1 , 0))

r10 <- r10 %>% group_by(`CEDULA ASOCIADO`) %>% 
                  mutate(sumEstado_corte = sum(ESTADO_INICIAL)) %>% 
                          ungroup()

r10 <- r10 %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE )

r10 <- r10 %>% mutate(ProbSt10_corte = sumEstado_corte/Qperiodo)

r10 <- r10 %>% select(-PERIODO_CORTE,-ESTADO_INICIAL)

r10 <- r10 %>% mutate(ProbSt10_corte = if_else(ProbSt10_corte > 1, 1, ProbSt10_corte))

write_xlsx(r10,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/Probabilidades_st10_Cali_Caribe.xlsx")
