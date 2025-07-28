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

## Corrección Reporte - 10 Cierre ##

r10_Cierre <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio/GCC_REPORTE10_CIERRE_202506_v20250701.xlsx")
 
## Validar cedulas faltantes con el ultimo dia del mes ##

r10_ultimodia <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio/Reporte10_2025-06-27.xlsx")

## Igualar Nombre de Columnas ##

colnames(r10_Cierre) <- colnames(r10_ultimodia)


r10_Cierre <- r10_Cierre %>%
  mutate(across(all_of(intersect(names(r10_Cierre), names(r10_ultimodia))),
                ~ as(., typeof(r10_ultimodia[[cur_column()]]))))

cedulas_faltantes <- anti_join(r10_ultimodia,r10_Cierre, by = "CEDULA ASOCIADO")

r10_Cierre <- r10_Cierre %>% bind_rows(cedulas_faltantes)

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)
r10_ultimodia <- r10_ultimodia %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>%
  inner_join(r10_ultimodia %>% select(`CEDULA ASOCIADO`), 
             by = "CEDULA ASOCIADO")

## Arreglar Vacios en Edad de mora inicial y edad de mora Hoy ##

r10_Cierre <- r10_Cierre %>% mutate(`EDAD CARTERA INICIAL` = if_else(is.na(`EDAD CARTERA INICIAL`),0,`EDAD CARTERA INICIAL`))

r10_Cierre <- r10_Cierre %>% mutate(`EDAD CARTERA HOY` = if_else(is.na(`EDAD CARTERA HOY`),0,`EDAD CARTERA HOY`))

## Validar Valor Vencido periodo con el r10 de inicio de mes ##

r10_inicio <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio/Reporte10_2025-06-04_INICIO_USO_INTERNO.xlsx")

r10_Cierre <- r10_Cierre %>% left_join(r10_inicio %>% select(`CEDULA ASOCIADO`, `VALOR VENCIDO PERIODO`), by = "CEDULA ASOCIADO")

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(VALOR_VENCIDO_PERIODO_INICIAL = `VALOR VENCIDO PERIODO.y`)

r10_Cierre <- r10_Cierre %>% rename(`VALOR VENCIDO PERIODO` = `VALOR VENCIDO PERIODO.x`)

r10_Cierre <- r10_Cierre %>% mutate(VALI_VALOR_VENCIDO_PERIODO = if_else(`VALOR VENCIDO PERIODO` == VALOR_VENCIDO_PERIODO_INICIAL,"OK","DIF"))

r10_Cierre <- r10_Cierre %>% mutate(
  `VALOR VENCIDO PERIODO` = case_when(
    is.na(VALOR_VENCIDO_PERIODO_INICIAL) ~ `VALOR VENCIDO PERIODO`,
    VALI_VALOR_VENCIDO_PERIODO == "DIF" & `EDAD CARTERA INICIAL` == 0 ~ `VALOR VENCIDO PERIODO`,
    VALI_VALOR_VENCIDO_PERIODO == "DIF" & `EDAD CARTERA INICIAL` != 0 ~ VALOR_VENCIDO_PERIODO_INICIAL,
    TRUE ~ `VALOR VENCIDO PERIODO`
  )
)

r10_Cierre <- r10_Cierre %>% select(-VALI_VALOR_VENCIDO_PERIODO,-VALOR_VENCIDO_PERIODO_INICIAL)

## Validar estados del reporte 10 con el primer dia del mes siguiente ##

r10_mes_siguiente <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo/Reporte10_2025-07-02.xlsx")

r10_Cierre <- r10_Cierre %>% left_join(r10_mes_siguiente %>% select(`CEDULA ASOCIADO`,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`), by = "CEDULA ASOCIADO")

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(ESTADO_HOY = `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.y`)

r10_Cierre <- r10_Cierre %>% rename(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` = `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.x`)

r10_Cierre <- r10_Cierre %>% mutate(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` = case_when(is.na(ESTADO_HOY) ~ `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,
                                                                                         `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` != ESTADO_HOY ~ ESTADO_HOY,
                                                                                         TRUE ~ `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` ))
r10_Cierre <- r10_Cierre %>% select(-ESTADO_HOY)

## Corregir edad de cartera hoy con la edad de la cartera inicial del primer dia del mes siguiente ##

r10_Cierre <- r10_Cierre %>% left_join(r10_mes_siguiente %>% select(`CEDULA ASOCIADO`,`EDAD CARTERA INICIAL`), by = "CEDULA ASOCIADO")

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(EDAD_HOY = `EDAD CARTERA INICIAL.y`)

r10_Cierre <- r10_Cierre %>% rename(`EDAD CARTERA INICIAL` = `EDAD CARTERA INICIAL.x`)

r10_Cierre <- r10_Cierre %>% mutate(`EDAD CARTERA HOY` = case_when(is.na(EDAD_HOY)~`EDAD CARTERA HOY`,
                                                                   `EDAD CARTERA HOY` != EDAD_HOY ~ EDAD_HOY,
                                                                   TRUE ~ `EDAD CARTERA HOY` ))

r10_Cierre <- r10_Cierre %>% select(-EDAD_HOY)

## CORREGIR ACTOR DE GESTIÓN ##

r10_Cierre <- r10_Cierre %>% left_join(r10_ultimodia %>% select(`CEDULA ASOCIADO`,`ACTOR DE GESTIÓN HOY`), by = c("CEDULA ASOCIADO"))

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(ACTOR_HOY = `ACTOR DE GESTIÓN HOY.y`)

r10_Cierre <- r10_Cierre %>% rename(`ACTOR DE GESTIÓN HOY` = `ACTOR DE GESTIÓN HOY.x`)

r10_Cierre <- r10_Cierre %>% mutate(`ACTOR DE GESTIÓN HOY` = case_when(is.na(ACTOR_HOY) ~ `ACTOR DE GESTIÓN HOY`,
                                                                             `ACTOR DE GESTIÓN HOY` != ACTOR_HOY ~ ACTOR_HOY,
                                                                             TRUE ~ `ACTOR DE GESTIÓN HOY`))
r10_Cierre <- r10_Cierre %>% select(-ACTOR_HOY)


##  CORREGIR SUBCAMPAÑA ##

r10_Cierre <- r10_Cierre %>% left_join(r10_ultimodia %>% select(`CEDULA ASOCIADO`, SUBCAMPAÑA), by = c("CEDULA ASOCIADO"))

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(SUBCAMPAÑA_HOY = SUBCAMPAÑA.y)

r10_Cierre <- r10_Cierre %>% rename(SUBCAMPAÑA = SUBCAMPAÑA.x)

r10_Cierre <- r10_Cierre %>% mutate(SUBCAMPAÑA = case_when(is.na(SUBCAMPAÑA_HOY) ~ SUBCAMPAÑA,
                                                                       SUBCAMPAÑA != SUBCAMPAÑA_HOY ~ SUBCAMPAÑA_HOY,
                                                                       TRUE ~ SUBCAMPAÑA))
r10_Cierre <- r10_Cierre %>% select(-SUBCAMPAÑA_HOY)

## CORREGIR USUARIO ##

r10_Cierre <- r10_Cierre %>% left_join(r10_ultimodia %>% select(`CEDULA ASOCIADO`, `USUARIO ASIGNADO`), by = c("CEDULA ASOCIADO"))

r10_Cierre <- r10_Cierre %>% distinct(`CEDULA ASOCIADO`, .keep_all = TRUE)

r10_Cierre <- r10_Cierre %>% rename(USUARIO_HOY = `USUARIO ASIGNADO.y`)

r10_Cierre <- r10_Cierre %>% rename(`USUARIO ASIGNADO` = `USUARIO ASIGNADO.x`)

r10_Cierre <- r10_Cierre %>% mutate(`USUARIO ASIGNADO` = case_when(is.na(USUARIO_HOY) ~ `USUARIO ASIGNADO`,
                                                           `USUARIO ASIGNADO` != USUARIO_HOY ~ USUARIO_HOY,
                                                           TRUE ~ `USUARIO ASIGNADO`))
r10_Cierre <- r10_Cierre %>% select(-USUARIO_HOY)

## CORREGIR ESTADO DE LOS INACTIVOS CON EL MOVIMIENTO DE INACTIVOS ##

Mov_inactivos <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/00. Comité Interno Recaudo/2. Reportes de Cierre/Reportes Cierre_2025/2. Inactivos/1.2. Dinamica Inactivos/06.Movimiento_Inactivos Jun-2025.xlsx", sheet = "POBLACION FINAL")

r10_Cierre <- r10_Cierre %>% 
  mutate(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` = case_when(
    `CEDULA ASOCIADO` %in% Mov_inactivos$CEDULA & `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` != "Inactivo" ~ "Inactivo",
    TRUE ~ `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` 
  ))


## CORREGIR VALOR VENCIDO PERIODO ##

r10_Cierre <- r10_Cierre %>%
  mutate(VENCIDO_PERIODO_CORREG = case_when(
    `VALOR VENCIDO HOY` < (`VALOR VENCIDO PERIODO` - `VALOR RECAUDO VENCIDO`)  ~ `VALOR VENCIDO HOY` + `VALOR RECAUDO VENCIDO`,
    TRUE ~ NA_real_  # pone NA
  ))

r10_Cierre <- r10_Cierre %>%
  mutate(`VALOR VENCIDO PERIODO` = case_when(is.na(VENCIDO_PERIODO_CORREG) ~ `VALOR VENCIDO PERIODO`,
                                             TRUE ~ VENCIDO_PERIODO_CORREG ))


r10_Cierre <- r10_Cierre %>% select(-VENCIDO_PERIODO_CORREG)

## TODOS LOS ASOCIADOS CON EDAD DE CARTERA HOY CERO Y % RECAUDO < 100% SE LES DEBE IGUALAR EL VALOR VENCIDO PERIODO AL VALOR RECAUDO VENCIDO ##

r10_Cierre <- r10_Cierre %>% mutate(`% RECAUDO` = if_else(`VALOR VENCIDO PERIODO` == 0, 0,(`RECAUDO TOTAL`/`VALOR VENCIDO PERIODO`)))

r10_Cierre <- r10_Cierre %>% mutate(`VALOR VENCIDO PERIODO` = if_else(`% RECAUDO` < 1 & `EDAD CARTERA HOY` == 0, `VALOR RECAUDO VENCIDO`,`VALOR VENCIDO PERIODO`))

## Escribir Nuevo Reporte 10 ##

write_xlsx(r10_Cierre,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/6. Junio/GCC_REPORTE10_CIERRE_202506_v20250701_JJVT.xlsx")
