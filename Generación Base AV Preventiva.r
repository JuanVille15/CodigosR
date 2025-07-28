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

##conflicts_prefer(dplyr::filter)

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)


# filtramos corte necesario #

data <- data %>% filter(`DESCRIPCIÓN DEL CORTE` == "Al 25 del Mes",
                         `TIPO CLIENTE` %in% c("Solo Asociado", "Mixto (Asociado/Cliente)"),
                         `VALOR CUOTA MES HOY` > 3000,
                         `EDAD CARTERA HOY` == 0,
                         `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` %in% c("Activo Normal","Activo Cobranza Interna","Inactivo"),
                          ! SUBCAMPAÑA %in% c("ASORETENIDOS", "VIDE"))

## Nos traemos la segmentación ##

Segmentacion_mensual <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Matriz_riesgo_monto Nov2024.xlsx")


data <- data %>% inner_join(Segmentacion_mensual %>% select(Segmentacion, Riesgo), by = c("SEGMENTO" = "Segmentacion"))


## CREAR COLUMNA PRIORIDAD MONTO - CAMBIAR VALORES CUANDO SE NECESITE ## 

MONTO_BAJO <- 149767.5
MONTO_MEDIO <- 222666
MONTO_ALTO <- 416235 

data <- data %>%
  mutate(PRIORIDAD_MONTO = case_when(`VALOR CUOTA MES HOY` <= MONTO_BAJO ~ "Monto Bajo",
                                     `VALOR CUOTA MES HOY` > MONTO_BAJO & `VALOR VENCIDO PERIODO`<MONTO_ALTO ~ "Monto Medio",
                                     `VALOR CUOTA MES HOY` >= MONTO_ALTO ~ "Monto Alto" ))

## Crear prioridad riesgo ##

data <- data %>% rename(PRIORIDAD_RIESGO= Riesgo) %>%
mutate(PRIORIDAD_RIESGO = ifelse(is.na(PRIORIDAD_RIESGO),"Riesgo bajo", PRIORIDAD_RIESGO))

## CREAR COLUMNA PRIORIDAD DE GESTIÓN ##

data <- data %>%
  mutate(PRIORIDAD_GESTION = case_when(
    PRIORIDAD_RIESGO == "Riesgo Alto" & PRIORIDAD_MONTO %in% c("Monto Alto", "Monto Medio") ~ "Prioridad 1",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Alto" ~ "Prioridad 1",
    PRIORIDAD_RIESGO == "Riesgo bajo" & PRIORIDAD_MONTO == "Monto Alto" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Medio" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo Alto" & PRIORIDAD_MONTO == "Monto Bajo" ~ "Prioridad 2",
    PRIORIDAD_RIESGO == "Riesgo bajo" & PRIORIDAD_MONTO %in% c("Monto Medio", "Monto Bajo") ~ "Prioridad 3",
    PRIORIDAD_RIESGO == "Riesgo medio" & PRIORIDAD_MONTO == "Monto Bajo" ~ "Prioridad 3",
    TRUE ~ NA_character_
  ))

conteo <- data %>% group_by(PRIORIDAD_GESTION) %>%
                        summarise(
                            Qasociados = n()
                        )
## Dejamos Riesgo Alto, Riesgo Medio y NA ##

data <- data %>% filter(PRIORIDAD_GESTION != "Prioridad 3")

## Vamos a quitar a los asociados mayores a 60 años ##

edades <- data%>% distinct(`EDAD ASOCIADO`) %>%
                arrange(`EDAD ASOCIADO`)
                
data <- data %>% filter(`EDAD ASOCIADO` <= 65)

data <- data %>% select(`CEDULA ASOCIADO`, `NOMBRE ASOCIADO`, `DESCRIPCIÓN DEL CORTE`, `TELÉFONO CELULAR`, `VALOR CUOTA MES HOY`, SUBCAMPAÑA,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`)

write_xlsx(data,"//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/7. Julio/Base AV Preventivo/Corte 25.xlsx")
