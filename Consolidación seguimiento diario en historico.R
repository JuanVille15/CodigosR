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
library(purrr)

conflicts_prefer(dplyr::filter)

## Vamos a pegar el seguimiento diario a el archivo historico (2024 - 2025)

hist <- read_csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Insumo_Tablero/insumo.csv")

## Leemos el seguimiento de hoy ##

ruta <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Resultado/Seguimientos"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

data <- data %>% mutate(Periodo = "202507") ## Creamos periodo actual

## Homologación Actor de Gestión ##

data <- data %>% mutate(ACTOR_GESTION_HOMOLOGADO = case_when(SUBCAMPAÑA %in% c("CSSMORA1CORTE5","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20",
                                                                               "CSSMORA1CORTE25","CSSMORA1CORTE30","CSS MORA 2","CSS MORA 3","CSS MORA 4",
                                                                               "CSSCUOTA0HASTA6","REG BOGOTA","REG CALI","REG MEDELLIN","REG EJECAFETERO","REG PALMIRA","REG CARIBE", "CSS MORA 5","CSS MORA 6","CSS MORA 7") ~ "Moras Tempranas",
                                                             SUBCAMPAÑA == "BANCOOMEVA_U" ~ "Bancoomeva",
                                                             SUBCAMPAÑA %in% c("CARTERA CASTIGO","MORA 2 - 5 CR","MORA 1 CR","MORA 2 - 3 CR", "MORA >= 150 CR","CR INSOLVENCIA","MORA 0 CR","CR EMPRESA" , "MORA >= 4 CR", "PREVENTIVO CR",
                                                                               "JURIDICOS CR", "CLAUSULA CR") ~ "Creditos",
                                                             SUBCAMPAÑA %in% c("CSSALTAPRIORID", "CSSBAJAPRIORID") ~ "Inactivos",
                                                             TRUE ~ SUBCAMPAÑA ))

columnas <- colnames(hist)

data <- data %>% select(columnas)

colnames(hist) <- colnames(data)

## cambiamos todos los tipos de columna ##

data <- data %>% mutate(across(everything(), as.character))

hist <- hist %>% mutate(across(everything(), as.character))

hist <- hist %>% filter(Periodo != 202507) %>% ## filtramos periodo actual y lo reemplazamos por el del dia de hoy ##
                    bind_rows(data)

hist <- hist %>% mutate(ACTOR_GESTION_HOMOLOGADO = if_else(ACTOR_GESTION_HOMOLOGADO == "MORA >= 4 CR", "Creditos", ACTOR_GESTION_HOMOLOGADO))

hist <- hist %>% filter(! `CEDULA ASOCIADO` %in% c(800208092,
                                                   805000427,
                                                   805030765,
                                                   811016426,
                                                   816001182,
                                                   900015339))

## Escribimos el historico ##

write_csv(hist,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Insumo_Tablero/insumo.csv")