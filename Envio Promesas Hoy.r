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

## Envio Comunicaciones Promesa Hoy ## 


ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/7. Julio/R10"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

data <- data %>% filter(Estado_compromiso == "Promesa Hoy",
                        ESTADO_CARTERA %in% c("DETERIORA", "POR VENCER"))

data <- data %>% select(`CEDULA ASOCIADO`, `NOMBRE ASOCIADO`, `TELÉFONO CELULAR`)%>%
                            mutate(`NOMBRE ASOCIADO` = str_to_title(`NOMBRE ASOCIADO`))

data <- data %>% rename( "TELEFONO_CELULAR" = `TELÉFONO CELULAR`, "CEDULA_ASOCIADO" = `CEDULA ASOCIADO`, "NOMBRE_ASOCIADO" = `NOMBRE ASOCIADO`)

data <- data %>% mutate(NOMBRE_ASOCIADO = word(NOMBRE_ASOCIADO, 1))

nombre_archivo <- paste0("Promesa_Hoy","_",Sys.Date(),".xlsx")

ruta_archivo <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado"

write_xlsx(data, pat = file.path(ruta_archivo,nombre_archivo))