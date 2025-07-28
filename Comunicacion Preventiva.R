library(tidyverse)
library(readxl)
library(writexl)
library(dplyr)
library(glue)
library(dplyr)
library(DBI)
library(odbc)
library(lubridate)
library(janitor)
library(dbplyr)
library(tools)
library(scales)

## Leemos el reporte 10 del dia ## 

ruta_carpeta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"

archivos_con_fechas <- file.info(list.files(path = ruta_carpeta, full.names = TRUE))

archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]

ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]

Reporte_10 <- read_excel(ultimo_archivo)


## Filtramos Subcampaña al dia ##

subcampaña <- 'ALDIA'


Reporte_10 <- Reporte_10 %>%
  filter(`EDAD CARTERA INICIAL` == 0, `EDAD CARTERA HOY` == 0, SUBCAMPAÑA == subcampaña)

# Fecha actual
hoy <- Sys.Date()

# Extraer el día de la fecha actual
dia_hoy <- as.numeric(format(hoy, "%d"))

# Filtrar registros que estén cerca de vencerse (1 o 2 días antes del corte)
Reporte_10 <- Reporte_10 %>%
  mutate(dia_corte = as.numeric(gsub(".*(\\d{2}).*", "\\1", `DESCRIPCIÓN DEL CORTE`))) %>%
  filter(dia_corte >= dia_hoy) %>%
  slice_min(dia_corte)

Reporte_no_pago <- Reporte_10 %>%
  filter(
    `VALOR CUOTA MES HOY` > 3000
  )

Reporte_no_pago <- Reporte_no_pago %>% 
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"),
         !is.na(`TELÉFONO CELULAR`) & `TELÉFONO CELULAR` != "")

# Seleccionar solo las columnas requeridas
Preventiva <- Reporte_no_pago %>% select(`CEDULA ASOCIADO`, `NOMBRE ASOCIADO`, `TELÉFONO CELULAR`)

Preventiva <- Preventiva %>%
  mutate(`NOMBRE ASOCIADO` = sub(" .*", "", `NOMBRE ASOCIADO`))

Preventiva <- Preventiva %>% clean_names(case="all_caps")

## Escribir archivo final ## 

nombre_archivo <- paste0("Preventiva_", Sys.Date(),".xlsx")

ruta_archivo <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/16. Envío comunicaciones/Bases Comunicaciones/Cargado"

write_xlsx(Preventiva, path = file.path(ruta_archivo, nombre_archivo))
