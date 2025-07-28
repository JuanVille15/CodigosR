#### CRUCE GESTIONES MP ###

### LIBRERIAS NECESARIAS ####

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
library(stringi)
library(readr)


gestiones1 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Mayo/202505-1.xlsx")

gestiones2 <-read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Mayo/202505-2.xlsx")

Base_gestiones_Bancoomeva <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Archivos de Alejandro Ocampo Losada - Bases de gestion asociados-banco/5. Mayo/GAB20250531_v2.xlsx")

gestiones1 <- gestiones1 %>% distinct()

gestiones2 <- gestiones2 %>% distinct()

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>%
  distinct()

## Modificar base Bancoomeva ##

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(fecha_gestion1 = fecha_gestion)

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% select(identificacion_deudor,fecha_gestion1,everything())

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(fecha_gestion = as.Date(fecha_gestion))

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(fecha_gestion1 = as.Date(fecha_gestion1))

Base_gestiones_Bancoomeva$fecha_gestion1 <- format(Base_gestiones_Bancoomeva$fecha_gestion1, "%d/%m/%Y")

Base_gestiones_Bancoomeva$fecha_gestion <- format(Base_gestiones_Bancoomeva$fecha_gestion, "%d/%m/%Y")

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(casa_cobranza = "BANCOOMEVA_U")

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% select(1:11)

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(identificacion_deudor = as.character(identificacion_deudor))

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(fecha_prox_accion = as.character(fecha_prox_accion))

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(fecha_promesa = as.character(fecha_promesa))

colnames(Base_gestiones_Bancoomeva) <- colnames(gestiones2)

## Agregamos las gestiones ##

Gestiones_Mensuales <- gestiones1 %>% bind_rows(gestiones2)

Gestiones_Mensuales <- Gestiones_Mensuales %>% mutate(Entidad = "Coomeva")

Base_gestiones_Bancoomeva <- Base_gestiones_Bancoomeva %>% mutate(Entidad = "Bancoomeva")

Gestiones_Mensuales <- Gestiones_Mensuales %>% bind_rows(Base_gestiones_Bancoomeva)

## Leemos archivo A cruzar ##

Cedulas_MP <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/MP/Asignación/2025/BASE ASOCIADO MAYO.xlsx")

Cedulas_MP <- Cedulas_MP %>% distinct(NIT)

Cedulas_MP <- Cedulas_MP %>% mutate(NIT = as.character(NIT))

Cruce <- Cedulas_MP %>% left_join(Gestiones_Mensuales %>% select(identificacion_deudor,fecha_gestion,Acción,Respuesta), by = c("NIT" = "identificacion_deudor"))

Cruce <- Cruce %>% arrange(NIT)

Cruce <- Cruce %>% filter(fecha_gestion != "")

Cruce <- Cruce %>% rename(CEDULA_ASOCIADO = NIT,
                          FECHA_GESTION = fecha_gestion,
                          ACCION = Acción,
                          RESPUESTA = Respuesta)

write_xlsx(Cruce, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/MP/Asignación/2025/Cruce/Gestiones_MP_Mayo.xlsx")
