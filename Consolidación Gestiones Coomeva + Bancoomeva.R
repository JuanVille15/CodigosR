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

Gestiones_Coomeva <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Enero/Gestiones_202501 1.xlsx")

Gestiones_Bancoomeva <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Enero/Gestiones_Bancoomeva_Enero.xlsx")


Gestiones_Coomeva <- Gestiones_Coomeva %>% distinct()

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% distinct()

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(fecha_gestion1 = fecha_gestion)

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% select(identificacion_deudor, fecha_gestion1, everything())

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(fecha_gestion = as.Date(fecha_gestion))

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(fecha_gestion1 = as.Date(fecha_gestion1))

Gestiones_Bancoomeva$fecha_gestion1 <- format(Gestiones_Bancoomeva$fecha_gestion1, "%d/%m/%Y")

Gestiones_Bancoomeva$fecha_gestion <- format(Gestiones_Bancoomeva$fecha_gestion, "%d/%m/%Y")

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(casa_cobranza = "BANCOOMEVA_U")

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% select(1:11)

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(identificacion_deudor = as.character(identificacion_deudor))

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(fecha_prox_accion = as.character(fecha_prox_accion))

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% mutate(fecha_promesa = as.character(fecha_promesa))

colnames(Gestiones_Bancoomeva) <- colnames(Gestiones_Coomeva)

Gestiones_Coomeva <- Gestiones_Coomeva %>% bind_rows(Gestiones_Bancoomeva)

write.csv(Gestiones_Coomeva,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Enero/Gestiones_Enero_Con_Bancoomeva.csv")
