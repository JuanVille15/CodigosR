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

gestiones1 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Febrero/Febrero 01-15.xlsx")
gestiones2 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Febrero/Febrero 15-28.xlsx")

gestiones2<- gestiones2 %>% distinct()

gestiones1<- gestiones1 %>% distinct()

gestiones_mensuales <- gestiones1 %>% bind_rows(gestiones2)

Gestiones_Bancoomeva <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Febrero/Gestiones_Bancoomeva_Febrero.xlsx")

Gestiones_Bancoomeva <- Gestiones_Bancoomeva %>% distinct()

## Pegar Gestiones ##

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

colnames(Gestiones_Bancoomeva) <- colnames(gestiones_mensuales)

gestiones_mensuales <- gestiones_mensuales %>% bind_rows(Gestiones_Bancoomeva)

write_csv(gestiones_mensuales,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/MP/Facturacion/Gestiones_Mensuales.csv")
          
          