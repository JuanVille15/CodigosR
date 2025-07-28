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

## CREACIÓN BASE SORTEO - PAGO OPORTUNO Y DEBITO AUTOMATICO##

files <- list.files(path = "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bases Sorteos", full.names = TRUE)

Data <- map_dfr(files,function(file){
  read_excel(file, sheet = "BASE SORTEO")
})

## PAGO - OPORTUNO ##

Base_Pago_Oportuno <- Data %>% filter(PagoOportuno == "Si")

write_xlsx(
  list("Pago Oportuno" = Base_Pago_Oportuno),
  path = "//coomeva.nal/dfscoomeva/Mercadeo_Corporativo/CAMPAÑA SORTEOS 2025/COOMEVA/02. JUNIO/03. PAGO OPORTUNO/Coomeva-PagoOportuno.xlsx"
)

## DEBITO AUTOMATICO ##

Base_Debito <- Base_Pago_Oportuno %>% filter(PagoDebitoAutomatico == "Si")

write_xlsx(list("Base Sorteo Debito Automatico" = Base_Debito),
                path = "//coomeva.nal/dfscoomeva/Mercadeo_Corporativo/CAMPAÑA SORTEOS 2025/COOMEVA/01. MAYO/01. DÉBITO AUTOMÁTICO/Coomeva-Debito.xlsx")
