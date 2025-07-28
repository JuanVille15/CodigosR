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

Base_Inicial <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/BD SORTEO ESTAR ALDIA TE PREMIA/Base Pago Oportuno 202506 - Definitiva.xlsx", sheet = "BASE SORTEO")

## PAGO - OPORTUNO ##

Base_Pago_Oportuno <- Base_Inicial %>% filter(PagoOportuno == "Si")

## DEBITO AUTOMATICO ##

Base_Debito <- Base_Pago_Oportuno %>% filter(PagoDebitoAutomatico == "Si")

write_xlsx(Base_Debito, "//coomeva.nal/dfscoomeva/Mercadeo_Corporativo/CAMPAÑA SORTEOS 2025/COOMEVA/02. JUNIO/01. DÉBITO AUTOMÁTICO/Coomeva-Debito.xlsx")
