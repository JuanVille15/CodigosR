### BONO SEMESTRAL ###

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


## R10 - Noviembre ##
R10Nov2024 <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/R10_Nov_2023.xlsx")

R10Nov2024 <- R10Nov2024 %>%
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))

R10Nov2024 <- R10Nov2024 %>%
  filter(`DESCRIPCIÓN DEL ESTADO MULTIAC` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo","Receso"))


Subcampaña <- c("CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                "CSSMORA1CORTE30", "CSSMORA1CORTE5")

R10Nov2024 <- R10Nov2024 %>%
  filter(`SUBCAMPAÑA` %in% Subcampaña )


columnas_deseadas <- c("CEDULA ASOCIADO", "NOMBRE ASOCIADO", "REGIONAL", "ZONA", 
                       "TIPO CLIENTE", "DESCRIPCIÓN DEL CORTE", "DESCRIPCIÓN DEL ESTADO MULTIAC", 
                       "EDAD CARTERA INICIAL", "EDAD CARTERA HOY", "VALOR RECAUDO VENCIDO", 
                       "VALOR VENCIDO PERIODO", "SUBCAMPAÑA")

R10Nov2024 <- R10Nov2024 %>% select(all_of(columnas_deseadas))



## Crear la columna ESTADO CARTERA ##

R10Nov2024 <- R10Nov2024 %>%
  mutate(ESTADO_CARTERA = case_when(
    `EDAD CARTERA HOY` == 0 ~ "NORMALIZA",
    `EDAD CARTERA HOY` < `EDAD CARTERA INICIAL` ~ "MEJORA",
    `EDAD CARTERA HOY` == `EDAD CARTERA INICIAL` ~ "MANTIENE",
    `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
    TRUE ~ NA_character_
  ))


## Filtrar estados de contención ##

R10Nov2024 <- R10Nov2024 %>%
  filter(ESTADO_CARTERA %in% c("NORMALIZA", "MEJORA", "MANTIENE"))


## R10 - Abril## 

R10Abril2024 <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/R10_Abr_2024.xlsx",sheet = "Exportar Hoja de Trabajo")


## Cruce de cedulas ##

R10Nov2024 <- R10Nov2024 %>% left_join(R10Abril2024 %>% select(`CEDULA ASOCIADO`,`EDAD CARTERA HOY`),by="CEDULA ASOCIADO")  

R10Nov2024 <- R10Nov2024 %>%
  rename(Edad_comparacion = `EDAD CARTERA HOY.y`)

## Resultado ## 

R10Nov2024 <- R10Nov2024 %>%
  mutate(Resultado = if_else(Edad_comparacion <= `EDAD CARTERA HOY.x`, 1, 0))


write_xlsx(R10Nov2024,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/Resultado/Nov23-Abr_24.xlsx")

