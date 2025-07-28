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

conflicts_prefer(dplyr::filter)


data <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/CENTRO DE CONTACTO/R10 Cierre Mensual/2025/5. Mayo/R10/R10-16-05-2025.xlsx")

Mora4 <- data %>% filter(SUBCAMPAÑA == "CSS MORA 4" & PRIORIDAD_GESTION == "Prioridad 1" & `Contacto Directo` == 0)

Mora3 <- data%>% filter(SUBCAMPAÑA == "CSS MORA 3" & PRIORIDAD_GESTION == "Prioridad 1" & `Contacto Directo` == 0)

## Filtramos por Estados ## MORA 4

Mora4 <- Mora4 %>% filter(ESTADO_CARTERA %in% c("DETERIORA","POR VENCER")) 


Mora4Cali <- Mora4 %>% filter(REGIONAL == "Cali", ZONA == "Cali")                  


Mora4Cali <- Mora4Cali %>% arrange(desc(`VALOR VENCIDO HOY`)) %>% 
                              slice_head(n = 50)

Mora4Caribe <- Mora4 %>% filter(REGIONAL == "Caribe", ZONA == "Barranquilla")                  


Mora4Caribe <- Mora4Caribe %>% arrange(desc(`VALOR VENCIDO HOY`)) %>% 
  slice_head(n = 50)

## Filtramos por Estados ## MORA 3

Mora3 <- Mora3 %>% filter(ESTADO_CARTERA %in% c("DETERIORA","POR VENCER"))

Mora3Cali <- Mora3 %>% filter(REGIONAL == "Cali", ZONA == "Cali") 

Mora3Cali <- Mora3Cali %>% arrange(desc(`VALOR VENCIDO HOY`)) %>% 
  slice_head(n = 50)

Mora3Caribe <- Mora3 %>% filter(REGIONAL == "Caribe", ZONA == "Barranquilla") 

Mora3Caribe <- Mora3Caribe %>% arrange(desc(`VALOR VENCIDO HOY`)) %>% 
  slice_head(n = 50)


### Crear Bases ##

BaseCali <- bind_rows(Mora3Cali,Mora4Cali)

BaseCaribe <- bind_rows(Mora3Caribe,Mora4Caribe)


## Escribimos las bases ##

write_xlsx(BaseCali,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/BaseCali_Mayo_2025.xlsx")

write_xlsx(BaseCaribe,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/BaseCaribe_Mayo_2025.xlsx")
