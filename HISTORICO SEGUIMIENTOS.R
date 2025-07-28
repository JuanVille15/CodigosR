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

HISTORICO <-read.csv("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Insumo_Tablero/insumo.csv")

Febrero_2025 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/2025/2. Febrero/R10-CIERRE-FEBRERO-2025.xlsx")


Febrero_2025 <- Febrero_2025 %>% mutate(Periodo = "Feb.2025") 

Febrero_2025 <- Febrero_2025 %>% select(Periodo,everything())

columnas <- c("Periodo",
              "CEDULA ASOCIADO",
              "NOMBRE ASOCIADO",
              "REGIONAL",
              "DESCRIPCIÓN DEL CORTE",
              "DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY",
              "EDAD CARTERA INICIAL",
              "EDAD CARTERA HOY",
              "VALOR RECAUDO VENCIDO",
              "VALOR VENCIDO PERIODO",
              "SUBCAMPAÑA",
              "VALOR VENCIDO HOY",
              "ESTADO_CARTERA",
              "PRIORIDAD_MONTO",
              "PRIORIDAD_RIESGO",
              "PRIORIDAD_GESTION",
              "fecha_ultima_gestion",
              "fecha_promesa",
              "Estado_compromiso",
              "Acción",
              "Respuesta",
              "motivo_nopago",
              "Contacto",
              "Contacto Directo",
              "Efectivo",
              "Qgestiones",
              "Promesa_Cumplida",
              "EDAD_CARTERA_HOMOLOGADA",
              "Gestion_Reciente",
              "Saldo_menor",
              "gestion_base",
              "ACTOR_GESTION_HOMOLOGADO"
)

Febrero_2025 <- Febrero_2025 %>% mutate(ACTOR_GESTION_HOMOLOGADO = case_when(SUBCAMPAÑA %in% c("CSSMORA1CORTE5","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20",
                                                                               "CSSMORA1CORTE25","CSSMORA1CORTE30","CSS MORA 2","CSS MORA 3","CSS MORA 4",
                                                                               "CSSCUOTA0HASTA6","REG BOGOTA","REG CALI","REG MEDELLIN","REG EJECAFETERO","REG PALMIRA","REG CARIBE", "CSS MORA 5","CSS MORA 6","CSS MORA 7") ~ "Moras Tempranas",
                                                             SUBCAMPAÑA == "BANCOOMEVA_U" ~ "Bancoomeva",
                                                             SUBCAMPAÑA %in% c("CARTERA CASTIGO","MORA >= 4 CR","MORA 1 CR","MORA 2 - 3 CR") ~ "Creditos",
                                                             SUBCAMPAÑA %in% c("CSSALTAPRIORID", "CSSBAJAPRIORID") ~ "Inactivos",
                                                             TRUE ~ SUBCAMPAÑA ))

Febrero_2025 <- Febrero_2025 %>% select(columnas)

HISTORICO <- HISTORICO %>% select(-X)

colnames(HISTORICO) <- colnames(Febrero_2025)

Febrero_2025 <- Febrero_2025 %>%
  mutate(across(names(HISTORICO), ~ as(., class(HISTORICO[[cur_column()]]))))

HISTORICO <- HISTORICO %>% bind_rows(Febrero_2025)

HISTORICO <- HISTORICO %>%
  filter(!`CEDULA ASOCIADO` %in% c("800208092", "805000427", "805030765", 
                                   "811016426", "816001182", "900015339"))

write.csv(HISTORICO,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Insumo_Tablero/insumo.csv")
