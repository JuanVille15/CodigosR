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
library(purrr)
library(stringr)

## Leemos los reporte 10 con el periodo ##

files <- list.files(path = "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Resultado", full.names = TRUE)

data <- map_dfr(files, function(file){
  read_excel(file, col_types = "text") %>% 
    mutate(periodo = substr(basename(file),1, 6))
})

data <- data %>% filter(ACTOR_GESTION_HOMOLOGADO == "Moras Tempranas")

data <- data %>% select(periodo,`CEDULA ASOCIADO`,`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,SEGMENTO,ESTADO_CARTERA)


data <- data %>%
  mutate(SEGMENTO = if_else(is.na(SEGMENTO), "en_blanco", SEGMENTO))

Indicadores <- data %>% group_by(periodo,SEGMENTO) %>% 
                     summarise(
                       nSegmento = n_distinct(`CEDULA ASOCIADO`),
                       ContSegmento = n_distinct(`CEDULA ASOCIADO`[ESTADO_CARTERA %in% c("NORMALIZA","MEJORA","MANTIENE")]),
                       .groups = "drop")

Indicadores <- Indicadores %>% group_by(periodo) %>% 
                    mutate(nperiodo = sum(nSegmento),
                           indice_contencion = ContSegmento / nSegmento,
                           participacion = nSegmento / nperiodo) %>% 
                                ungroup()

Indicadores <- Indicadores %>% select(periodo,SEGMENTO,indice_contencion,participacion)

Indicadores <- Indicadores %>% filter(periodo >= 202405)

Indicadores <- Indicadores %>% filter(periodo != 202410)

write_xlsx(Indicadores, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Prioridad/Historico_Contenciones.xlsx")

## Generamos Prioridades ##

Prioridad <- Indicadores %>% group_by(SEGMENTO) %>% 
                      summarise(AvgCont = mean(indice_contencion),
                                AvgPart = mean(participacion))

## Definimos media y desviación estandar ##

mediaCont <- mean(Prioridad$AvgCont)

SdCont <- sd(Prioridad$AvgCont)

mediaPart <- mean(Prioridad$AvgPart)

SdPart <- sd(Prioridad$AvgPart)

Prioridad <- Prioridad %>% mutate(Rank_Contención = ntile(AvgCont,3), ## Calculo Porcentiles ## 3 -- Mayor Contención
                                  Rank_Participacion = ntile(AvgPart, 3)) ## Calculo Porcentiles ## 3 -- Mayor Participación

Prioridad <- Prioridad %>% mutate(
  Prioridad_Gestión = case_when( Rank_Contención == 1 & Rank_Participacion == 3 ~ 1,
                                 Rank_Contención == 1 & Rank_Participacion == 2 ~ 1,
                                 Rank_Contención == 1 & Rank_Participacion == 1 ~ 1,
                                 Rank_Contención == 2 & Rank_Participacion == 3 ~ 1,
                                 Rank_Contención == 2 & Rank_Participacion == 2 ~ 2,
                                 Rank_Contención == 2 & Rank_Participacion == 1 ~ 2,
                                 TRUE ~ 3))
 

write_xlsx(Prioridad, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Prioridad/Prioridad_Julio.xlsx")
