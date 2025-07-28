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


## Cargamos el Mlcarcre ##

mlcarcre <- read_excel("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Resultado/MLCARCRE_COMPLETO.xlsx")


## Validación Duplicados ##

mlcarcre <- mlcarcre %>% arrange(NITCLI)

mlcarcre <- mlcarcre %>% mutate(Key = paste0(NITCLI,PEREVA))

mlcarcre <- mlcarcre %>%  mutate (Vali = if_else(Key == lead(Key),0,1))

## creamos un vector con la ruta de los archivos de gestiones ##

files <- list.files(path = "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024/Gestiones", full.names = TRUE)

## Leemos y creamos la columna Periodo##
gestiones <- map_dfr(files, function(file){
  read_excel(file) %>% 
    mutate(periodo = substr(basename(file),1, 6))
})

gestiones <- gestiones %>% mutate(periodo = as.character(periodo))

gestiones <- gestiones %>% mutate(Key = paste0(identificacion_deudor,periodo))

gestiones$periodo <- str_trim(gestiones$periodo)

gestiones$Key <- str_trim(gestiones$Key)

## Qgestiones ##

gestiones <- gestiones %>%
  group_by(Key) %>%
  mutate(Qgestiones = n()) %>%
  ungroup()


## Homologacion Respuestas ##

gestiones <- gestiones %>% distinct()

Homologacion <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Corrección R-10/Insumos/Homologacion_respuestas.xlsx")

gestiones <- gestiones %>% left_join(Homologacion %>% select(RESPUESTA,Contacto,`Contacto Directo`,Efectivo,Prioridad), by = c("Respuesta" = "RESPUESTA"))


## Dejamos la de menor prioridad dentro de cada key (CCPeriodo)

Base_gestiones_reducida <- gestiones %>%
group_by(Key) %>%
slice_min(Prioridad, with_ties = FALSE) %>%  # Toma la fila con menor prioridad
ungroup()

Base_gestiones_reducida <- Base_gestiones_reducida %>% arrange(Key)

mlcarcre <- mlcarcre %>% left_join(Base_gestiones_reducida %>% select(Key, Qgestiones, Contacto, `Contacto Directo`, Efectivo), by = "Key")

mlcarcre <- mlcarcre %>% filter(PEREVA != 202505)

mlcarcre <- mlcarcre %>%
  mutate(
    Qgestiones = if_else(Vali == 0, 0, Qgestiones),
    Contacto = if_else(Vali == 0, 0, Contacto),
    `Contacto Directo` = if_else(Vali == 0, 0, `Contacto Directo`),
    Efectivo = if_else(Vali == 0, 0, Efectivo)
  )

## Creamos Columna Estado de Cartera ##

mlcarcre <- mlcarcre %>% mutate(EDAD_DE_MORA_INI = if_else(is.na(EDAD_DE_MORA_INI), 0, EDAD_DE_MORA_INI),
                                EDAD_DE_MORA_FIN = if_else(is.na(EDAD_DE_MORA_FIN), 0, EDAD_DE_MORA_FIN))

mlcarcre <- mlcarcre %>% group_by(Key) %>% 
                         mutate(EDAD_DE_MORA_INI = max(EDAD_DE_MORA_INI),
                                EDAD_DE_MORA_FIN = max(EDAD_DE_MORA_FIN)) %>% 
                                          ungroup()

mlcarcre <- mlcarcre %>% mutate(ESTADO_CARTERA = case_when(EDAD_DE_MORA_FIN == 0 ~ "NORMALIZA",
                                                           EDAD_DE_MORA_FIN < EDAD_DE_MORA_INI ~ "MEJORA",
                                                           EDAD_DE_MORA_FIN == EDAD_DE_MORA_INI ~ "MANTIENE",
                                                           EDAD_DE_MORA_FIN > EDAD_DE_MORA_INI ~ "DETERIORA",
                                                           TRUE ~ NA_character_)) 


write_xlsx(mlcarcre, "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/2. Crediticia/1.Archivos detalle de créditos/Consol Mlcarcre/Resultado/MLCARCRE_COMPLETO_JJVT.xlsx")
