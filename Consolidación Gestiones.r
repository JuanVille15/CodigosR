library(readxl)
library(dplyr)
library(DBI)
library(odbc)
library(writexl)
library(glue)
library(janitor)
library(tidyr)
library(lubridate)
library(conflicted)
library(dbplyr)
library(readr)
library(Microsoft365R)
library(blastula)


g1 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506-1.xlsx")

g2 <- read_excel("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506-2.xlsx")

## Consolidamos las gestiones ##

gestiones <- rbind(g1,g2)

gestiones <- gestiones %>% distinct()

## Escribimos Archivo Resultante ##

write_csv(gestiones,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/WSAC/Gestiones/Junio/202506.csv")
