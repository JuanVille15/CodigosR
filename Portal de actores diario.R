library(readxl)
library(dplyr)
library(writexl)

##conflicts_prefer(dplyr::filter)

ruta <- "//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/13. Archivos de gestión/Reporte 10 nuevo"
archivos_con_fechas <- file.info(list.files(path = ruta, full.names = TRUE))
archivos_excel <- archivos_con_fechas[grep("\\.xlsx$", rownames(archivos_con_fechas), ignore.case = TRUE), ]
ultimo_archivo <- rownames(archivos_excel)[which.max(archivos_excel$mtime)]
data <- read_excel(ultimo_archivo)

data <- data %>%
  filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado"))

data <- data %>%
  filter(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo","Receso"))


Subcampaña <- c("BANCOOMEVA_U", "CSS MORA 2", "CSS MORA 3","CSS MORA 4","CSS MORA 5","CSS MORA 6",
                "CSS MORA 7","CSSMORA1CORTE10","CSSMORA1CORTE15","CSSMORA1CORTE20", "CSSMORA1CORTE25",
                "CSSMORA1CORTE30", "CSSMORA1CORTE5", "REG BOGOTA", "REG CALI", "REG CARIBE", "REG EJECAFETERO",
                "REG MEDELLIN", "REG PALMIRA"
                   )

data <- data %>%
  filter(`SUBCAMPAÑA` %in% Subcampaña )

## Seleccionar Columnas ##

Final <- data %>% select(
  REGIONAL, ZONA, `NOMBRE OFICINA`, `CEDULA ASOCIADO`, `NOMBRE ASOCIADO`, `TIPO CARTERA`,
  `EDAD CARTERA INICIAL`, `EDAD CARTERA HOY`, `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`,
  `FECHA ÚLTIMO RECAUDO`, `VALOR RECAUDO VENCIDO`, `VALOR VENCIDO PERIODO`, `% RECAUDO`,
  `DESCRIPCIÓN DEL CORTE`, `ACTOR DE GESTIÓN HOY`, `USUARIO ASIGNADO`, SUBCAMPAÑA
)


## Escribirlo ##

fecha_actual <- format(Sys.Date(), "%Y-%m-%d")
nombre_archivo <- paste0("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Portal de Actores/Resultado/R10_Filtrado",fecha_actual,".xlsx")
write_xlsx(Final, nombre_archivo)
