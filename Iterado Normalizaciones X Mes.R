library(dplyr)
library(readxl)
library(writexl)
library(janitor)

# Definir la ruta de los archivos
ruta_archivos <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Solicitudes a demanda/R-10s/"

# Lista de archivos y sus períodos
archivos <- list(
  "R10_Sep_2023.xlsx" = "Septiembre 2023",
  "R10_Oct_2023.xlsx" = "Octubre 2023",
  "R10_Nov_2023.xlsx" = "Noviembre 2023",
  "R10_Dic_2023.xlsx" = "Diciembre 2023"
)

# Lista para almacenar los datos procesados
datos_consolidados <- list()

# Iterar sobre los archivos
for (archivo in names(archivos)) {
  # Leer el archivo
  data <- read_xlsx(file.path(ruta_archivos, archivo))
  
  # Filtrar y procesar los datos
  data <- data %>%
    filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado")) %>%
    filter(`DESCRIPCIÓN DEL ESTADO MULTIAC` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo", "Receso")) %>%
    filter(`EDAD CARTERA INICIAL` %in% "3") %>%
    select(all_of(c("CEDULA ASOCIADO", "NOMBRE ASOCIADO", "REGIONAL", "ZONA", 
                    "EDAD CARTERA INICIAL", "EDAD CARTERA HOY"))) %>%
    mutate(
      ESTADO_CARTERA = case_when(
        `EDAD CARTERA HOY` == 0 ~ "NORMALIZA",
        `EDAD CARTERA HOY` < `EDAD CARTERA INICIAL` ~ "MEJORA",
        `EDAD CARTERA HOY` == `EDAD CARTERA INICIAL` ~ "MANTIENE",
        `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
        TRUE ~ NA_character_
      ),
      PERIODO = archivos[[archivo]] # Agregar columna del período
    )
  
  # Agregar los datos procesados a la lista
  datos_consolidados[[archivo]] <- data
}

# Consolidar todos los datos en un solo data frame
datos_consolidados <- bind_rows(datos_consolidados)

# Guardar el archivo consolidado
write_xlsx(datos_consolidados, file.path(ruta_archivos, "R10_Consolidado.xlsx"))

