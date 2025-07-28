library(dplyr)
library(readxl)
library(writexl)

# Define los pares de meses para iterar
periodos <- list(
  list(input = "R10_Oct_2023.xlsx", output = "R10_Mar_2024.xlsx", resultado = "Oct23-Mar_24.xlsx"),
  list(input = "R10_Nov_2023.xlsx", output = "R10_Abr_2024.xlsx", resultado = "Nov23-Abr_24.xlsx"),
  list(input = "R10_Dic_2023.xlsx", output = "R10_May_2024.xlsx", resultado = "Dic23-May_24.xlsx"),
  list(input = "R10_Ene_2024.xlsx", output = "R10_Jun_2024.xlsx", resultado = "Ene24-Jun_24.xlsx"),
  list(input = "R10_Feb_2024.xlsx", output = "R10_Jul_2024.xlsx", resultado = "Feb24-Jul_24.xlsx"),
  list(input = "R10_Mar_2024.xlsx", output = "R10_Ago_2024.xlsx", resultado = "Mar24-Ago_24.xlsx")
)

# Define las subcampañas y las columnas deseadas
Subcampaña <- c("CSS MORA 2", "CSS MORA 3", "CSS MORA 4", "CSS MORA 5", "CSS MORA 6",
                "CSS MORA 7", "CSSMORA1CORTE10", "CSSMORA1CORTE15", "CSSMORA1CORTE20", 
                "CSSMORA1CORTE25", "CSSMORA1CORTE30", "CSSMORA1CORTE5")

columnas_deseadas <- c("CEDULA ASOCIADO", "NOMBRE ASOCIADO", "REGIONAL", "ZONA", 
                       "TIPO CLIENTE", "DESCRIPCIÓN DEL CORTE", "DESCRIPCIÓN DEL ESTADO MULTIAC", 
                       "EDAD CARTERA INICIAL","USUARIO ASIGNADO", "EDAD CARTERA HOY", "VALOR RECAUDO VENCIDO", 
                       "VALOR VENCIDO PERIODO", "SUBCAMPAÑA")

# Iterar sobre los periodos
for (periodo in periodos) {
  # Leer los archivos de entrada
  R10Inicial <- read_xlsx(paste0("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/", periodo$input),
                          sheet = "Exportar Hoja de Trabajo")
  R10Final <- read_xlsx(paste0("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/", periodo$output), 
                        sheet = "Exportar Hoja de Trabajo")
  
  # Asegurar que CEDULA ASOCIADO tenga el mismo tipo
  R10Inicial <- R10Inicial %>%
    mutate(`CEDULA ASOCIADO` = as.character(`CEDULA ASOCIADO`))
  R10Final <- R10Final %>%
    mutate(`CEDULA ASOCIADO` = as.character(`CEDULA ASOCIADO`))
  
  # Filtrar y procesar datos iniciales
  R10Inicial <- R10Inicial %>%
    filter(`TIPO CLIENTE` %in% c("Mixto (Asociado/Cliente)", "Solo Asociado")) %>%
    filter(`DESCRIPCIÓN DEL ESTADO MULTIAC` %in% c("Activo Normal", "Activo Cobranza Interna", "Inactivo", "Receso")) %>%
    filter(`SUBCAMPAÑA` %in% Subcampaña) %>%
    select(all_of(columnas_deseadas)) %>%
    mutate(ESTADO_CARTERA = case_when(
      `EDAD CARTERA HOY` == 0 ~ "NORMALIZA",
      `EDAD CARTERA HOY` < `EDAD CARTERA INICIAL` ~ "MEJORA",
      `EDAD CARTERA HOY` == `EDAD CARTERA INICIAL` ~ "MANTIENE",
      `EDAD CARTERA HOY` > `EDAD CARTERA INICIAL` ~ "DETERIORA",
      TRUE ~ NA_character_
    )) %>%
    filter(ESTADO_CARTERA %in% c("NORMALIZA", "MEJORA", "MANTIENE"))
  
  # Cruzar con el archivo final
  R10Resultado <- R10Inicial %>%
    left_join(R10Final %>% select(`CEDULA ASOCIADO`, `EDAD CARTERA HOY`), by = "CEDULA ASOCIADO") %>%
    rename(Edad_comparacion = `EDAD CARTERA HOY.y`) %>%
    mutate(Resultado = if_else(Edad_comparacion <= `EDAD CARTERA HOY.x`, 1, 0))
  
  # Guardar el resultado
  write_xlsx(R10Resultado, paste0("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/Resultado/", periodo$resultado))
}


## Consolidar todo en un solo archivo ##

ruta_resultados <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Bono Semestral/Resultado/"

# Lista de archivos a consolidar
archivos <- list.files(path = ruta_resultados, pattern = "\\.xlsx$", full.names = TRUE)

# Leer y consolidar los datos con columna "periodo_analisis"
consolidado <- lapply(archivos, function(archivo) {
  datos <- read_xlsx(archivo)
  datos <- datos %>%
    mutate(periodo_analisis = basename(archivo)) # Agregar la columna con el nombre del archivo
  return(datos)
}) %>% 
  bind_rows()

# Mover la columna "periodo_analisis" al inicio
consolidado <- consolidado %>%
  select(periodo_analisis, everything())

# Guardar el archivo consolidado
write_xlsx(consolidado, paste0(ruta_resultados, "Resultado_6meses.xlsx"))

cat("Archivo consolidado creado: Resultado_6meses.xlsx")

