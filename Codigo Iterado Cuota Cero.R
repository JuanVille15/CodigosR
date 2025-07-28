library(dplyr)
library(readxl)
library(purrr)
library(openxlsx)  # Para exportar a Excel

# Ruta base donde se encuentran los archivos
ruta_base <- "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/2024"

# Crear un vector con los nombres de los meses en orden
meses <- c("Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", 
           "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre")

# Lista vacía para guardar los resultados procesados de cada mes
resultados_mensuales <- list()

# Iterar sobre cada mes
for (mes in meses) {
  # Rutas de los archivos para el mes actual
  archivo_inicio <- paste0(ruta_base, "/Reporte10_Inicio_", mes, ".xlsx")
  archivo_cierre <- paste0(ruta_base, "/Reporte10_Cierre_", mes, ".xlsx")
  
  # Leer los archivos de inicio y cierre
  Reporte_inicio <- read_excel(archivo_inicio)
  Reporte_cierre <- read_excel(archivo_cierre, sheet = "Hoja1")
  
  # Asegurar que las columnas de cédulas sean de tipo character
  Reporte_inicio <- Reporte_inicio %>% mutate(`CEDULA ASOCIADO` = as.character(`CEDULA ASOCIADO`))
  Reporte_cierre <- Reporte_cierre %>% mutate(`CEDULA ASOCIADO` = as.character(`CEDULA ASOCIADO`))
  
  # Igualar los nombres de las columnas de Reporte_cierre a los de Reporte_inicio
  colnames(Reporte_cierre) <- colnames(Reporte_inicio)
  
  # Procesar los datos
  Reporte_inicio <- Reporte_inicio %>% 
    filter(SUBCAMPAÑA %in% "CSSCUOTA0HASTA6") %>% 
    select(`CEDULA ASOCIADO`, REGIONAL, `EDAD CARTERA INICIAL`, `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`) %>%
    left_join(Reporte_cierre %>% select(`CEDULA ASOCIADO`, `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY`), 
              by = "CEDULA ASOCIADO") %>%
    mutate(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.y` = if_else(is.na(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.y`), 
                                                                "Retiro Administrativo BUC", 
                                                                `DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.y`)) %>%
    mutate(Estado_contencion = if_else(`DESCRIPCIÓN DEL ESTADO MULTIACTIVA HOY.y` != "Retiro Administrativo BUC", 
                                       "Retenido", 
                                       "Retirado")) %>%
    select(`CEDULA ASOCIADO`, REGIONAL, `EDAD CARTERA INICIAL`, Estado_contencion) %>%
    mutate(Mes = mes)  # Agregar la columna del mes
  
  # Guardar el resultado del mes en la lista
  resultados_mensuales[[mes]] <- Reporte_inicio
}

# Combinar los resultados de todos los meses en un solo data frame
consolidado_anual <- bind_rows(resultados_mensuales)

write.xlsx(consolidado_anual,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/R10consolidado/Seguimiento_cuota0_2024.xlsx")
