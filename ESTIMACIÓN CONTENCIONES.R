library(readxl)
library(dplyr)
library(writexl)
library(forecast)
library(tidyverse)
library(lubridate)
library(plm)
library(forecast)
library(janitor)

Insumo <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Recaudo/Insumo_Recaudo_Regionales.xlsx") %>% 
  clean_names(case = "all_caps")

# Agrupar por PERIODO, EDAD_INICIAL y ACTOR
Insumo <- Insumo %>%
  group_by(PERIODO, EDAD_INICIAL, ACTOR) %>%
  summarise(
    TOTAL_RECAUDADO = sum(TOTAL_RECAUDADO, na.rm = TRUE),
    TOTAL_VENCIDO = sum(TOTAL_VENCIDO, na.rm = TRUE),
    PORCENTAJE_RECAUDO = sum(TOTAL_RECAUDADO, na.rm = TRUE) / sum(TOTAL_VENCIDO, na.rm = TRUE) * 100
  ) %>%
  ungroup()

# Convertir PERIODO a tipo fecha y extraer Año y Mes
Insumo <- Insumo %>% 
  mutate(PERIODO = as.Date(PERIODO),
         Año = year(PERIODO),
         Mes = month(PERIODO))

# Ajustar modelos ARIMA para cada grupo de EDAD_INICIAL y ACTOR
pred_asignados_list <- list()
pred_contenidos_list <- list()

for (actor in unique(Insumo$ACTOR)) {
  # Filtrar los datos por actor
  datos_actor <- subset(Insumo, ACTOR == actor)
  
  for (grupo in unique(datos_actor$EDAD_INICIAL)) {
    # Filtrar los datos por grupo de edad dentro del actor
    datos_grupo <- subset(datos_actor, EDAD_INICIAL == grupo)
    
    # Convertir las series temporales
    ts_asignados <- ts(datos_grupo$TOTAL_VENCIDO, frequency = 12)
    ts_contenidos <- ts(datos_grupo$TOTAL_RECAUDADO, frequency = 12)
    
    # Ajustar modelos ARIMA
    modelo_asignados <- tslm(ts_asignados ~ season + trend)
    modelo_contenidos <- tslm(ts_contenidos ~ season + trend)
    
    # Hacer predicciones para los próximos 14 periodos
    pred_asignados <- forecast(modelo_asignados, h = 12)
    pred_contenidos <- forecast(modelo_contenidos, h = 12)
    
    # Almacenar las predicciones
    pred_asignados_list[[paste(actor, grupo, sep = "_")]] <- pred_asignados$mean
    pred_contenidos_list[[paste(actor, grupo, sep = "_")]] <- pred_contenidos$mean
  }
}

# Unir todas las predicciones de asignados y contenidos
total_asignados_futuros <- unlist(pred_asignados_list)
total_contenidos_futuros <- unlist(pred_contenidos_list)

# Crear el dataframe con las predicciones
nuevos_datos <- data.frame(
  TOTAL_ASIGNADO = total_asignados_futuros,
  TOTAL_NORMALIZACIONES = total_contenidos_futuros,
  ACTOR = rep(names(pred_asignados_list), each = 12),  # Replicar cada actor para cada grupo de edad
  EDAD_INICIAL = rep(unique(Insumo$EDAD_INICIAL), each = 12 * length(unique(Insumo$ACTOR)))
)

# Calcular la META_2025
nuevos_datos$META_2025 <- (nuevos_datos$TOTAL_NORMALIZACIONES / nuevos_datos$TOTAL_ASIGNADO) * 100

# Guardar los resultados en un archivo de Excel
write_xlsx(nuevos_datos, "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Recaudo/Resultado/Meta_recaudo_2025_Regionales.xlsx")

