library(readxl)
library(dplyr)
library(writexl)
library(forecast)
library(tidyverse)
library(lubridate)
library(janitor)

# Cargar y limpiar los datos
Insumo <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Recaudo/Insumo_recuado_CSS.xlsx") %>% 
  clean_names(case = "all_caps")

# Agrupar por PERIODO y EDAD_INICIAL
Insumo <- Insumo %>%
  group_by(PERIODO, EDAD_INICIAL) %>%
  summarise(
    TOTAL_RECAUDO = sum(TOTAL_RECAUDO, na.rm = TRUE),
    TOTAL_VENCIDO = sum(TOTAL_VENCIDO, na.rm = TRUE)
  ) %>%
  ungroup()

# Validación de datos
if (any(Insumo$TOTAL_VENCIDO <= 0, na.rm = TRUE)) {
  stop("Existen valores de TOTAL_VENCIDO menores o iguales a cero. Revisa los datos de entrada.")
}

# Convertir PERIODO a tipo fecha y extraer Año y Mes
Insumo <- Insumo %>% 
  mutate(
    PERIODO = as.Date(PERIODO),
    Año = year(PERIODO),
    Mes = month(PERIODO)
  )

# Ajustar modelos ARIMA para cada grupo de EDAD_INICIAL
pred_asignados_list <- list()
pred_contenidos_list <- list()

for (grupo in unique(Insumo$EDAD_INICIAL)) {
  # Filtrar los datos por grupo de edad
  datos_grupo <- subset(Insumo, EDAD_INICIAL == grupo)
  
  # Validar que haya suficientes datos para el modelo
  if (nrow(datos_grupo) < 24) {
    warning(paste("Grupo", grupo, "no tiene suficientes datos para ajustar un modelo."))
    next
  }
  
  # Convertir las series temporales
  ts_asignado <- ts(datos_grupo$TOTAL_VENCIDO, frequency = 12)
  ts_recaudado <- ts(datos_grupo$TOTAL_RECAUDO, frequency = 12)
  
  # Ajustar modelos ARIMA
  modelo_asignado <- tslm(ts_asignado ~ season + trend)
  modelo_recaudado <- tslm(ts_recaudado ~ season + trend)
  
  # Hacer predicciones para los próximos 12 periodos
  pred_asignado <- forecast(modelo_asignado, h = 12)
  pred_recaudado <- forecast(modelo_recaudado, h = 12)
  
  # Validar predicciones
  if (any(pred_asignado$mean < 0 | pred_recaudado$mean < 0)) {
    warning(paste("Predicciones negativas en el grupo", grupo, "- revisa los datos o el modelo."))
    next
  }
  
  # Almacenar las predicciones
  pred_asignados_list[[paste(grupo)]] <- pred_asignado$mean
  pred_contenidos_list[[paste(grupo)]] <- pred_recaudado$mean
}

# Unir todas las predicciones de asignados y contenidos
total_asignados_futuros <- unlist(pred_asignados_list)
total_recaudo_futuros <- unlist(pred_contenidos_list)

# Crear el dataframe con las predicciones
nuevos_datos <- data.frame(
  TOTAL_ASIGNADO = total_asignados_futuros,
  TOTAL_RECAUDO = total_recaudo_futuros,
  EDAD_INICIAL = rep(unique(Insumo$EDAD_INICIAL), each = 12)
)

# Calcular el índice de recaudo
nuevos_datos <- nuevos_datos %>%
  mutate(
    INDICE_RECAUDO = case_when(
      # Para EDAD_INICIAL == 1, permitir índices mayores a 100%
      EDAD_INICIAL == 1 ~ (TOTAL_RECAUDO / TOTAL_ASIGNADO) * 100,
      
      # Para otros grupos, limitar el índice a un máximo de 100%
      TRUE ~ pmin((TOTAL_RECAUDO / TOTAL_ASIGNADO) * 100, 100)
    )
  )

# Comprobar índices de recaudo sospechosos
if (any(nuevos_datos$INDICE_RECAUDO > 100 & nuevos_datos$EDAD_INICIAL != 1, na.rm = TRUE)) {
  warning("Hay índices de recaudo superiores al 100% en grupos que no son EDAD_INICIAL == 1.")
}

# Guardar los resultados en un archivo de Excel
write_xlsx(
  nuevos_datos, 
  "C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Recaudo/Resultado/Meta_recaudo_2025_CSS.xlsx"
)

write_xlsx(Insumo,"C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Real_Recaudo_CSS.xlsx")

