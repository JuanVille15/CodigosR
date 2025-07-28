library(tidyverse)
library(caTools)
library(readxl)

# Cargar los datos desde un archivo CSV
data = read_xlsx("//coomeva.nal/DFSCoomeva/Cartera_Coomeva/CARTERA/1. Estatutaria/Soportes informe Consejo Presentaciones/Proyección Cierre/Proyeccion Cierre.xlsx")
# Ver una vista previa de los datos
head(data)
############################################################
# Dividir los datos en conjuntos de entrenamiento y prueba
set.seed(42)
split <- sample.split(data$`Valor real Acumulada`, SplitRatio = 0.8)
train_data <- subset(data, split == TRUE)
test_data <- subset(data, split == FALSE)

# Ajustar el modelo de regresión lineal
model <- lm(`Valor real Acumulada` ~ `Días Hábiles` + `Valor Meta Acumulada`, data = train_data)

# Ver un resumen del modelo
summary(model)
##############################################################
predictions <- predict(model, test_data)

# Calcular el error absoluto medio (MAE) y el error cuadrático medio (MSE)
mae <- mean(abs(predictions - test_data$`Valor real Acumulada`))
mse <- mean((predictions - test_data$`Valor real Acumulada`)^2)

print(paste("MAE:", mae))
print(paste("MSE:", mse))

#####################################################

# Reemplaza `dias_habiles_actuales`, `valor_real_acumulado_actual` y `valor_meta_acumulado_actual` con los valores reales actuales

dias_habiles_actuales <- 5  # Ejemplo: 15 días hábiles hasta la fecha actual
valor_real_acumulado_actual <- 26638561594  # Reemplaza con el valor real acumulado hasta la fecha actual
valor_meta_acumulado_actual <- 83426306479  # Reemplaza con el valor meta acumulado para el mes

nuevos_datos <- data.frame(`Días Hábiles` = c(dias_habiles_actuales), 
                           `Valor real Acumulada` = c(valor_real_acumulado_actual),
                           `Valor Meta Acumulada` = c(valor_meta_acumulado_actual))

names(nuevos_datos)[names(nuevos_datos) == "Días.Hábiles"] <- "Días Hábiles"
names(nuevos_datos)[names(nuevos_datos) == "Valor.real.Acumulada"] <- "Valor real Acumulada"
names(nuevos_datos)[names(nuevos_datos) == "Valor.Meta.Acumulada"] <- "Valor Meta Acumulada"

prediccion <- predict(model, nuevos_datos)

print(paste("Predicción de cierre del mes:", prediccion))


##########################################################################################################

modelo <- lm(`Valor real Acumulada` ~ `Días Hábiles`, data = data[1:8, ])

# Ver los resultados del modelo
summary(modelo)
################################################################################
dias_futuros <- data.frame(Dia_habil = 9:22)
names(dias_futuros)[names(dias_futuros) == "Dia_habil"]<-"Días Hábiles"
predicciones <- predict(modelo, newdata = dias_futuros)

# Agregar las predicciones al data frame original
predict=data$`Valor Meta Acumulada` <- c(data$`Valor real Acumulada`[1:8], predicciones)
predict=data.frame(predict)
# Mostrar los resultados
print(data)

#####################################################

proyeccion_cierre = read_xlsx("C:/Users/sasv0280/OneDrive - Grupo Coomeva/Documentos/Proyeccion cierre.xlsx", sheet="recaudo")

datos_completos <- proyeccion_cierre %>% filter(año %in% c(2022, 2023))

# Ajustar un modelo lineal simple a los datos históricos
modelo <- lm(recaudo ~ dia + factor(año), data = proyeccion_cierre)

# Crear un data frame con los días faltantes de mayo 2024
dias_faltantes <- 13:21
data_faltante <- data.frame(
  dia = dias_faltantes,
  año = rep(2024, length(dias_faltantes))
)

# Predecir los recaudos para los días faltantes de mayo 2024
data_faltante$año <- factor(data_faltante$año, levels = levels(proyeccion_cierre$año))
predicciones <- predict(modelo, newdata = data_faltante)
predicciones
# Agregar las predicciones a los datos existentes de mayo 2024
mayo_2024_completo <- c(proyeccion_cierre$recaudo[proyeccion_cierre$año == 2024], predicciones)
mayo_2024_completo=data.frame(mayo_2024_completo)
# Calcular el total proyectado para mayo 2024
total_proyectado_mayo_2024 <- sum(mayo_2024_completo)

# Mostrar resultados
cat("Predicciones para los días faltantes de mayo 2024:\n")
print(data.frame(dia = dias_faltantes, prediccion_recaudo = predicciones))
cat("\nRecaudo total proyectado para mayo 2024:", total_proyectado_mayo_2024, "\n")