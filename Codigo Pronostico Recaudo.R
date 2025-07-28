library(readxl)
library(dplyr)
library(writexl)
library(forecast)
library(tidyverse)
library(lubridate)
library(zoo)
library(plm)

Historico_Recaudo <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Insumo_recaudo2024.xlsx")
Regresoras_Forecast <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Regresoras_Forecast.xlsx")

Recaudo_ts <-ts(Historico_Recaudo$Recaudo, frequency = 12, start =  2021)
view(Recaudo_ts)

autoplot(Recaudo_ts)+
  labs(title = "Serie de tiempo",       
       x = "Tiempo",
       y = "Valor",
       colour = "#00a0dc")+
  theme_bw() 



## Descomponemos la serie de tiempo ##
fit <- decompose(Recaudo_ts, type='additive')

autoplot(fit)+
  labs(title = "Descomposición de la serie de tiempo",                   
       x = "Tiempo",
       y = "Valor",
       colour = "Gears")+
  theme_bw()

## Graficar serie de tiempo con su tendencia ##

autoplot(Recaudo_ts, series="Serie tiempo") + 
  autolayer(trendcycle(fit), series="Tendencia") +
  labs(title = "Serie de tiempo",      
       x = "Tiempo",
       y = "Valor"
  ) + 
  theme_bw()

## pronosticos ##


## Considerando Estacionalidad ##
m1 <- snaive(Recaudo_ts, h=4)
autoplot(m1)
print(m1)

autoplot(m1)+autolayer(fitted(m1), series="Ajuste")

## metodo regresivo ##

regresion <- tslm(Recaudo_ts ~ season + trend)
m2 <- forecast(regresion, h=4)
print(m2)

autoplot(m2)
autoplot(m2)+autolayer(fitted(m2), series="Ajuste")


## Metodo de Holt Winters ##

m3 <- hw(Recaudo_ts, h=4, seasonal = 'additive')
print(m3)
autoplot(m3)
autoplot(m3)+autolayer(fitted(m3), series="Ajuste")



## ARIMA ## 

modelo_arima <- auto.arima(Recaudo_ts, seasonal = TRUE)
m4 <- forecast(modelo_arima, h=4)
print(m4)
autoplot(m4)
autoplot(m4)+autolayer(fitted(m4), series="Ajuste")


## Variables Exogenas - Redes Neuronales##

exogenos <- cbind(Historico_Recaudo$Dias_laborales, Historico_Recaudo$Termina_dia_H,Historico_Recaudo$Mes_Ant_DiaH)
exgonas_forecast <- cbind(Regresoras_Forecast$Dias_laborales, Regresoras_Forecast$Termina_dia_H, Regresoras_Forecast$Mes_Ant_DiaH)
model_nnet <- nnetar(Recaudo_ts, xreg = exogenos)
summary(model_nnet)
forecast_nnet <- forecast(model_nnet,xreg = exgonas_forecast)
print(forecast_nnet)
autoplot(forecast_nnet)+autolayer(fitted(forecast_nnet), series="Ajuste")





  