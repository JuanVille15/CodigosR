library(readxl)
library(dplyr)
library(writexl)
library(forecast)
library(tidyverse)
library(lubridate)

Historico <- read_xlsx("C:/Users/jjvt9593/OneDrive - Grupo Coomeva/Escritorio/Carpetas/Proyecciones Cierre/Insumo_proyeccion2024.xlsx")


facturacion_ts <-ts(Historico, frequency = 12, start =  2021)
view(facturacion_ts)

autoplot(facturacion_ts)+
  labs(title = "Serie de tiempo",       
       x = "Tiempo",
       y = "Valor",
       colour = "#00a0dc")+
  theme_bw() 



## Descomponemos la serie de tiempo ##
fit <- decompose(facturacion_ts, type='additive')

autoplot(fit)+
  labs(title = "Descomposición de la serie de tiempo",                   
       x = "Tiempo",
       y = "Valor",
       colour = "Gears")+
  theme_bw()

## Graficar serie de tiempo con su tendencia ##

autoplot(facturacion_ts, series="Serie tiempo") + 
  autolayer(trendcycle(fit), series="Tendencia") +
  labs(title = "Serie de tiempo",      
       x = "Tiempo",
       y = "Valor"
  ) + 
  theme_bw()

## pronosticos ##


## Considerando Estacionalidad ##
m1 <- snaive(facturacion_ts, h=4)
autoplot(m1)
print(m1)

autoplot(m1)+autolayer(fitted(m1), series="Ajuste")

## metodo regresivo ##

regresion <- tslm(facturacion_ts ~ season + trend)
m2 <- forecast(regresion, h=12)
print(m2)

autoplot(m2)
autoplot(m2)+autolayer(fitted(m2), series="Ajuste")


## Metodo de Holt Winters ##

m3 <- hw(facturacion_ts, h=12, seasonal = 'additive')
print(m3)
autoplot(m3)
autoplot(m3)+autolayer(fitted(m3), series="Ajuste")



## ARIMA ## 

modelo_arima <- auto.arima(facturacion_ts)
m4 <- forecast(modelo_arima, h=12)
print(m4)
autoplot(m4)
autoplot(m4)+autolayer(fitted(m4), series="Ajuste")



