from pyspark import SparkContext
import json
sc = SparkContext()
archivo = 'abril2017.json'
rdd_base = sc.textFile(archivo)
def datos(line):
    dic = {}
    data = json.loads(line)
    dic['usuario'] = data['user_day_code']
    dic['start'] = data['idunplug_station']
    time =  data['unplug_hourTime']['$date']
    fecha , hora = time.split('T')
    año,mes,dia= fecha.split('-')
    hora , *s = hora.split(':')
    dic['año'] = int(año)
    dic['mes'] = int(mes)
    dic['dia'] = int(dia)
    dic['hora'] = int(hora)
    return dic

data = rdd_base.map(datos)

# Seleccionamos los usuarios de mañana [6:00-14:00)
data_mañana = data.filter(lambda x: 6 <= x['hora'] < 14)
# Seleccionamos los usuarios de tarde [14:00-22:00)
data_tarde = data.filter(lambda x: 14 <= x['hora'] < 22)
# Seleccionamos los usuarios de noche [22:00-6:00)
data_noche = data.filter(lambda x: 6 > x['hora'] or x['hora'] >= 22)

cantidad_datos_mañana = data_mañana.count()
# Imprimir la cantidad de datos en data_mañana
print("Cantidad de bicicletas alquiladas por la mañana:", cantidad_datos_mañana)
cantidad_datos_tarde = data_tarde.count()
# Imprimir la cantidad de datos en data_tarde
print("Cantidad de bicicletas alquiladas por la tarde:", cantidad_datos_tarde)
cantidad_datos_noche = data_noche.count()
# Imprimir la cantidad de datos en data_noche
print("Cantidad de bicicletas alquiladas por la noche:", cantidad_datos_noche)
# Los porcentajes de popularidad en función de la franja horaria
print('mañana:',(cantidad_datos_mañana/data.count()) * 100, 'tarde:',(cantidad_datos_tarde/data.count()) * 100, 'noche:',(cantidad_datos_noche/data.count()) * 100)

#Obtener el día más popular y el menos y la cantidad de bicis alquiladas
listadia = []
for i in range(data.count()):
    data_dia = data.filter(lambda x: x['dia'] == i+1)
    cantidad_data_dia = data_dia.count()
    listadia.append(cantidad_data_dia)
# Obtener el máximo y el menor valor de bicicletas alquiladas en un día y el día
maximo_valor = max(listadia)
menor_valor = min(listadia)
dia = listadia.index(maximo_valor)+1
# Imprimir el máximo y el mínimo valor y el día 
print("Máxima cantidad de bicicletas alquiladas en un día:", maximo_valor)
print("Día en el que se alquilaron más bicicletas:", dia)
print("Menor cantidad de bicicletas alquiladas en un día:", maximo_valor)
print("Día en el que se alquilaron menos bicicletas:", dia)
    
#Obtener la estación más popular del mes y la menos popular
#Realizar un conteo de frecuencia de idunplug_station
frecuencia_estaciones = data.map(lambda x: (x['start'], 1)).reduceByKey(lambda a, b: a + b)
# Encontrar la estación más popular y la menos popular
estacion_popular = max(frecuencia_estaciones, key=lambda x: x[1])
estacion_no_popular = min(frecuencia_estaciones, key=lambda x: x[1])
# Imprimir la estación más popular y la menos popular
print("El id de la estación más popular es:", estacion_popular[0])
print("Cantidad de veces que se encontró:", estacion_popular[1])
print("El id de la estación menos popular es:", estacion_no_popular[0])
print("Cantidad de veces que se encontró:", estacion_no_popular[1])

#Obtener el mes más popular y el menos popular
listames = []
cantidadmes = []
for i,mes in enumerate(['abril', 'mayo','junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre']):
    archivo = mes + '2017.json'
    rdd_base = sc.textFile(archivo)
    data = rdd_base.map(datos)
    listames.append((mes, data.count()))
    cantidadmes.append(data.count())
mes_popular = max(listames, key=lambda x: x[1])
mes_no_popular = min(listames, key=lambda x: x[1])
print("El mes más popular es:", mes_popular[0])
print("Cantidad de bicis alquiladas en el mes más popular:", mes_popular[1]) 
print("El mes menos popular es:", mes_no_popular[0])
print("Cantidad de bicis alquiladas en el mes menos popular:", mes_no_popular[1])     

# Histograma de la cantidad de bicis alquiladas cada mes
import matplotlib.pyplot as plt
fig, ax = plt.subplots()
ax.barh(['abril', 'mayo','junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'],cantidadmes)
fig.show()

    
    








