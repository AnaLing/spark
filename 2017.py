from pyspark import SparkContext, SparkConf
import json
FILES = [
    "/public/bicimad/201704_movements.json",
    "/public/bicimad/201705_movements.json",
    "/public/bicimad/201706_movements.json",
    "/public/bicimad/201707_movements.json",
    "/public/bicimad/201708_movements.json",
    "/public/bicimad/201709_movements.json",
    "/public/bicimad/201710_movements.json",
    "/public/bicimad/201711_movements.json",
    "/public/bicimad/201712_movements.json"] #Vamos a usar los datos del año 2017
        
def datos(line): #Diccionario con los datos de una linea
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

def hora(data): #Nos da una lista con los porcentajes de uso de bicicletas por la mañana, tarde y noche
    # Seleccionamos los usuarios de mañana [6:00-14:00)
    data_mañana = data.filter(lambda x: 6 <= x['hora'] < 14)
    # Seleccionamos los usuarios de tarde [14:00-22:00)
    data_tarde = data.filter(lambda x: 14 <= x['hora'] < 22)
    # Seleccionamos los usuarios de noche [22:00-6:00)
    data_noche = data.filter(lambda x: 6 > x['hora'] or x['hora'] >= 22)    
    cantidad_datos_mañana = data_mañana.count()
    cantidad_datos_tarde = data_tarde.count()
    cantidad_datos_noche = data_noche.count()
    lista = [cantidad_datos_mañana/data.count() * 100, cantidad_datos_tarde/data.count() * 100, cantidad_datos_noche/data.count() * 100]
    return lista

def dia(data): #Nos da el día más popular y el menos popular del mes y la cantidad de bicicletas alquiladas dichos días.
    lista_dia = []
    for i in range(data.count()):
        data_dia = data.filter(lambda x: x['dia'] == i+1)
        cantidad_data_dia = data_dia.count()
        lista_dia.append(cantidad_data_dia)
    # Obtener el máximo y el menor valor de bicicletas alquiladas en un día y el día
    valor_max = max(lista_dia)
    valor_min = min(lista_dia)
    dia_max = lista_dia.index(valor_max)+1 #El día 1 estará en la posición 0, por ello se suma 1 al índice
    dia_min = lista_dia.index(valor_min)+1
    lista = [(dia_max, valor_max), (dia_min, valor_min)]
    return lista
    
def estacion(data): #Nos da la id de la estación más popular y la menos popular y la cantidad de bicicletas alquiladas en dichas estaciones
    #Realizar un conteo de frecuencia de idunplug_station
    frecuencia_estaciones = data.map(lambda x: (x['start'], 1)).reduceByKey(lambda a, b: a + b)
    # Encontrar la estación más popular y la menos popular
    estacion_popular = max(frecuencia_estaciones, key=lambda x: x[1])
    estacion_no_popular = min(frecuencia_estaciones, key=lambda x: x[1])
    lista = [estacion_popular, estacion_no_popular]
    return lista

def main():
    conf = SparkConf().setMaster("local").setAppName('Bicimad')
    sc = SparkContext(conf=conf)
    lista_mes = []
    cantidad_mes = []
    with open("resultados.txt", 'w') as fout:
         for i, f in enumerate(FILES): #Vamos a clasificar los meses según su númmero. Así, el mes 4 es abril, el mes 5 es mayo, etc.
             rdd_base = sc.textFile(f)
             data = rdd_base.map(datos)
            
             horas = hora(data)
             fout.write(f'En el mes {i+4} la afluencia por las mañanas es de un {horas[0]} %. La de por la tarde es de un {horas[1]} % y la de por la noche de un {horas[2]} % \n')
             
             dias = dia(data)
             fout.write(f'En el mes {i+4} el día más popular fue el {dias[0][0]} con una cantidad de {dias[0][1]} bicicletas alquiladas y el menos popular fue el {dias[1][0]} con un total de {dias[1][1]} bicicletas alquiladas \n')
             
             estaciones = estacion(data)
             fout.write(f'En el mes {i+4} la id de la estación más popular fue la id {estaciones[0][0]} con un total de {estaciones[0][1]} bicicletas alquiladas y la id de la estación menos popular fue la id {estaciones[1][0]} con un total de {estaciones[1][1]} bicicletas alquiladas \n')

             lista_mes.append((i+4, data.count()))
             cantidad_mes.append(data.count())
         mes_popular = max(lista_mes, key=lambda x: x[1])
         mes_no_popular = min(lista_mes, key=lambda x: x[1])
         fout.write(f'El mes más popular fue el mes {mes_popular[0]} con una cantidad de {mes_popular[1]} bicicletas alquiladas y el mes menos popular fue el mes {mes_no_popular[0]} con un total de {mes_no_popular[1]} bicicletas alquiladas \n')
 
    # Histograma de la cantidad de bicis alquiladas cada mes
    import matplotlib.pyplot as plt
    fig, ax = plt.subplots()
    ax.barh(['abril', 'mayo','junio', 'julio', 'agosto', 'septiembre', 'octubre', 'noviembre', 'diciembre'],cantidad_mes)
    fig.show()

if __name__ == "__main__":
    main()


    
    








