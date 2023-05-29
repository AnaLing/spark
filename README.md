#Laura Bodas López y Ana Ling Fernández Barba  <br />
**Planteamiento del problema**  <br />
Vamos a usar los datos de Bicimad del año 2017, en donde están recopilados los datos de los meses desde abril hasta diciembre. Los datos que hemos usado de cada línea son el id del ususario, el id de la estación, el día, el mes, el año y la hora de cada alquiler de bicicleta.  <br />
EL objetivo es tener una visión global de varios aspectos:  <br />
- La franja horaria en la que más se alquilan las bicicletas.  <br />
- El día con más afluencia en cada mes.  <br />
- La estación más popular en cada mes. <br />
- El mes con mayor demanda en el alquiler de bicicletas. <br />

Con estos datos se pretende establecer medidas de marketing para mejorar la eficiencia en el servicio de alquiler. <br /> <br /> 
**Ejecución del problema en Spark** <br />
Hemos definido las funciones con las que obtendremos los datos que queremos. <br />
La función **datos(line)** nos proporciona un diccionario a partir de los datos de una línea del archivo .json. <br />
La función **hora(data)** tiene como argumento un rdd de diccionarios y lo filtra para obtener un rdd con los diccionarios de cada ususario según su franja horaria. Después hacemos un recuento de cuántos usuarios han alquilado en cada momento y creamos una lista con los porcentajes correspondientes. <br />
La función **dia(data)** nos da una lista con dos tuplas. La primera tupla tiene como elementos el día del mes en el que se han alquilado más bicicletas y la cantidad de bicicletas alquiladas. La segunda tupla corresponde al día con menos cantidad de alquileres de bicicletas. <br />
La función **estacion(data)** devuelve una lista con dos tuplas. La primera tupla contiene la id de la estación con más alquileres y la cantidad y la segunda tupla sería para la estación con menos alquileres de bicicletas. <br />
En el main() cargamos los archivos de los meses de 2017 y a cada archivo le asignamos el número del mes. Así el mes 4 es abril, el mes 5 es mayo, etc. Para cada mes obtenemos todos los datos y ejecutamos las funciones que hemos definido previamente. Luego lo escribimos en un archivo llamado 'resultado.txt'. <br /> 
Por último, hemos añadido la creación de un histograma para tener una imagen más visual de la cantidad de bicicletas alquiladas a lo largo de todo el año. <br /> <br /> 
**Solución del problema** <br /> 
Una vez obtenidos todos los datos de cada mes en el archivo 'resultado.txt' se puede hacer un análisis y tomar medidas. Por ejemplo, se pueden establecer ofertas de grupos numerosos durante las franjas con más afluencia para así incentivar a los usuarios a que traigan a conocidos y alquilar bicilctas en grupos. Con los datos de los días más concurridos de cada mes nos permite por ejemplo, realizar descuentos en esos días ya que la disponibilidad de los usuarios parece ser mayor. <br /> 
Los datos de la estación más popular nos serviría para realizar, por ejemplo, reformas en esa estación y mejorar su imagen de cara al público. <br /> 
Por último, el histograma nos ofrece un panorama del uso del servicio de alquiler de bicicletas por temporada. Sería de esperar que en las temporadas de vacaciones (vacaciones de verano, vacaciones de invierno, Semana Santa, etc) haya más afluencia. Y durante esas temporadas subir los precios conforme a la demanda.

