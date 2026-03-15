/*
Enunciado 1.  
Explora el fichero flights y analiza: 

	1. Cuántos registros hay en total = 1209
	2. Cuántos vuelos distintos hay = 266
	3. Cuántos vuelos tienen más de un registro = 250
*/

with start as (
	select
		count(*) as total_registros,
		count(distinct unique_identifier) as distinct_unique_flights
	from flights
),
base as (
	select
		unique_identifier,
		row_number() over(partition by unique_identifier order by updated_at) as rn
	from flights
),
final as (
select
	unique_identifier,
	max(rn) as max_rn_flight
from base
group by unique_identifier
having max(rn) > 1
order by unique_identifier, max_rn_flight
)
select
	st.total_registros,
	st.distinct_unique_flights,
	count(fn.unique_identifier) as flights_more_than_one_record
from start as st
cross join final as fn
group by 1, 2;

/*
Enunciado 2. 
Por qué hay registro duplicados para un mismo vuelo. Para ello, selecciona varios vuelos y 
analiza la evolución temporal de cada vuelo. 

	1. Qué información cambia de un registro a otro
*/

-- Vuelos con 1 registro
with base as (
	select
		unique_identifier,
		row_number() over(partition by unique_identifier order by updated_at) as rn
	from flights
),
final as (
	select
		unique_identifier,
		max(rn) as max_rn
	from base
	group by 1
	having max(rn) = 1
	order by unique_identifier, max_rn
)
select
	fl.*
from flights as fl
inner join final as fn
on fl.unique_identifier = fn.unique_identifier;

/*
 Los vuelos que UNICAMENTE tienen 1 registo tienen NULL en las columnas donde 
 aparece "actual" (como local_actual_departure, local_actual_arrival), Eso se
 interpreta como que el avion se CANCELO ya que no hay registros de los valores
 reales ("actual")
 */

-- Vuelos con 2 registro
with base as (
	select
		unique_identifier,
		row_number() over(partition by unique_identifier order by updated_at) as rn
	from flights
),
final as (
	select
		unique_identifier,
		max(rn) as max_rn
	from base
	group by 1
	having max(rn) = 2
	order by unique_identifier, max_rn
)
select
	fl.*
from flights as fl
inner join final as fn
on fl.unique_identifier = fn.unique_identifier;

/*
 En los vuelos donde existe MAS de 1 registo, por ejemplo esta query donde los 
 regitros = 2, las columnas donde aparece "actual" (como local_actual_departure, local_actual_arrival) 
 si poseen informacion real y se interpreta como que el avion SI DESPEGO y pudo haber tenido un DELAY, 
 ya sea positivo (llego tarde), negativo (llego antes de lo previsto). Cuando el DELAY es 0 es porque
 el avion llego segun lo planeado.
 */

/*
 Los registros duplicados existen porque cada vuelo se actualiza varias veces a lo largo del tiempo.
 Cada registro refleja un estado diferente del vuelo según la información disponible en ese momento (horarios reales, retrasos, estado del vuelo, etc.).
 La columna updated_at permite observar esa evolución temporal. La informacion que cambia son el status (active, cancelled), local_actual_departure,
 local_actual_arrival, departure_delay, arrival_delay, updated at.
 */

/*
Enunciado 3. 
Evalúa la calidad del dato. La calidad del dato nos indica si la información es consistente, 
completa, coherente y representa una realidad verosímil. Para ello debemos establecer 
unos criterios: 

	1. La información de created_at debe ser única para cada vuelo aunque tenga más de 
	un registro. 
	2. La información de updated_at deber ser igual o más que la información de 
	created_at, lo que nos indica coherencia y consistencia
*/
select
    unique_identifier,
    count(distinct created_at) as created_at_distinct
from flights
group by unique_identifier
having count(distinct created_at) > 1;

/*
 Como el resultado de la query anterior dio 0 filas, la conclusion es que la columna
 created_at es consistente por vuelo (unica). Cada unique_identifier mantiene el mismo 
 created_at aunque tenga múltiples registros.
 */

select *
from flights
where updated_at < created_at;

/*
 Como el resultado de la query anterior dio 0 filas, la conclusion es que la secuencia
 temporal de la columna updated_at es consistente. Los registros reflejan actualizaciones 
 posteriores del vuelo, por lo que la evolución temporal es coherente.
 */

/*
Enunciado 4. 
El último estado de cada vuelo. Cada vuelo puede aparecer varias veces en el dataset, para 
avanzar con nuestro análisis necesitamos quedarnos solo con el último registro de cada 
vuelo. 
 
Puedes crear una tabla o vista resultante de esta query en tu base de datos local, la 
utilizaremos en los siguientes enunciados. Si prefieres no guardar la última información, 
tendrás que hacer uso de esa query como una CTE en los enunciados siguientes.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
)
select
	*
from base
where rn = 1;

/*
Enunciado 5. 
Considerando que los campos local_departure y local_actual_departure son necesarios 
para el análisis, valida y reconstruye estos valores siguiendo estas reglas: 

	1. Si local_departure es nulo, utiliza created_at. 
	2. Si local_actual_departure es nulo, utiliza local_departure. Si este también es nulo, 
	utiliza created_at.
 
Crea dos nuevos campos: 
	● effective_local_departure 
	● effective_local_actual_departure 
	
Extra: 
Realiza las validaciones para los campos local_arrival y local_actual_arrival.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn,
		case
			when local_departure is null then created_at
			else local_departure -- Comentar para checar que lo este haciendo bien
		end as effective_local_departure,
		case
			when local_actual_departure is null and local_departure is null then created_at
			when local_actual_departure is null then local_departure
			else local_actual_departure -- Comentar para checar que lo este haciendo bien
		end as effective_local_actual_departure	
	from flights
)
select
	unique_identifier,
	local_departure,
	effective_local_departure,
	local_actual_departure,
	effective_local_actual_departure,
	created_at,
	rn
from base
where rn = 1;

---------------------------------

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		unique_identifier,
		local_departure,
		local_actual_departure,
		local_arrival,
		local_actual_arrival,
		created_at,
		updated_at,
		case
			when local_departure is null then created_at
			else local_departure
		end as effective_local_departure,
		case
			when local_actual_departure is null and local_departure is null then created_at
			when local_actual_departure is null then local_departure
			else local_actual_departure
		end as effective_local_actual_departure,
		case
			when local_arrival is null then created_at
			else local_arrival
		end as effective_local_arrival,
		case
			when local_actual_arrival is null and local_arrival is null then created_at
			when local_actual_arrival is null then local_arrival
			else local_actual_arrival
		end as effective_local_actual_arrival
	from base
	where rn = 1
)
select *
from final;

/*
Enunciado 6. 
Análisis del estado del vuelo. Haciendo uso del resultado del enunciado 4, analiza los 
estados de los vuelos.
  
	1. Qué estados de vuelo existen:
		CX: Canceled
		DY: Delayed
		EY: Early
		NS: No Show
		OT: On Time
		[Null]: No hay registro

	2. Cuántos vuelos hay por cada estado 
		CX: 6
		DY: 143
		EY: 9
		NS: 8
		OT: 93
		[Null]: 7
	
¿Podrías decir qué significa las siglas de cada estado? 
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
)
select
	arrival_status,
	count(*) as total_vuelos
from base
where rn = 1
group by 1
order by 1;


/*
Enunciado 7. 
País de salida de cada vuelo. Tienes disponible un csv. con información de aeropuertos 
airports.csv. Haciendo uso del resultado del enunciado 4, analiza los aeropuertos de salida. 

	1. De qué país despegan los vuelos 
	2. Cuántos vuelos despegan por país 
	
Los vuelos despegan principalmente de Spain, seguido de NULL (no hay registro), luego France, 
United States, Netherlands y United Kingdom. La presencia de NULL indica que hay aeropuertos de 
salida en flights que no están dados de alta en la tabla airports.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		unique_identifier,
		departure_airport,
		updated_at,
		rn
	from base
	where rn = 1
),
info_airports as (
	select
		airport_code,
		country
	from airports
)
select
	air.country,
	count(fn.unique_identifier) as total_vuelos
from final as fn
left join info_airports as air
on fn.departure_airport = air.airport_code
group by 1
order by total_vuelos desc, country asc;

/*
Enunciado 8. 
Delay medio y estado de vuelo por país de salida. Haciendo uso del resultado del enunciado 
4, analiza el estado y el delay/retraso medio con el objetivo de identificar si existen países 
que pueden presentar problemas operativos en los aeropuertos de salida.
 
	1. Cuál es el delay medio por país 
	2. Cuál es la distribución de estados de vuelos por país. 
	
Extra: 
Representa gráficamente la distribución de estados por país. Puedes dibujar un gráfico de 
barras o representarlo como creas que mejor se visualiza.

Los países con más problemas operativos, tomando como referencia el delay_medio y la 
concentración de vuelos DY, son principalmente:
	- United States
	- France
	- Spain
Además, Spain concentra el mayor volumen total de vuelos y también una cantidad alta de 
vuelos con estado DY, por lo que operativamente es el país más relevante del dataset.

Netherlands destaca porque su delay_medio es negativo, lo que sugiere mejor comportamiento 
operativo en comparación con los países anteriores.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
info_airports as (
	select
		airport_code,
		country
	from airports
)
select
	air.country,
	count(fn.unique_identifier) as total_vuelos,
	round(avg(fn.delay_mins), 2) as delay_medio
from final as fn
left join info_airports as air
on fn.departure_airport = air.airport_code
group by 1
order by delay_medio desc nulls last, country asc;

----------------------------------------------

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
info_airports as (
	select
		airport_code,
		country
	from airports
)
select
	air.country,
	arrival_status,
	count(fn.unique_identifier) as total_vuelos
from final as fn
left join info_airports as air
on fn.departure_airport = air.airport_code
group by 1, 2
order by 1, 2;

/*
Enunciado 9. 
El estado de vuelo por país y por época del año. Dado que no en todas las épocas del año 
las condiciones climatólogicas son iguales, analiza si la estaciones del año impactan en el 
delay medio por país. Considera la siguiente clasificación de meses del año por época: 

	● Invierno: diciembre, enero, febrero 
	● Primavera: marzo, abril, mayo 
	● Verano: junio, julio, agosto 
	● Otoño: septiembre, octubre, noviembre 
	
Sí, la época del año sí impacta el delay medio por país.

Lo más claro en el dataset es:
	- Spain empeora en verano.
	- United States empeora mucho en otoño.
	- France también sube en verano.
	- Netherlands mantiene el mejor comportamiento, incluso con delays medios negativos 
		en invierno y verano.

El delay_medio no se comporta igual en todas las épocas del año. Al agrupar por país y 
estación, se observa que en algunos países sí existe un impacto estacional. Por ejemplo, 
Spain presenta su peor delay_medio en verano y United States en otoño. France también 
empeora en verano, mientras que Netherlands mantiene un comportamiento más estable e incluso
con retrasos medios negativos en algunas estaciones. Por tanto, en este dataset sí hay 
evidencia de que la estación del año puede impactar el retraso medio según el país de salida.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn,
		case
			when local_departure is null then created_at
			else local_departure
		end as effective_local_departure
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
info_airports as (
	select
		airport_code,
		country
	from airports
),
season_flights as (
	select
		fn.unique_identifier,
		air.country,
		fn.arrival_status,
		fn.delay_mins,
		fn.effective_local_departure,
		case
			when extract(month from fn.effective_local_departure) in (12, 1, 2) then 'Invierno'
			when extract(month from fn.effective_local_departure) in (3, 4, 5) then 'Primavera'
			when extract(month from fn.effective_local_departure) in (6, 7, 8) then 'Verano'
			when extract(month from fn.effective_local_departure) in (9, 10, 11) then 'Otoño'
		end as season_year
	from final as fn
	left join info_airports as air
	on fn.departure_airport = air.airport_code
)
select
	country,
	season_year,
	count(*) as total_vuelos,
	round(avg(delay_mins), 2) as delay_medio
from season_flights
group by 1, 2
order by 1, 2;

/*
Enunciado 10. 
Frecuencia de actualización de los vuelos. Volviendo al análisis de la calidad del dataset, 
explora con qué frecuencia se registran actualizaciones de cada vuelo y calcula la 
frecuencia media de actualización por aeropuerto de salida.

R: El dataset muestra que las actualizaciones se registran con una frecuencia media de 6 horas 
entre snapshots consecutivos del mismo vuelo.
*/

with base as (
	select
		*,
		lag(updated_at) over(partition by unique_identifier order by updated_at) as after_updated_at,
		updated_at - lag(updated_at) over(partition by unique_identifier order by updated_at) as dif_updated_at
	from flights
),
final as (
	select
		unique_identifier,
		departure_airport,
		updated_at,
		after_updated_at,
		dif_updated_at
	from base
	where after_updated_at is not null
),
info_airports as (
	select
		airport_code,
		airport_name,
		country
	from airports
)
select
	fn.departure_airport,
	air.airport_name,
	air.country,
	count(distinct fn.unique_identifier) as total_vuelos,
	avg(fn.dif_updated_at) as frecuencia_media_actualizacion
from final as fn
left join info_airports as air
on fn.departure_airport = air.airport_code
group by 1, 2, 3
order by 1;

/*
Enunciado 11. 
Consistencia del dato. El campo unique_identifier identifica el vuelo y se construye con: 
aerolínea, número de vuelo, fecha y aeropuertos. Para cada vuelo (último snapshot), 
comprueba si la información del unique_identifier es consistente con las columnas del 
dataset. 

	1. Crea un flag is_consistent. 
	2. Calcula cuántos vuelos no son consistentes = 15
	3. Usando la tabla airlines, muestra el nombre de la aerolínea y cuántos vuelos no 
	consistentes tiene
	
	Todos los vuelos no consistentes (15) pertenecen a la aerolinea IB: Iberia.
*/

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
consistency_flights as (
	select
		unique_identifier,
		airline_code,
		departure_airport,
		arrival_airport,
		local_departure,
		created_at,
		updated_at,
		split_part(unique_identifier, '-', 1) as id_airline_code,
		split_part(unique_identifier, '-', 2) as id_flight_number,
		split_part(unique_identifier, '-', 3) as id_date_flight,
		split_part(unique_identifier, '-', 4) as id_departure_airport,
		split_part(unique_identifier, '-', 5) as id_arrival_airport,
		case
			when split_part(unique_identifier, '-', 1) = airline_code
			 and split_part(unique_identifier, '-', 3) = replace(coalesce(local_departure, created_at)::date::text, '-', '')
			 and split_part(unique_identifier, '-', 4) = departure_airport
			 and split_part(unique_identifier, '-', 5) = arrival_airport
			then 1
			else 0
		end as is_consistent
	from final
)
select
	*
from consistency_flights;

------------------------------------

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
consistency_flights as (
	select
		unique_identifier,
		airline_code,
		departure_airport,
		arrival_airport,
		local_departure,
		created_at,
		case
			when split_part(unique_identifier, '-', 1) = airline_code
			 and split_part(unique_identifier, '-', 3) = replace(coalesce(local_departure, created_at)::date::text, '-', '')
			 and split_part(unique_identifier, '-', 4) = departure_airport
			 and split_part(unique_identifier, '-', 5) = arrival_airport
			then 1
			else 0
		end as is_consistent
	from final
)
select
	count(*) as total_vuelos_no_consistentes
from consistency_flights
where is_consistent = 0;

---------------------------------------------

with base as (
	select
		*,
		row_number() over(partition by unique_identifier order by updated_at desc) as rn
	from flights
),
final as (
	select
		*
	from base
	where rn = 1
),
consistency_flights as (
	select
		unique_identifier,
		airline_code,
		case
			when split_part(unique_identifier, '-', 1) = airline_code
			 and split_part(unique_identifier, '-', 3) = replace(coalesce(local_departure, created_at)::date::text, '-', '')
			 and split_part(unique_identifier, '-', 4) = departure_airport
			 and split_part(unique_identifier, '-', 5) = arrival_airport
			then 1
			else 0
		end as is_consistent
	from final
)
select
	al.name,
	count(*) as total_vuelos_no_consistentes
from consistency_flights as cf
left join airlines as al
on cf.airline_code = al.airline_code
where cf.is_consistent = 0
group by 1
order by 2 desc, 1 asc;
