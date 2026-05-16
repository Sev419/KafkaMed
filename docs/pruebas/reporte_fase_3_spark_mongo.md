# Reporte de Fase 3 - Spark Structured Streaming -> MongoDB

## 1. Objetivo

Persistir en MongoDB los registros clinicos leidos por Spark Structured
Streaming desde Kafka, sin introducir aun modelo ML, Flask clinico ni Power BI.

## 2. Archivos creados

- `spark_processing/src/mongo_config.py`
- `spark_processing/src/mongo_writer.py`
- `spark_processing/jobs/process_heart_records_to_mongo.py`
- `docs/pruebas/reporte_fase_3_spark_mongo.md`

## 3. Archivos modificados

- `.env.example`
- `docker-compose.yml`
- `tests/test_project_structure.py`

## 4. Comandos ejecutados y resultado

### Sintaxis y pruebas basicas

```powershell
python -m py_compile spark_processing\src\mongo_config.py
python -m py_compile spark_processing\src\mongo_writer.py
python -m py_compile spark_processing\jobs\process_heart_records_to_mongo.py
python -m pytest tests\test_project_structure.py
docker compose config
```

Resultado:
- OK
- La nueva estructura de Fase 3 compila correctamente.
- La prueba de estructura paso con 2 pruebas.
- `docker compose config` valido el Compose con `mongo` y `spark-heart-mongo`.

### Intento operativo con Docker

```powershell
docker version
docker compose up -d kafka mongo
python kafka\producer.py --limit 5
docker compose --profile processing build spark-heart-mongo
```

Resultado:
- `docker version` fallo con:
  `permission denied while trying to connect to the docker API at npipe:////./pipe/dockerDesktopLinuxEngine`
- `docker compose up -d kafka mongo` fallo con el mismo bloqueo del daemon Docker.
- `python kafka\producer.py --limit 5` fallo de forma controlada porque no habia broker disponible:
  `KafkaError{code=_TRANSPORT,val=-195,str="Failed to get metadata: Local: Broker transport failure"}`
- `docker compose --profile processing build spark-heart-mongo` fallo por acceso al daemon/builder de Docker Desktop:
  `open C:\Users\User\.docker\buildx\.lock: Access is denied.`

## 5. Evidencia de registros insertados en MongoDB

No fue posible validar inserciones reales en MongoDB en esta sesion porque Docker
Desktop no respondio desde la terminal actual.

## 6. Cantidad de documentos insertados

No verificado en esta sesion.

## 7. Ejemplo de documento insertado

No disponible, porque la escritura a MongoDB no pudo ejecutarse con el daemon
Docker inaccesible.

## 8. Problemas encontrados

1. El daemon de Docker Desktop no responde desde esta sesion.
2. El productor Kafka no pudo conectar a `localhost:9092` porque el broker no
   pudo levantarse.
3. El build del servicio `spark-heart-mongo` quedo bloqueado por permisos sobre
   `C:\Users\User\.docker\buildx\.lock`.

## 9. Estado final

La Fase 3 queda **pendiente** en esta sesion, no por el codigo sino por el
acceso al runtime Docker.

## 10. Proximo paso recomendado

Reactivar el acceso al daemon Docker Desktop y repetir:

```powershell
docker compose up -d kafka mongo
python kafka\producer.py --limit 5
docker compose --profile processing build spark-heart-mongo
docker compose --profile processing run --rm spark-heart-mongo
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_records.countDocuments({})"
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_records.findOne()"
```

