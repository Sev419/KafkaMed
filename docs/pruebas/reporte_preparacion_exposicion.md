# Reporte de preparacion para exposicion KafkaMed

## 1. Elementos que deben estar abiertos
- GitHub README del repositorio KafkaMed.
- Power BI Desktop con el dashboard cargado.
- PowerShell en la raiz del proyecto.
- Navegador con endpoints:
  - `http://localhost:8000/health`
  - `http://localhost:8000/stats`
  - `http://localhost:8000/risk-summary`
- Docker Desktop funcionando.

## 2. Orden sugerido de exposicion
1. Problema: monitoreo cardiaco y alerta temprana de riesgo.
2. Arquitectura: Kafka, Spark, MongoDB, Flask API y Power BI.
3. Pipeline: productor Kafka y topic `heart-records`.
4. Procesamiento: Spark Structured Streaming.
5. Modelo ML: metricas y prediccion `risk` / `no_risk`.
6. Persistencia: MongoDB con `heart_predictions`.
7. API: endpoints clinicos.
8. Power BI: dashboard y visualizaciones.
9. Conclusiones: ventajas de Kafka frente a socket simple.

## 3. Comandos utiles para la demo

Levantar servicios base:

```powershell
docker compose up -d kafka mongo api
```

Publicar eventos:

```powershell
python kafka/producer.py --limit 5
```

Ejecutar streaming con ML:

```powershell
docker compose --profile processing run --rm spark-heart-ml-mongo
```

Probar API:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/stats
Invoke-RestMethod http://localhost:8000/risk-summary
Invoke-RestMethod "http://localhost:8000/predictions?limit=5"
```

Validar MongoDB:

```powershell
docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('kafkamed_db').heart_predictions.countDocuments({})"
```

## 4. Riesgos de demo
- Docker Desktop debe estar funcionando antes de iniciar.
- La API debe estar levantada antes de refrescar Power BI.
- Power BI debe tener datos actualizados desde la API Flask.
- Evitar ejecutar pipelines pesados durante la exposicion si no es necesario.
- Si el volumen de MongoDB fue eliminado, se debe ejecutar nuevamente el pipeline de streaming para repoblar `heart_predictions`.

## 5. Evidencias visuales esperadas
- `docs/pruebas/imagenes/powerbi_resumen.png`
- `docs/pruebas/imagenes/powerbi_predicciones.png`
- `docs/pruebas/imagenes/powerbi_analisis_clinico.png`
- `docs/pruebas/imagenes/powerbi_monitoreo.png`

Estado actual: pendientes de adjuntar al repositorio, salvo que el usuario las agregue manualmente.
