# Dashboard Power BI KafkaMed

## Objetivo
El dashboard Power BI de KafkaMed permite visualizar registros clinicos, predicciones de riesgo cardiaco y metricas agregadas generadas por el pipeline Big Data.

## Fuente de datos
Power BI debe consumir la API Flask clinica, no MongoDB directamente.

Base local:

```text
http://localhost:8000
```

Endpoints usados:

- `/stats`
- `/risk-summary`
- `/predictions`
- `/patients`

## Archivo PBIX esperado
Guardar el archivo del dashboard como:

```text
dashboard/powerbi/KafkaMed_Dashboard.pbix
```

No afirmar que el archivo existe hasta que este guardado en esta ruta.

## Capturas esperadas
Guardar evidencias visuales en:

```text
docs/pruebas/imagenes/
```

Nombres esperados:

- `docs/pruebas/imagenes/powerbi_resumen.png`
- `docs/pruebas/imagenes/powerbi_predicciones.png`
- `docs/pruebas/imagenes/powerbi_analisis_clinico.png`
- `docs/pruebas/imagenes/powerbi_monitoreo.png`
- `docs/pruebas/imagenes/powerbi_api_datos.png`
- `docs/pruebas/imagenes/powerbi_modelo_datos.png`

Las imagenes originales del dashboard estan en:

```text
dashboard/powerbi/
```

Las evidencias oficiales usadas por el README principal estan copiadas en:

```text
docs/pruebas/imagenes/
```

Si se desea versionar el archivo de Power BI, guardarlo como:

```text
dashboard/powerbi/KafkaMed_Dashboard.pbix
```

## Visualizaciones sugeridas
- Tarjetas KPI: total, risk, no_risk, probabilidad promedio.
- Grafico de barras: distribucion `risk` vs `no_risk`.
- Tabla de predicciones filtrable por `prediction_label`.
- Analisis clinico por edad, sexo, tipo de dolor de pecho y variables cardiacas.

## Validacion antes de exponer
1. Levantar API:

```powershell
docker compose up -d mongo api
```

2. Verificar endpoints:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/stats
Invoke-RestMethod http://localhost:8000/risk-summary
```

3. Abrir Power BI Desktop y refrescar datos.
