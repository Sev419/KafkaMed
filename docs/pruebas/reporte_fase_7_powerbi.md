# Reporte Fase 7: Dashboard Power BI consumiendo API Flask clinica

## 1. Objetivo de la Fase 7
Construir el dashboard Power BI de KafkaMed consumiendo datos desde la API Flask clinica ya validada.

La fuente oficial de datos para Power BI es la API REST en `http://localhost:8000`, no MongoDB directamente.

Flujo consumido por BI:

```text
Kafka -> Spark Structured Streaming -> ML -> MongoDB -> Flask API -> Power BI
```

## 2. Estado previo validado
La API Flask clinica ya expone datos reales de MongoDB desde la coleccion `heart_predictions`.

Endpoints disponibles:
- `GET /`
- `GET /health`
- `GET /patients`
- `GET /predictions`
- `GET /stats`
- `GET /risk-summary`

Datos reales observados durante la validacion de Fase 6:
- Total de registros: `25`
- Registros `risk`: `6`
- Registros `no_risk`: `19`
- Promedio de probabilidad: `0.26713404364674964`
- Version del modelo: `logistic_regression_complete_dataset_v1`

## 3. Requisitos Power BI

### Version recomendada
- Power BI Desktop: version reciente de 2024 o superior.
- Power BI Service: opcional para publicacion y refresh programado.

### Modo de conexion
Para esta fase se recomienda **Import Mode**.

Justificacion:
- Los endpoints REST de Flask se consumen con `Web.Contents`.
- Power BI no ofrece DirectQuery nativo sobre una API REST simple sin conector personalizado.
- Import Mode permite transformar JSON, normalizar tablas y crear medidas DAX de forma estable.

Para Power BI Service, si la API corre localmente en `localhost`, se requiere un **On-premises data gateway** o desplegar la API en un host accesible.

## 4. Paso a paso Power BI -> API Flask

1. Confirmar que los servicios esten activos:

```powershell
docker compose up -d mongo api
```

2. Validar API desde PowerShell:

```powershell
Invoke-RestMethod http://localhost:8000/health
Invoke-RestMethod http://localhost:8000/stats
Invoke-RestMethod http://localhost:8000/risk-summary
Invoke-RestMethod "http://localhost:8000/predictions?limit=100"
Invoke-RestMethod "http://localhost:8000/patients?limit=100"
```

3. Abrir Power BI Desktop.

4. Usar **Get Data -> Blank Query**.

5. Abrir **Advanced Editor**.

6. Pegar los scripts M de este documento.

7. Crear una consulta por endpoint:
- `Stats`
- `RiskSummary`
- `Predictions`
- `Patients`

8. Aplicar cambios con **Close & Apply**.

9. Crear relaciones si se decide separar dimensiones; para esta fase se puede trabajar con tablas independientes.

10. Crear medidas DAX y visualizaciones.

## 5. Scripts M completos

### 5.1 Parametros base
Crear una consulta llamada `ApiBaseUrl`:

```powerquery
let
    ApiBaseUrl = "http://localhost:8000"
in
    ApiBaseUrl
```

Crear una consulta llamada `GetJson`:

```powerquery
(relativePath as text, optional queryParams as nullable record) as any =>
let
    Source = Json.Document(
        Web.Contents(
            ApiBaseUrl,
            [
                RelativePath = relativePath,
                Query = if queryParams = null then [] else queryParams,
                Timeout = #duration(0, 0, 0, 30)
            ]
        )
    )
in
    Source
```

### 5.2 Stats
Consulta `Stats`:

```powerquery
let
    Source = GetJson("stats", null),
    AsTable = Record.ToTable(Source),
    Pivoted = Table.Pivot(
        AsTable,
        List.Distinct(AsTable[Name]),
        "Name",
        "Value"
    ),
    ExpandedModelVersions = Table.TransformColumns(
        Pivoted,
        {
            {
                "model_versions",
                each if _ is list then Text.Combine(List.Transform(_, Text.From), ", ") else Text.From(_),
                type text
            }
        }
    ),
    Typed = Table.TransformColumnTypes(
        ExpandedModelVersions,
        {
            {"average_probability", type number},
            {"latest_processed_at", type datetime},
            {"total", Int64.Type},
            {"total_no_risk", Int64.Type},
            {"total_risk", Int64.Type},
            {"model_versions", type text}
        }
    )
in
    Typed
```

### 5.3 Risk Summary
Consulta `RiskSummary`:

```powerquery
let
    Source = GetJson("risk-summary", null),
    BaseRecord = Record.RemoveFields(Source, {"top_high_probability"}, MissingField.Ignore),
    BaseTable = Table.FromRecords({BaseRecord}),
    DistributionExpanded = Table.ExpandRecordColumn(
        BaseTable,
        "distribution",
        {"risk", "no_risk"},
        {"distribution_risk", "distribution_no_risk"}
    ),
    Typed = Table.TransformColumnTypes(
        DistributionExpanded,
        {
            {"total_records", Int64.Type},
            {"risk", Int64.Type},
            {"no_risk", Int64.Type},
            {"risk_percentage", type number},
            {"distribution_risk", Int64.Type},
            {"distribution_no_risk", Int64.Type}
        }
    )
in
    Typed
```

Consulta opcional `TopHighProbability`:

```powerquery
let
    Source = GetJson("risk-summary", null),
    Items = Source[top_high_probability],
    TableFromItems = Table.FromList(Items, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(
        TableFromItems,
        "Column1",
        {
            "id",
            "Age",
            "Sex",
            "ChestPainType",
            "RestingBP",
            "Cholesterol",
            "FastingBS",
            "RestingECG",
            "MaxHR",
            "ExerciseAngina",
            "Oldpeak",
            "ST_Slope",
            "HeartDisease",
            "prediction",
            "prediction_label",
            "probability",
            "model_version",
            "processed_at",
            "inserted_at",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "source_file",
            "row_number"
        }
    ),
    Typed = Table.TransformColumnTypes(
        Expanded,
        {
            {"id", type text},
            {"Age", Int64.Type},
            {"Sex", type text},
            {"ChestPainType", type text},
            {"RestingBP", Int64.Type},
            {"Cholesterol", Int64.Type},
            {"FastingBS", Int64.Type},
            {"RestingECG", type text},
            {"MaxHR", Int64.Type},
            {"ExerciseAngina", type text},
            {"Oldpeak", type number},
            {"ST_Slope", type text},
            {"HeartDisease", Int64.Type},
            {"prediction", Int64.Type},
            {"prediction_label", type text},
            {"probability", type number},
            {"model_version", type text},
            {"processed_at", type datetime},
            {"inserted_at", type datetime},
            {"kafka_partition", Int64.Type},
            {"kafka_offset", Int64.Type},
            {"kafka_timestamp", type datetime},
            {"source_file", type text},
            {"row_number", Int64.Type}
        }
    )
in
    Typed
```

### 5.4 Predictions
Consulta `Predictions`:

```powerquery
let
    Limit = "500",
    Source = GetJson("predictions", [limit = Limit, skip = "0"]),
    Items = Source[items],
    TableFromItems = Table.FromList(Items, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(
        TableFromItems,
        "Column1",
        {
            "id",
            "Age",
            "Sex",
            "ChestPainType",
            "RestingBP",
            "Cholesterol",
            "FastingBS",
            "RestingECG",
            "MaxHR",
            "ExerciseAngina",
            "Oldpeak",
            "ST_Slope",
            "HeartDisease",
            "prediction",
            "prediction_label",
            "probability",
            "model_version",
            "processed_at",
            "inserted_at",
            "ingestion_batch_id",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "source_file",
            "row_number"
        }
    ),
    Typed = Table.TransformColumnTypes(
        Expanded,
        {
            {"id", type text},
            {"Age", Int64.Type},
            {"Sex", type text},
            {"ChestPainType", type text},
            {"RestingBP", Int64.Type},
            {"Cholesterol", Int64.Type},
            {"FastingBS", Int64.Type},
            {"RestingECG", type text},
            {"MaxHR", Int64.Type},
            {"ExerciseAngina", type text},
            {"Oldpeak", type number},
            {"ST_Slope", type text},
            {"HeartDisease", Int64.Type},
            {"prediction", Int64.Type},
            {"prediction_label", type text},
            {"probability", type number},
            {"model_version", type text},
            {"processed_at", type datetime},
            {"inserted_at", type datetime},
            {"ingestion_batch_id", Int64.Type},
            {"kafka_partition", Int64.Type},
            {"kafka_offset", Int64.Type},
            {"kafka_timestamp", type datetime},
            {"source_file", type text},
            {"row_number", Int64.Type}
        }
    )
in
    Typed
```

Consulta filtrada por riesgo `PredictionsRisk`:

```powerquery
let
    Source = GetJson("predictions", [limit = "500", skip = "0", prediction_label = "risk"]),
    Items = Source[items],
    TableFromItems = Table.FromList(Items, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(
        TableFromItems,
        "Column1",
        {"id", "Age", "Sex", "ChestPainType", "prediction_label", "probability", "processed_at", "model_version"}
    ),
    Typed = Table.TransformColumnTypes(
        Expanded,
        {
            {"id", type text},
            {"Age", Int64.Type},
            {"Sex", type text},
            {"ChestPainType", type text},
            {"prediction_label", type text},
            {"probability", type number},
            {"processed_at", type datetime},
            {"model_version", type text}
        }
    )
in
    Typed
```

### 5.5 Patients
Consulta `Patients`:

```powerquery
let
    Source = GetJson("patients", [limit = "500", skip = "0"]),
    Items = Source[items],
    TableFromItems = Table.FromList(Items, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
    Expanded = Table.ExpandRecordColumn(
        TableFromItems,
        "Column1",
        {
            "id",
            "Age",
            "Sex",
            "ChestPainType",
            "RestingBP",
            "Cholesterol",
            "FastingBS",
            "RestingECG",
            "MaxHR",
            "ExerciseAngina",
            "Oldpeak",
            "ST_Slope",
            "HeartDisease",
            "source_file",
            "row_number"
        }
    ),
    Typed = Table.TransformColumnTypes(
        Expanded,
        {
            {"id", type text},
            {"Age", Int64.Type},
            {"Sex", type text},
            {"ChestPainType", type text},
            {"RestingBP", Int64.Type},
            {"Cholesterol", Int64.Type},
            {"FastingBS", Int64.Type},
            {"RestingECG", type text},
            {"MaxHR", Int64.Type},
            {"ExerciseAngina", type text},
            {"Oldpeak", type number},
            {"ST_Slope", type text},
            {"HeartDisease", Int64.Type},
            {"source_file", type text},
            {"row_number", Int64.Type}
        }
    )
in
    Typed
```

## 6. Parametrizacion de filtros

La API soporta:
- `limit`
- `skip`
- `prediction_label`

Ejemplo M:

```powerquery
GetJson("predictions", [limit = "100", skip = "0", prediction_label = "risk"])
```

Para paginacion manual:

```powerquery
GetJson("predictions", [limit = "100", skip = "100"])
```

## 7. Autenticacion y errores

La API local actual no requiere autenticacion.

Si en una fase posterior se agrega Bearer Token, `Web.Contents` puede recibir headers:

```powerquery
Web.Contents(
    ApiBaseUrl,
    [
        RelativePath = "predictions",
        Headers = [Authorization = "Bearer " & Token],
        Query = [limit = "100"]
    ]
)
```

Manejo basico de errores en Power Query:

```powerquery
let
    Response = try GetJson("health", null),
    Result = if Response[HasError] then [status = "error"] else Response[Value]
in
    Result
```

Para refresh en Power BI Service con API local:
- usar On-premises data gateway; o
- desplegar la API en un host accesible para Power BI Service.

## 8. Estructura de tablas recomendada

| Tabla | Endpoint | Uso |
|---|---|---|
| `Predictions` | `/predictions` | Tabla principal para visualizaciones y filtros. |
| `Patients` | `/patients` | Vista clinica base sin campos de prediccion. |
| `Stats` | `/stats` | KPI cards agregadas. |
| `RiskSummary` | `/risk-summary` | Distribucion y resumen ejecutivo. |
| `TopHighProbability` | `/risk-summary` | Tabla de casos con mayor probabilidad. |

Tipos recomendados:
- Fechas: `processed_at`, `inserted_at`, `kafka_timestamp` como `datetime`.
- Categorias: `Sex`, `ChestPainType`, `RestingECG`, `ExerciseAngina`, `ST_Slope`, `prediction_label` como texto.
- Numericos: `Age`, `RestingBP`, `Cholesterol`, `FastingBS`, `MaxHR`, `Oldpeak`, `probability`.

## 9. Visualizaciones recomendadas

### Pagina 1: Dashboard general
- KPI card: total de registros.
- KPI card: total `risk`.
- KPI card: total `no_risk`.
- KPI card: promedio de `probability`.
- Barra: conteo por `prediction_label`.
- Pastel o dona: distribucion `risk` vs `no_risk`.

Justificacion:
- Permite ver el estado general del sistema y la proporcion de riesgo de forma inmediata.

### Pagina 2: Predicciones detalladas
- Tabla con `Age`, `Sex`, `ChestPainType`, `prediction_label`, `probability`, `processed_at`, `model_version`.
- Slicers: `prediction_label`, `Sex`, `ChestPainType`, `ST_Slope`.
- Orden por `probability` descendente.

Justificacion:
- Permite inspeccionar casos concretos de mayor riesgo.

### Pagina 3: Estadisticas agregadas
- Tarjetas desde `Stats`.
- Grafico de barras de `total_risk` vs `total_no_risk`.
- Indicador de version del modelo.

Justificacion:
- Da trazabilidad del modelo y volumen procesado.

### Pagina 4: Resumen clinico
- Distribucion de riesgo por edad.
- Promedio de probabilidad por `ChestPainType`.
- Promedio de probabilidad por `Sex`.
- Tabla `TopHighProbability`.

Justificacion:
- Permite explorar atributos clinicos asociados a mayor probabilidad.

## 10. Medidas DAX sugeridas

```DAX
Total Registros = COUNTROWS(Predictions)
```

```DAX
Total Riesgo = CALCULATE(COUNTROWS(Predictions), Predictions[prediction_label] = "risk")
```

```DAX
Total Sin Riesgo = CALCULATE(COUNTROWS(Predictions), Predictions[prediction_label] = "no_risk")
```

```DAX
Proporcion Riesgo = DIVIDE([Total Riesgo], [Total Registros], 0)
```

```DAX
Probabilidad Promedio = AVERAGE(Predictions[probability])
```

```DAX
Max Probabilidad = MAX(Predictions[probability])
```

```DAX
Ultima Prediccion = MAX(Predictions[processed_at])
```

Formato recomendado:
- `Proporcion Riesgo`: porcentaje con 1 o 2 decimales.
- `Probabilidad Promedio`: porcentaje o decimal segun criterio visual.

## 11. Capturas esperadas

No se generaron capturas desde esta sesion porque Power BI Desktop no fue operado aqui.

Evidencias esperadas para cerrar la fase visual:
- Captura de la conexion Power Query a `http://localhost:8000`.
- Captura de tabla `Predictions` cargada.
- Captura de KPI cards.
- Captura de distribucion `risk` vs `no_risk`.
- Captura de tabla de predicciones filtrada.
- Archivo `.pbix` guardado en `dashboard/powerbi/KafkaMed.pbix`.

## 12. Recomendaciones de diseno
- Usar una paleta clinica sobria y legible.
- Priorizar lectura rapida de riesgo y volumen procesado.
- Mantener filtros visibles para `prediction_label`, `Sex` y `ChestPainType`.
- Evitar conectar Power BI directamente a MongoDB; la fuente oficial es la API Flask.
- Mantener texto tecnico minimo dentro del dashboard; la explicacion metodologica debe quedar en el informe.

## 13. Conclusiones
La API Flask de KafkaMed ya expone los endpoints necesarios para alimentar Power BI. Este documento deja los scripts M, estructura de tablas, medidas DAX y diseno recomendado para construir el reporte.

La fase queda lista para construccion visual en Power BI Desktop usando datos reales de la API.

## 14. Estado final
**Pendiente de cierre visual.**

La documentacion tecnica de Power BI esta preparada, pero la fase no debe marcarse como aprobada hasta crear el archivo `.pbix` y adjuntar capturas del dashboard funcionando contra la API Flask.
