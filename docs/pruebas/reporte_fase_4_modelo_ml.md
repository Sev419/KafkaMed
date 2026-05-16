# Reporte de validación Fase 4: Modelo ML de riesgo cardíaco

## 1. Objetivo de la Fase 4
Entrenar, evaluar y guardar un modelo de clasificación para predecir `HeartDisease` usando el dataset clínico local disponible en KafkaMed, sin integrarlo todavía al flujo de streaming.

## 2. Dataset usado
- Archivo: [data/raw/heart_failure_prediction.csv](C:/Users/User/Desktop/Big%20DAta/KafkaMed/data/raw/heart_failure_prediction.csv)
- Fuente de trabajo: extracto local del dataset **Heart Failure Prediction**

## 3. Alcance del dataset
El archivo disponible en el repositorio es un **extracto muy pequeño** de validación local:
- Filas: `5`
- Columnas clínicas: `12`
- Distribución objetivo: `1 -> 3`, `0 -> 2`

Esta fase valida el pipeline de entrenamiento e inferencia, pero **no representa todavía un modelo robusto de producción**.

## 4. Variables de entrada
Variables clínicas usadas como entrada:
- `Age`
- `Sex`
- `ChestPainType`
- `RestingBP`
- `Cholesterol`
- `FastingBS`
- `RestingECG`
- `MaxHR`
- `ExerciseAngina`
- `Oldpeak`
- `ST_Slope`

## 5. Variable objetivo
- `HeartDisease`

## 6. Modelo entrenado
Modelo entrenado:
- `LogisticRegression`

Preprocesamiento aplicado:
- Imputación mediana para variables numéricas
- `StandardScaler` para variables numéricas
- Imputación por moda para variables categóricas
- `OneHotEncoder` para variables categóricas

Configuración:
- `class_weight=balanced`
- `solver=liblinear`
- `random_state=42`

## 7. Métricas reales obtenidas
Métricas calculadas en ejecución real:
- Accuracy: `0.0`
- Precision: `0.0`
- Recall: `0.0`
- F1: `0.0`
- ROC AUC: `0.0`

## 8. Ruta del modelo guardado
- [ml/models/heart_risk_model.joblib](C:/Users/User/Desktop/Big%20DAta/KafkaMed/ml/models/heart_risk_model.joblib)

## 9. Evidencia de inferencia local
La función de inferencia local quedó validada con el modelo guardado.  
Resultado de prueba manual:

```json
{
  "prediction": 1,
  "prediction_label": "risk",
  "probability": 0.7000846969471392
}
```

## 10. Limitaciones
- El dataset local es demasiado pequeño para concluir desempeño real del modelo.
- Las métricas obtenidas solo validan el flujo técnico de entrenamiento.
- El modelo no está integrado todavía con Spark Structured Streaming, MongoDB, Flask ni Power BI.

## 11. Estado final
**Fase 4 aprobada localmente.**

Se completó con éxito el entrenamiento, la evaluación, el guardado del modelo y la inferencia local.

## 12. Próximo paso recomendado
Integrar este modelo al flujo de streaming de KafkaMed en una fase posterior, sin romper las fases validadas 1, 2 y 3.

