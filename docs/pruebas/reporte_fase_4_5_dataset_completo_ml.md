# Reporte de validación Fase 4.5: Dataset completo y reentrenamiento ML

## 1. Motivo de la microfase
La Fase 4 inicial validó el pipeline técnico de entrenamiento e inferencia usando un extracto local de solo 5 filas. Esa validación fue útil para confirmar la implementación, pero no era suficiente para defender el modelo en un informe académico.

Esta microfase reentrena el modelo sobre el dataset completo para obtener métricas más representativas.

## 2. Limitación del entrenamiento anterior con 5 filas
En la validación inicial:
- Dataset: 5 filas
- Distribución: `1 -> 3`, `0 -> 2`
- Métricas: `accuracy=0.0`, `precision=0.0`, `recall=0.0`, `f1=0.0`, `roc_auc=0.0`

Ese resultado confirmó el flujo técnico, pero no la capacidad predictiva del modelo.

## 3. Dataset usado ahora
- Archivo: [data/raw/heart_failure_prediction.csv](C:/Users/User/Desktop/Big%20DAta/KafkaMed/data/raw/heart_failure_prediction.csv)
- Fuente: dataset completo del problema **Heart Failure Prediction** incorporado al repositorio desde un espejo público de GitHub del dataset original de Kaggle

## 4. Número real de filas
- Filas: `918`

## 5. Columnas validadas
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
- `HeartDisease`

## 6. Distribución de clases
- `HeartDisease = 1`: `508`
- `HeartDisease = 0`: `410`

## 7. Modelo entrenado
- `LogisticRegression`
- Preprocesamiento:
  - imputación mediana en numéricas
  - `StandardScaler` en numéricas
  - imputación por moda en categóricas
  - `OneHotEncoder` en categóricas
- Configuración:
  - `class_weight=balanced`
  - `solver=liblinear`
  - `random_state=42`

## 8. Métricas reales obtenidas
Entrenamiento y evaluación sobre split real `734/184`:
- Accuracy: `0.8967391304347826`
- Precision: `0.8878504672897196`
- Recall: `0.9313725490196079`
- F1: `0.9090909090909091`
- ROC AUC: `0.9295791487326638`

## 9. Comparación breve contra la validación inicial de 5 filas
Comparación resumida:

| Versión | Filas | Accuracy | Precision | Recall | F1 | ROC AUC |
|---|---:|---:|---:|---:|---:|---:|
| Validación técnica inicial | 5 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| Dataset completo | 918 | 0.8967391304347826 | 0.8878504672897196 | 0.9313725490196079 | 0.9090909090909091 | 0.9295791487326638 |

## 10. Limitaciones restantes
- El modelo sigue siendo local y todavía no está integrado al streaming.
- No se ha conectado aún con MongoDB, Flask ni Power BI.
- Aunque las métricas ya son defendibles, aún conviene validar el modelo con más estrategias si el curso lo pide, por ejemplo cross-validation o una partición adicional.

## 11. Estado final
**Aprobado.**

La microfase deja un modelo entrenado con un dataset suficientemente representativo para el informe académico.

## 12. Próximo paso recomendado
Integrar el modelo entrenado al flujo de streaming de KafkaMed en una fase posterior, sin romper las validaciones anteriores ni mezclar dominios.

