# KafkaMed heart disease model report

## Dataset
- Rows: 5
- Columns: Age, RestingBP, Cholesterol, FastingBS, MaxHR, Oldpeak, Sex, ChestPainType, RestingECG, ExerciseAngina, ST_Slope
- Target distribution: {1: 3, 0: 2}
- Train rows: 3
- Test rows: 2
- Test size: 0.4

## Model
- LogisticRegression
- Preprocessing: median imputation + StandardScaler for numeric features, most-frequent imputation + OneHotEncoder for categorical features
- class_weight=balanced

## Metrics
- Accuracy: 0.0
- Precision: 0.0
- Recall: 0.0
- F1: 0.0
- ROC AUC: 0.0

## Limitations
- The local CSV extract is very small, so the metrics are only a technical validation of the pipeline.
- This model is not yet integrated with Spark streaming, MongoDB, Flask, or Power BI.

## Model artifact
- Saved at: `C:\Users\User\Desktop\Big DAta\KafkaMed\ml\models\heart_risk_model.joblib`
