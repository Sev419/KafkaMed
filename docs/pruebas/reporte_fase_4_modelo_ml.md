# KafkaMed heart disease model report

## Dataset
- Rows: 918
- Columns: Age, RestingBP, Cholesterol, FastingBS, MaxHR, Oldpeak, Sex, ChestPainType, RestingECG, ExerciseAngina, ST_Slope
- Target distribution: {1: 508, 0: 410}
- Train rows: 734
- Test rows: 184
- Test size: 0.2

## Model
- LogisticRegression
- Preprocessing: median imputation + StandardScaler for numeric features, most-frequent imputation + OneHotEncoder for categorical features
- class_weight=balanced

## Metrics
- Accuracy: 0.8967391304347826
- Precision: 0.8878504672897196
- Recall: 0.9313725490196079
- F1: 0.9090909090909091
- ROC AUC: 0.9295791487326638

## Limitations
- The local CSV extract is very small, so the metrics are only a technical validation of the pipeline.
- This model is not yet integrated with Spark streaming, MongoDB, Flask, or Power BI.

## Model artifact
- Saved at: `C:\Users\User\Desktop\Big DAta\KafkaMed\ml\models\heart_risk_model.joblib`
