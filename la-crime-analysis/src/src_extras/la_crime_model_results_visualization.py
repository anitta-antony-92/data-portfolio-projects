import pandas as pd
import json
import joblib
import numpy as np

# Paths to existing output files
evaluation_metrics_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\deployment_model\model_evaluation_metrics.json"
model_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\deployment_model\model.pkl"

# Load evaluation metrics from JSON
with open(evaluation_metrics_path, "r") as file:
    eval_metrics = json.load(file)

# Extract confusion matrix
conf_matrix = np.array(eval_metrics["confusion_matrix"])

# Load trained model and extract crime type labels
model = joblib.load(model_path)
model_labels = sorted(set(model.classes_))  # Ensure sorted & unique labels

# Extract labels from confusion matrix
cm_labels = model_labels[:conf_matrix.shape[0]]  # Trim labels if necessary

# Convert confusion matrix into DataFrame
cm_df = pd.DataFrame(conf_matrix, index=cm_labels, columns=cm_labels)

# Ensure all labels from the model exist in the confusion matrix
for label in model_labels:
    if label not in cm_df.index:
        cm_df.loc[label] = 0  # Add missing labels as rows with zero values
    if label not in cm_df.columns:
        cm_df[label] = 0  # Add missing labels as columns with zero values

# Reorder DataFrame to match model label order
cm_df = cm_df.reindex(index=model_labels, columns=model_labels)

# Reset index for Tableau compatibility
cm_df.reset_index(inplace=True)
cm_df.rename(columns={'index': 'Actual Crime Type'}, inplace=True)

# Save confusion matrix as CSV
output_path = r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\model_training_output_files\confusion_matrix.csv"
cm_df.to_csv(output_path, index=False)

print(f"Confusion Matrix CSV saved successfully at {output_path}!")
print(f"Final Confusion Matrix Shape: {cm_df.shape}")

# Extract classification report metrics
report = eval_metrics["classification_report"]
labels = list(report.keys())[:-3]  # Ignore summary rows like 'accuracy', 'macro avg', 'weighted avg'

# Create a DataFrame
metrics_df = pd.DataFrame({
    "Crime Type": labels,
    "Precision": [report[label]["precision"] for label in labels],
    "Recall": [report[label]["recall"] for label in labels],
    "F1-Score": [report[label]["f1-score"] for label in labels]
})

# Save classification metrics as CSV
metrics_df.to_csv(r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\model_training_output_files\classification_metrics.csv", index=False)
