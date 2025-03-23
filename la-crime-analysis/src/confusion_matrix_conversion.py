import pandas as pd

# Load confusion matrix
confusion_matrix_df = pd.read_csv(r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\model_training_output_files\confusion_matrix.csv", index_col=0)

# Reset index to make it easier to melt
confusion_matrix_df_reset = confusion_matrix_df.reset_index()

# Melt the dataframe to long format
melted_df = confusion_matrix_df_reset.melt(id_vars=["Actual Crime Type"], var_name="Predicted Crime Type", value_name="Count")

# Filter out rows with zero count
melted_df = melted_df[melted_df["Count"] > 0]

# Export the reshaped dataframe to CSV
melted_df.to_csv(r"C:\Users\Anitta\data-portfolio-projects\la-crime-analysis\output\model_training_output_files\reshaped_confusion_matrix.csv", index=False)

# Optionally, export to Excel
# melted_df.to_excel("reshaped_confusion_matrix.xlsx", index=False)
