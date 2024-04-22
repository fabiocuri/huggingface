# Hugging Face

Platform: Google Cloud

Project name: OECD AI Policy Observatory

Virtual machine: vm-huggingface

Google Storage: huggingface-data

Bigquery: huggingface

GitHub: https://github.com/oecd-ai-org/huggingface

Frequency: monthly.

Methodology: https://oecd.ai/en/huggingface

General flow:

- Retrieve models and datasets.
- Generates charts.

Output: 

| Type     | Name      | Visualisations |
|----------|-----------|---------|
| BigQuery | Average number of parameters of new AI models | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HUGGINGFACE_N_PARAMS/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=evolution-of-number-of-parameters-from-hugging-face-models) |
| BigQuery | Average training costs of new AI models | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HUGGINGFACE_TRAINING_COSTS/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=average-training-cost-of-new-ai-models-from-hugging-face) |
| BigQuery | Average training costs of training 1 million parameters | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HUGGINGFACE_TRAINING_COSTS/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=average-training-cost-of-new-ai-models-from-hugging-face) |
| BigQuery | Evolution of new AI models | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HUGGINGFACE_MODELS/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=evolution-of-new-ai-models) |
| BigQuery | Evolution of new AI models per intended use | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HUGGINGFACE_INTENDED_USE_N_PARAMS/Sheet1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=evolution-of-new-ai-models-from-hugging-face-per-intended-use) |
| BigQuery | Open source datasets by language and application task | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/OECDAI_HUGGINGFACE_APPLICATION_TASKS_MODELS/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=open-source-datasets-from-hugging-face-by-language-and-application-task) |
| BigQuery | Percentage of open source datasets by language | [Tableau](https://public.tableau.com/app/profile/oecd.ai/viz/HF_LANGUAGES_PERCENTAGE/Dashboard1) [OECD.AI](https://oecd.ai/en/data?selectedArea=ai-models-and-datasets&selectedVisualization=percentage-of-open-source-datasets-from-hugging-face-by-language) |

Note: the first two charts above are very costly, thus it is better to run the download_params.py file separately.
