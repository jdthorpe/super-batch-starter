""" Main entry point for the Azure Batch task worker
"""
import joblib
from constants import GLOBAL_CONFIG_FILE, TASK_INPUTS_FILE, TASK_OUTPUTS_FILE
from task import task

# Read the designated global config and iteration parameter files
global_config = joblib.load(GLOBAL_CONFIG_FILE)
parameters = joblib.load(TASK_INPUTS_FILE)

# Do the actual work
result = task(global_config, parameters)

# Write the results to the designated output file
joblib.dump(result, TASK_OUTPUTS_FILE)
