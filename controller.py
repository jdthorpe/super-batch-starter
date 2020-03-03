from os.path import join, expanduser
import datetime
import pathlib
import joblib
import super_batch

from constants import (
    GLOBAL_CONFIG_FILE,
    TASK_RESOURCE_FILE,
    TASK_OUTPUT_FILE,
    LOCAL_RESOURCE_PATTERN,
    LOCAL_OUTPUT_PATTERN,
)

# ------------------------------
# define constants
# ------------------------------

# The `$name` of our created resources:
NAME = "superbatchtest"

# a local directory where temporary files will be stored:
BATCH_DIRECTORY = expanduser("~/temp/super-batch-test")
pathlib.Path(BATCH_DIRECTORY).mkdir(parents=True, exist_ok=True)

# used to generate unique task names:
_TIMESTAMP = datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S")

# instantiate the batch helper client:
batch_client = super_batch.client(
    POOL_ID=NAME,
    JOB_ID=NAME + _TIMESTAMP,
    POOL_VM_SIZE="STANDARD_A1_v2",
    POOL_NODE_COUNT=0,
    POOL_LOW_PRIORITY_NODE_COUNT=2,
    DELETE_POOL_WHEN_DONE=False,
    BLOB_CONTAINER_NAME=NAME,
    BATCH_DIRECTORY=BATCH_DIRECTORY,
    DOCKER_IMAGE="myusername/sum-of-powers:v1",
    COMMAND_LINE="python /worker.py",
)

# ------------------------------
# build the global parameters
# ------------------------------

# <<< YOUR CODE GOES BELOW >>>
global_parameters = {"power": 3, "size": (10,)}
# <<< YOUR CODE GOES ABOVE >>>

# write the global parameters resource to disk
joblib.dump(global_parameters, join(BATCH_DIRECTORY, GLOBAL_CONFIG_FILE))

# upload the task resource
global_parameters_resource = batch_client.build_resource_file(
    GLOBAL_CONFIG_FILE, GLOBAL_CONFIG_FILE
)

# ------------------------------
# build the batch tasks
# ------------------------------

# <<< YOUR CODE GOES BELOW >>>
SEEDS = (1, 12, 123, 1234)
for i, seed in enumerate(SEEDS):
    parameters = {"seed": seed}
    # <<< YOUR CODE GOES ABOVE >>>

    # write the resource to disk
    local_resource_file = LOCAL_RESOURCE_PATTERN.format(i)
    joblib.dump(parameters, join(BATCH_DIRECTORY, local_resource_file))

    # upload the task resource
    input_resource = batch_client.build_resource_file(
        local_resource_file, TASK_RESOURCE_FILE
    )

    # create an output resource
    output_resource = batch_client.build_output_file(
        LOCAL_OUTPUT_FILE, LOCAL_OUTPUT_PATTERN.format(i)
    )

    # create a task
    batch_client.add_task(
        [input_resource, global_parameters_resource], [output_resource]
    )

# ------------------------------
# run the batch job
# ------------------------------

batch_client.run()

# ------------------------------
# aggregate the results
# ------------------------------

task_results = []
for task in batch_client.tasks:
    task_results.append(joblib.load(task.something))

# <<< YOUR CODE GOES BELOW >>>
print(sum(task_results))
# <<< YOUR CODE GOES ABOVE >>>
