# something

## Usage

1. Check that the example code runs in your current python environment by
    calling `python run_local.py`. This might require installing some
    dependencies with `pip intsall joblib numpy`

1. Update this package with the following:

    * Put the code that will do your work in `task.py`
    * Update the constants in `controller.py`
    * Define your global parameters in `controller.py`.  These are the data that are used in all tasks.
    * Create the tasks by updating the task specific parameters in `controller.py`
    * Aggregate the results in `controller.py`

1. Create the required azure resources by following the instructions on this
    page, preferably including an Azure Docker Registry.

1. Export your python dependencies by calling:

    ```shell
    pip freeze >> requirements.txt
    ```

    and then Build and publish the docker file with:

    ```shell
    # build the docker image locally
    docker build . -t my-worker:v1

    # log in to Azure and the container registry
    az acr login --name myownprivateregistry

    # tag the local image
    docker tag my-worker:v1 myownprivateregistry.azurecr.io/my-worker:v1

    # push the image to the private registry
    docker push myownprivateregistry.azurecr.io/my-worker:v1
    ```

1. Run the batch job by calling:

    ```shell
    python controller.py
    ```
