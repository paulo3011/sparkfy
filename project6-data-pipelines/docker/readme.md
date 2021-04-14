# Building the airflow custom image

1. Clone the github official repository

```shell
# enter to your preferred path
cd ~/projects/external
# clone the repo
git clone https://github.com/apache/airflow.git
```

2. Checkout to the stable version that you want

```shell
git checkout v2-0-stable
```

3. Enter on airflow folder

```shell
cd ~/projects/external/airflow
```

4. Check if is up to date

```shell
git status
# The result needs to be:
# On branch v2-0-stable
# Your branch is up to date with 'origin/v2-0-stable'.
```

5. Build your build command with your customizations like:

```shell
docker build . \
  --build-arg PYTHON_BASE_IMAGE="python:3.8-slim-buster" \
  --build-arg AIRFLOW_PIP_VERSION=21.0.1 \
  --build-arg PYTHON_MAJOR_MINOR_VERSION=3.8 \
  --build-arg AIRFLOW_INSTALLATION_METHOD="apache-airflow" \
  --build-arg AIRFLOW_VERSION="2.0.1" \
  --build-arg AIRFLOW_INSTALL_VERSION="==2.0.1" \
  --build-arg AIRFLOW_CONSTRAINTS_REFERENCE="constraints-2-0" \
  --build-arg AIRFLOW_SOURCES_FROM="empty" \
  --build-arg AIRFLOW_SOURCES_TO="/empty" \
  --build-arg INSTALL_MYSQL_CLIENT="false" \
  --build-arg AIRFLOW_EXTRAS="amazon,http,postgres,sqlite" \
  --tag moreira-udacity-airlfow-v2
```

Notes about configurations used:

- AIRFLOW_VERSION:

    This is airflow version that is put in the label of the image build

- AIRFLOW_INSTALL_VERSION?

    By default latest released version of airflow is installed (when empty) but this value can be overridden and we can install specific version of airflow this way.

    sample: --build-arg AIRFLOW_INSTALL_VERSION="==2.0.1" \

- AIRFLOW_EXTRAS:

    This is where we define additional softwares that needs to be install with airflow.
    Seealso: https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html#

- AIRFLOW_CONSTRAINTS_REFERENCE

    This configuration is used to set the url:
    https://raw.githubusercontent.com/apache/airflow/constraints-2-0/constraints-3.7.txt

- AIRFLOW_INSTALLATION_METHOD:

    Determines the way airflow is installed. By default we install airflow from PyPI `apache-airflow` package But it also can be `.` from local installation or GitHub URL pointing to specific branch or tag Of Airflow.

    Note That __for local source installation__ you need to have local sources of Airflow checked out together with the Dockerfile and AIRFLOW_SOURCES_FROM and AIRFLOW_SOURCES_TO set to "." and "/opt/airflow" respectively.

- INSTALL_FROM_DOCKER_CONTEXT_FILES:

    We can seet this value to true in case we want to install .whl .tar.gz packages placed in the docker-context-files folder. This can be done for both - additional packages you want to install and for airflow as well (you have to set INSTALL_FROM_PYPI to false in this case)

# References

## CI image build arguments

- https://github.com/apache/airflow/blob/v2-0-stable/IMAGES.rst
- https://github.com/apache/airflow/blob/master/IMAGES.rst

## Others

- https://www.youtube.com/watch?v=wDr3Y7q2XoI
- https://airflow.apache.org/docs/apache-airflow/stable/production-deployment.html#production-container-images
- https://airflow.apache.org/docs/apache-airflow/stable/extra-packages-ref.html
