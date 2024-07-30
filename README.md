# <img src="resources/info/icon.jpeg" width="5%"> Land Consumption

The Land Consumption Plugin created in partnership with WWF Österreich.

## Preparation

### Python Environment

We use [poetry](https://python-poetry.org) as an environment management system.
Make sure you have it installed.
Apart from some base dependencies, there is only one fixed dependency for you, which is the [climatoology](https://gitlab.gistools.geog.uni-heidelberg.de/climate-action/climatoology) package that holds all the infrastructure functionality.
Make sure you have read-access to the climatoology repository (i.e. you can clone it).

To begin installation run:

```shell
poetry install --no-root
```

and you are now ready to code within your poetry environment.

### Testing

We use [pytest](pytest.org) as testing engine.
Ensure all tests are passing on the unmodified repository by running `poetry run pytest`.

### Linting and formatting

It is important that the code created by the different plugin developers adheres to a certain standard.
We use [ruff](https://docs.astral.sh/ruff/) for linting and formatting the code as part of our pre-commit hooks.
Please activate pre-commit by running `poetry run pre-commit install`.
It will now run automatically before each commit and apply fixes for a variety of lint errors to your code.
Note that we have increased the maximum number of characters per line to be 120 to make better use of large modern displays.

## Docker (for admins and interested devs)

If the infrastructure is reachable you can copy [.env_template](.env_template) to `.env` and then run

```shell
DOCKER_BUILDKIT=1 docker build --secret id=CI_JOB_TOKEN . --tag heigit/land_consumption:devel
docker run --env-file .env --network=host heigit/land_consumption:devel
```

Make sure the cone-token is copied to the text-file named `CI_JOB_TOKEN` that is mounted to the container build process as secret.

To deploy this plugin to the central docker repository run

```shell
DOCKER_BUILDKIT=1 docker build --secret id=CI_JOB_TOKEN . --tag heigit/land_consumption:devel
docker image push heigit/land_consumption:devel
```

### Kaniko

To test the docker build from Kaniko run

```shell
docker run -v ./:/workspace \
    -v ./CI_JOB_TOKEN:/kaniko/CI_JOB_TOKEN \
    gcr.io/kaniko-project/executor:v1.14.0-debug \
    --dockerfile /workspace/Dockerfile.Kaniko \
    --context dir:///workspace/ \
    --no-push
```
