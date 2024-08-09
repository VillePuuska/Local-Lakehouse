from testcontainers.core.image import DockerImage  # type: ignore
from testcontainers.core.container import DockerContainer  # type: ignore
from testcontainers.core.waiting_utils import wait_for_logs  # type: ignore
from collections.abc import Generator
import time
import requests
import os
import polars as pl
import uuid
import random
import string
import pytest
from typing import Callable
from uchelper import UCClient, Column, DataType


USE_EXISTING_IMAGE_ENV_VAR = "UC_TEST_USE_IMAGE"
IMAGE_NAME_ENV_VAR = "UC_TEST_IMAGE_TAG"

RANDOM_DF_ROWS = 10


@pytest.fixture(scope="package")
def image() -> Generator[str, None, None]:
    env_var = os.getenv(USE_EXISTING_IMAGE_ENV_VAR)
    if env_var is not None and env_var.upper() == "TRUE":
        image_name = os.getenv(IMAGE_NAME_ENV_VAR)
        assert image_name is not None
        yield image_name
        return
    path = os.path.join(*os.path.split(__file__)[:-1])  # path to the current file
    with DockerImage(path=path, tag="uc-catalog-test-image:latest") as img:
        yield str(img)


@pytest.fixture
def client(image: str) -> Generator[UCClient, None, None]:
    with DockerContainer(image).with_exposed_ports(8080) as container:
        _ = wait_for_logs(
            container,
            "###################################################################",
            120.0,
        )

        url = container.get_container_host_ip()
        port = container.get_exposed_port(8080)
        uc_url = "http://" + url + ":" + port

        # Unity Catalog does not log anything after it has started listening to its port
        # and it is not immediately ready after logging its startup message so we might
        # need to wait for it to be ready.
        counter = 0
        while counter <= 5:
            try:
                requests.get(uc_url)
                break
            except:
                if counter == 5:
                    raise Exception(
                        "Unity Catalog failed to be responsive within 5 seconds after logging startup message."
                    )
                time.sleep(1.0)
                counter += 1

        yield UCClient(uc_url=uc_url)


def _random_df() -> pl.DataFrame:
    uuids = [str(uuid.uuid4()) for _ in range(RANDOM_DF_ROWS)]
    ints = [random.randint(0, 10000) for _ in range(RANDOM_DF_ROWS)]
    floats = [random.uniform(0, 10000) for _ in range(RANDOM_DF_ROWS)]
    decimals = [random.uniform(0, 10000) for _ in range(RANDOM_DF_ROWS)]
    strings = [
        "".join(
            random.choices(population=string.ascii_letters, k=random.randint(2, 256))
        )
        for _ in range(RANDOM_DF_ROWS)
    ]
    return pl.DataFrame(
        {
            "id": uuids,
            "ints": ints,
            "floats": floats,
            "decimals": decimals,
            "strings": strings,
        },
        schema={
            "id": pl.String,
            "ints": pl.Int64,
            "floats": pl.Float64,
            "decimals": pl.Decimal(precision=10, scale=5),
            "strings": pl.String,
        },
    )


@pytest.fixture
def random_df() -> Callable[[], pl.DataFrame]:
    return _random_df


@pytest.fixture
def random_df_cols() -> list[Column]:
    return [
        Column(
            name="id",
            data_type=DataType.STRING,
            position=0,
            nullable=False,
        ),
        Column(
            name="ints",
            data_type=DataType.LONG,
            position=1,
            nullable=False,
        ),
        Column(
            name="floats",
            data_type=DataType.DOUBLE,
            position=2,
            nullable=False,
        ),
        Column(
            name="decimals",
            data_type=DataType.DECIMAL,
            type_precision=10,
            type_scale=5,
            position=3,
            nullable=False,
        ),
        Column(
            name="strings",
            data_type=DataType.STRING,
            position=4,
            nullable=False,
        ),
    ]


@pytest.fixture
def random_partitioned_df() -> Callable[[], pl.DataFrame]:
    def _random_partitioned_df() -> pl.DataFrame:
        df = _random_df()
        part1 = random.choices(population=[0, 1, 2], k=df.height)
        part2 = random.choices(population=[0, 1, 2], k=df.height)
        df = df.with_columns(
            [pl.Series(part1).alias("part1"), pl.Series(part2).alias("part2")]
        )
        return df

    return _random_partitioned_df


@pytest.fixture
def random_partitioned_df_cols() -> list[Column]:
    return [
        Column(
            name="id",
            data_type=DataType.STRING,
            position=0,
            nullable=False,
        ),
        Column(
            name="ints",
            data_type=DataType.LONG,
            position=1,
            nullable=False,
        ),
        Column(
            name="floats",
            data_type=DataType.DOUBLE,
            position=2,
            nullable=False,
        ),
        Column(
            name="decimals",
            data_type=DataType.DECIMAL,
            type_precision=10,
            type_scale=5,
            position=3,
            nullable=False,
        ),
        Column(
            name="strings",
            data_type=DataType.STRING,
            position=4,
            nullable=False,
        ),
        Column(
            name="part1",
            data_type=DataType.LONG,
            position=5,
            nullable=False,
            partition_index=0,
        ),
        Column(
            name="part2",
            data_type=DataType.LONG,
            position=6,
            nullable=False,
            partition_index=1,
        ),
    ]
