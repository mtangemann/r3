import json
import sqlite3
from typing import Any, Dict, Set

import pytest

from r3.query import Condition, mongo_to_sql


@pytest.fixture
def database(tmp_path):
    path = tmp_path / "database.sqlite"
    
    connection = sqlite3.connect(path)
    cursor = connection.cursor()

    cursor.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, metadata JSON NOT NULL)")

    datasets = ["mnist", "cifar10"]
    models = ["cnn", "resnet"]
    image_sizes = [16, 28, 32]

    for dataset in datasets:
        for model in models:
            for image_size in image_sizes:
                cursor.execute(
                    "INSERT INTO jobs (id, metadata) VALUES (?, ?)",
                    (f"{dataset}-{model}-{image_size}", json.dumps({
                        "dataset": dataset,
                        "model": model,
                        "image_size": image_size,
                        "tags": [model, dataset, f"{model}/{dataset}", image_size],
                    }))
                )
    
    connection.commit()
    connection.close()

    return path


QUERY_TEST_CASES = [
    (
        {"dataset": "mnist"},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32"},
    ),
    (
        {"dataset": {"$eq": "mnist"}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32"},
    ),
    (
        {"dataset": {"$ne": "mnist"}},
        {"cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"dataset": {"$in": ["mnist", "cifar10"]}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"dataset": {"$nin": ["mnist", "cifar10"]}},
        set(),
    ),
    (
        {"image_size": 28 },
        {"mnist-cnn-28", "mnist-resnet-28", "cifar10-cnn-28", "cifar10-resnet-28"},
    ),
    (
        {"image_size": {"$eq": 28}},
        {"mnist-cnn-28", "mnist-resnet-28", "cifar10-cnn-28", "cifar10-resnet-28"},
    ),
    (
        {"image_size": {"$gt": 28}},
        {"mnist-cnn-32", "mnist-resnet-32", "cifar10-cnn-32", "cifar10-resnet-32"},
    ),
    (
        {"image_size": {"$gte": 28}},
        {"mnist-cnn-28", "mnist-resnet-28", "mnist-cnn-32", "mnist-resnet-32",
         "cifar10-cnn-28", "cifar10-resnet-28", "cifar10-cnn-32", "cifar10-resnet-32"},
    ),
    (
        {"image_size": {"$lt": 28}},
        {"mnist-cnn-16", "mnist-resnet-16", "cifar10-cnn-16", "cifar10-resnet-16"},
    ),
    (
        {"image_size": {"$lte": 28}},
        {"mnist-cnn-16", "mnist-resnet-16", "mnist-cnn-28", "mnist-resnet-28",
         "cifar10-cnn-16", "cifar10-resnet-16", "cifar10-cnn-28", "cifar10-resnet-28"},
    ),
    (
        {"image_size": {"$ne": 28}},
        {"mnist-cnn-16", "mnist-resnet-16", "mnist-cnn-32", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-resnet-16", "cifar10-cnn-32", "cifar10-resnet-32"},
    ),
    (
        {"image_size": {"$in": [16, 32]}},
        {"mnist-cnn-16", "mnist-resnet-16", "mnist-cnn-32", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-resnet-16", "cifar10-cnn-32", "cifar10-resnet-32"},
    ),
    (
        {"image_size": {"$nin": [28, 32]}},
        {"mnist-cnn-16", "mnist-resnet-16", "cifar10-cnn-16", "cifar10-resnet-16"},
    ),
    (
        {"model": {"$glob": "res*"}},
        {"mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"dataset": {"$glob": "*i*"}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"dataset": "mnist", "model": "cnn"},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32"},
    ),
    (
        {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32"},
    ),
    (
        {"$or": [{"dataset": "mnist"}, {"model": "cnn"}]},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32"},
    ),
    (
        {"dataset": "mnist", "$or": [{"model": "cnn"}, {"image_size": {"$gt": 28}}]},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32", "mnist-resnet-32"},
    ),
    (
        {"$and": [
            {"dataset": "mnist"},
            {"$or": [{"model": "cnn"}, {"image_size": {"$gt": 28}}]}
        ]},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32", "mnist-resnet-32"},
    ),
    (
        {"$or": [
            {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]},
            {"$and": [{"dataset": "cifar10"}, {"model": "resnet"}]},
        ]},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"$not": {"dataset": "mnist"}},
        {"cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"$not": {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]}},
        {"mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"$nor": [{"dataset": "mnist"}, {"model": "cnn"}]},
        {"cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"tags": {"$all": ["mnist", "cnn"]}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32"},
    ),
    (
        {"tags": "mnist"},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32"},
    ),
    (
        {"tags": {"$ne": "mnist"}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32",
         "cifar10-resnet-16", "cifar10-resnet-28", "cifar10-resnet-32"},
    ),
    (
        {"tags": {"$in": ["mnist", "imagenet"]}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "mnist-resnet-16", "mnist-resnet-28", "mnist-resnet-32"},
    ),
    (
        {"tags": {"$glob": "cnn/*"}},
        {"mnist-cnn-16", "mnist-cnn-28", "mnist-cnn-32",
         "cifar10-cnn-16", "cifar10-cnn-28", "cifar10-cnn-32"},
    ),
    (
        {"tags": {"$all": ["mnist", 28]}},
        {"mnist-cnn-28", "mnist-resnet-28"},
    ),
    (
        {"tags": {"$all": ["mnist", 1]}},
        set(),
    ),
    (
        {"tags": {"$all": ["mnist", "28"]}},
        set(),
    ),
    (
        {"tags": {"$elemMatch": {"$gt": 16, "$lt": 32}}},
        {"mnist-cnn-28", "mnist-resnet-28", "cifar10-cnn-28", "cifar10-resnet-28"},
    ),
]
@pytest.mark.parametrize("query,ids", QUERY_TEST_CASES)
def test_mongo_to_sql(database: str, query: Dict[str, Any], ids: Set[str]):
    connection = sqlite3.connect(database)
    cursor = connection.cursor()

    cursor.execute(f"SELECT id FROM jobs WHERE {mongo_to_sql(query)}")
    results = set(result[0] for result in cursor.fetchall())

    assert results == ids


CONDITION_TEST_CASES = [
    ("mnist",                        "field = 'mnist'"),
    ({"$eq": "mnist"},               "field = 'mnist'"),
    ({"$ne": "mnist"},               "field != 'mnist'"),
    ({"$in": ["mnist", "cifar10"]},  "field IN ('mnist', 'cifar10')"),
    ({"$nin": ["mnist", "cifar10"]}, "field NOT IN ('mnist', 'cifar10')"),
    (28 ,                            "field = 28"),
    ({"$eq": 28},                    "field = 28"),
    ({"$gt": 28},                    "field > 28"),
    ({"$gte": 28},                   "field >= 28"),
    ({"$lt": 28},                    "field < 28"),
    ({"$lte": 28},                   "field <= 28"),
    ({"$ne": 28},                    "field != 28"),
    ({"$in": [28, 32]},              "field IN (28, 32)"),
    ({"$nin": [28, 32]},             "field NOT IN (28, 32)"),
    ({"$glob": "resnet/*"},          "field GLOB 'resnet/*'"),
    (
        {"$all": ["new", "mnist"]},
        "EXISTS (SELECT 1 FROM json_each(field) WHERE value = 'new') AND "
        "EXISTS (SELECT 1 FROM json_each(field) WHERE value = 'mnist')",
    ),
    (
        {"$all": ["new", 1]},
        "EXISTS (SELECT 1 FROM json_each(field) WHERE value = 'new') AND "
        "EXISTS (SELECT 1 FROM json_each(field) WHERE value = 1)",
    ),
    (
        {"$elemMatch": {"$gt": 28, "$lt": 32}},
        "EXISTS (SELECT 1 FROM json_each(field) WHERE value > 28 AND value < 32)",
    ),
]
@pytest.mark.parametrize("mongo,sql", CONDITION_TEST_CASES)
def test_condition_to_sql(mongo, sql):
    condition = Condition.from_mongo(mongo)
    assert condition.to_sql("field") == sql
