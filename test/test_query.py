import pytest

from r3.query import mongo_to_sql

TEST_CASES = [
    (
        {"dataset": "mnist"},
        "metadata->>'$.dataset' = 'mnist'",
    ),
    (
        {"dataset": {"$eq": "mnist"}},
        "metadata->>'$.dataset' = 'mnist'",
    ),
    (
        {"dataset": {"$ne": "mnist"}},
        "metadata->>'$.dataset' != 'mnist'",
    ),
    (
        {"dataset": {"$in": ["mnist", "cifar10"]}},
        "metadata->>'$.dataset' IN ('mnist', 'cifar10')",
    ),
    (
        {"dataset": {"$nin": ["mnist", "cifar10"]}},
        "metadata->>'$.dataset' NOT IN ('mnist', 'cifar10')",
    ),
    (
        {"image_size": 28 },
        "metadata->>'$.image_size' = 28",
    ),
    (
        {"image_size": {"$eq": 28}},
        "metadata->>'$.image_size' = 28",
    ),
    (
        {"image_size": {"$gt": 28}},
        "metadata->>'$.image_size' > 28",
    ),
    (
        {"image_size": {"$gte": 28}},
        "metadata->>'$.image_size' >= 28",
    ),
    (
        {"image_size": {"$lt": 28}},
        "metadata->>'$.image_size' < 28",
    ),
    (
        {"image_size": {"$lte": 28}},
        "metadata->>'$.image_size' <= 28",
    ),
    (
        {"image_size": {"$ne": 28}},
        "metadata->>'$.image_size' != 28",
    ),
    (
        {"image_size": {"$in": [28, 32]}},
        "metadata->>'$.image_size' IN (28, 32)",
    ),
    (
        {"image_size": {"$nin": [28, 32]}},
        "metadata->>'$.image_size' NOT IN (28, 32)",
    ),
    (
        {"dataset": "mnist", "model": "cnn"},
        "(metadata->>'$.dataset' = 'mnist') AND (metadata->>'$.model' = 'cnn')",
    ),
    (
        {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]},
        "(metadata->>'$.dataset' = 'mnist') AND (metadata->>'$.model' = 'cnn')",
    ),
    (
        {"$or": [{"dataset": "mnist"}, {"model": "cnn"}]},
        "(metadata->>'$.dataset' = 'mnist') OR (metadata->>'$.model' = 'cnn')",
    ),
    (
        {"dataset": "mnist", "$or": [{"model": "cnn"}, {"image_size": {"$gt": 28}}]},
        "(metadata->>'$.dataset' = 'mnist') AND "
        "((metadata->>'$.model' = 'cnn') OR (metadata->>'$.image_size' > 28))",
    ),
    (
        {"$and": [
            {"dataset": "mnist"},
            {"$or": [{"model": "cnn"}, {"image_size": {"$gt": 28}}]}
        ]},
        "(metadata->>'$.dataset' = 'mnist') AND "
        "((metadata->>'$.model' = 'cnn') OR (metadata->>'$.image_size' > 28))",
    ),
    (
        {"$or": [
            {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]},
            {"$and": [{"dataset": "cifar10"}, {"model": "resnet"}]},
        ]},
        "((metadata->>'$.dataset' = 'mnist') AND (metadata->>'$.model' = 'cnn')) OR "
        "((metadata->>'$.dataset' = 'cifar10') AND (metadata->>'$.model' = 'resnet'))",
    ),
    (
        {"$not": {"dataset": "mnist"}},
        "NOT (metadata->>'$.dataset' = 'mnist')",
    ),
    (
        {"$not": {"$and": [{"dataset": "mnist"}, {"model": "cnn"}]}},
        "NOT ((metadata->>'$.dataset' = 'mnist') AND (metadata->>'$.model' = 'cnn'))",
    ),
    (
        {"$nor": [{"dataset": "mnist"}, {"model": "cnn"}]},
        "NOT ((metadata->>'$.dataset' = 'mnist') OR (metadata->>'$.model' = 'cnn'))",
    ),
    (
        {"tags": {"$all": ["new", "mnist"]}},
        "EXISTS (SELECT 1 FROM json_each(metadata->>'$.tags') WHERE value = 'new') AND "
        "EXISTS (SELECT 1 FROM json_each(metadata->>'$.tags') WHERE value = 'mnist')",
    ),
    (
        {"tags": {"$all": ["new", 1]}},
        "EXISTS (SELECT 1 FROM json_each(metadata->>'$.tags') WHERE value = 'new') AND "
        "EXISTS (SELECT 1 FROM json_each(metadata->>'$.tags') WHERE value = 1)",
    ),
]


@pytest.mark.parametrize("mongo,sql", TEST_CASES)
def test_mongo_to_sql(mongo, sql):
    assert mongo_to_sql(mongo) == sql
