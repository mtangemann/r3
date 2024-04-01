"""Converts MongoDB query documents to SQL"""

import abc
from dataclasses import dataclass
from typing import Any, Dict, List


def mongo_to_sql(query: Dict[str, Any]) -> str:
    """Converts a MongoDB query document to a SQL query."""
    return Query.from_mongo(query).to_sql()


class Query(abc.ABC):
    @staticmethod
    def from_mongo(query: Dict[str, Any]) -> "Query":
        """Creates a query from a MongoDB query document."""
        if len(query) > 1:
            return AndQuery([
                Query.from_mongo({key: value}) for key, value in query.items()
            ])
    
        key, value = next(iter(query.items()))

        if key == "$and":
            assert isinstance(value, list)
            return AndQuery([
                Query.from_mongo(subquery) for subquery in value
            ])
        
        if key == "$or":
            assert isinstance(value, list)
            return OrQuery([
                Query.from_mongo(subquery) for subquery in value
            ])
    
        if key == "$not":
            return NotQuery(Query.from_mongo(value))
    
        if key == "$nor":
            assert isinstance(value, list)
            return NorQuery([
                Query.from_mongo(subquery) for subquery in value
            ])
    
        if key.startswith("$"):
            raise ValueError(f"Unsupported operator: {key}")
    
        return FieldQuery(key, Condition.from_mongo(value))

    @abc.abstractmethod
    def to_sql(self) -> str:
        """Converts the query to a SQL query."""
        pass


@dataclass
class AndQuery(Query):
    queries: List[Query]

    def to_sql(self) -> str:
        return " AND ".join(f"({query.to_sql()})" for query in self.queries)


@dataclass
class OrQuery(Query):
    queries: List[Query]

    def to_sql(self) -> str:
        return " OR ".join(f"({query.to_sql()})" for query in self.queries)


@dataclass
class NotQuery(Query):
    query: Query

    def to_sql(self) -> str:
        return f"NOT ({self.query.to_sql()})"


@dataclass
class NorQuery(Query):
    queries: List[Query]

    def to_sql(self) -> str:
        return f"NOT ({OrQuery(self.queries).to_sql()})"


@dataclass
class FieldQuery(Query):
    field: str
    condition: "Condition"

    def to_sql(self) -> str:
        """Converts the field query to a SQL query."""
        return self.condition.to_sql(f"metadata->>'$.{self.field}'")


class Condition(abc.ABC):
    @abc.abstractmethod
    def to_sql(self, field: str) -> str:
        """Converts the condition to a SQL query."""
        pass

    @staticmethod
    def from_mongo(value: Any) -> "Condition":
        if isinstance(value, dict):
            if len(value) != 1:
                raise ValueError(f"Invalid condition: {value}")
            
            key, value = next(iter(value.items()))

            if key == "$eq":
                return Eq(value)
            if key == "$ne":
                return Ne(value)
            if key == "$in":
                return In(value)
            if key == "$nin":
                return Nin(value)
            if key == "$gt":
                return Gt(value)
            if key == "$gte":
                return Gte(value)
            if key == "$lt":
                return Lt(value)
            if key == "$lte":
                return Lte(value)

        return Eq(value)


@dataclass
class Eq(Condition):
    """Equality condition ($eq, implicit)."""
    value: Any

    def to_sql(self, field: str) -> str:
        if isinstance(self.value, str):
            return f"{field} = '{self.value}'"
        else:
            return f"{field} = {self.value}"


@dataclass
class Ne(Condition):
    """Inequality condition ($ne)."""
    value: Any

    def to_sql(self, field: str) -> str:
        if isinstance(self.value, str):
            return f"{field} != '{self.value}'"
        else:
            return f"{field} != {self.value}"


@dataclass
class In(Condition):
    """In condition ($in)."""
    values: List[Any]

    def to_sql(self, field: str) -> str:
        values = [
            f"'{value}'" if isinstance(value, str) else str(value)
            for value in self.values
        ]
        return f"{field} IN ({', '.join(values)})"


@dataclass
class Nin(Condition):
    """Not in condition ($nin)."""
    values: List[Any]

    def to_sql(self, field: str) -> str:
        values = [
            f"'{value}'" if isinstance(value, str) else str(value)
            for value in self.values
        ]
        return f"{field} NOT IN ({', '.join(values)})"


@dataclass
class Gt(Condition):
    """Greater than condition ($gt)."""
    value: Any

    def to_sql(self, field: str) -> str:
        return f"{field} > {self.value}"


@dataclass
class Gte(Condition):
    """Greater than or equal condition ($gte)."""
    value: Any

    def to_sql(self, field: str) -> str:
        return f"{field} >= {self.value}"


@dataclass
class Lt(Condition):
    """Less than condition ($lt)."""
    value: Any

    def to_sql(self, field: str) -> str:
        return f"{field} < {self.value}"


@dataclass
class Lte(Condition):
    """Less than or equal condition ($lte)."""
    value: Any

    def to_sql(self, field: str) -> str:
        return f"{field} <= {self.value}"
