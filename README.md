<h1>Databricks Agentic AI Project:<br><i>Simulation Package</i></h1>

*Simulation package for the Databricks agentic AI project.*

> **Repository for ideation and planning**: [`pranavg-platformatory`/`dbx-agentic-ai-project`, **github.com**](https://github.com/pranavg-platformatory/dbx-agentic-ai-project)

---

<h1>Package</h1>

[`warehouse_sim`](./warehouse_sim/)

<h1>Data Store Definition</h1>

[`data_store_definition`](./data_store_definition/)

<h1>Test Notebooks</h1>

[`test_notebooks`](./test_notebooks/)

<h1>Devlog</h1>

[`devlog.md`](./devlog.md)

<h1>Implementation Notes</h1>

**Contents**:

- [Type Checking Specification](#type-checking-specification)
- [Notes on Certain Packages Used](#notes-on-certain-packages-used)
  - [Pydantic](#pydantic)
  - [`dataclasses`](#dataclasses)

## Type Checking Specification
Multiple modules have the following lines...

```py
...

from typing import TYPE_CHECKING

...

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

...
```

The `if TYPE_CHECKING` condition allows us to import `SparkSession` strictly for type annotations (like `def my_func(spark: SparkSession): ...`) without actually loading the module at runtime. This prevents the overhead caused by unnecessary imports (since `SparkSession` would be imported by the script/module importing this module).

> **References**:
>
> - (Usage example): [Source code for pyspark.sql.udtf, **spark.apache.org/docs/latest/api/python/_modules**](https://spark.apache.org/docs/latest/api/python/_modules/pyspark/sql/udtf.html)
> - [*Type Checking in Databricks projects. Huge Pain! Solutions?*, **reddit.com/r/databricks**](https://www.reddit.com/r/databricks/comments/1lkw4zi/type_checking_in_databricks_projects_huge_pain/)

## Notes on Certain Packages Used
### Pydantic
Pydantic is the most widely used data validation library for Python. It enables defining models you can use (and reuse) to verify that data conforms to the format you expect before you store or process it. Pydantic supports several methods for validation, but at its base, the package uses Python type hints to ensure data conforms to a specific type, such as an integer, string, or date. Pydantic's BaseModel is the core component for data validation and settings management in Python, utilizing type annotations to enforce data structure, coerce types, and serialize data. By inheriting from BaseModel, classes automatically gain validation logic, JSON schema generation, and IDE support.

---

**Usage in `warehouse_sim`**:

In this package, Pydantic is used for [`warehouse_sim/config`](./warehouse_sim/config/models.py) in order to enforce type validation in the simulation configuration model (`SimConfig`, which inherits from `BaseModel`), ensuring that bad configuration values (i.e. values with wrong types or constraint mismatches) are caught before the code attempts to write to the env tables.

---

> **References**:
>
> - [*Welcome to Pydantic*, **pydantic.dev/docs/validation/latest**](https://pydantic.dev/docs/validation/latest/get-started/)
> - [*What is Pydantic? Validating Data in Python*, **prefect.io/blog**](https://www.prefect.io/blog/what-is-pydantic-validating-data-in-python)

### `dataclasses`
This module provides a decorator and functions for automatically adding generated special methods such as `__init__()` and `__repr__()` to user-defined classes. The member variables to use in these generated methods are defined using PEP 526 type annotations. For example, this code...

```py
from dataclasses import dataclass

@dataclass
class InventoryItem:
    '''Class for keeping track of an item in inventory.'''
    name: str
    unit_price: float
    quantity_on_hand: int = 0

    def total_cost(self) -> float:
        return self.unit_price * self.quantity_on_hand
```

... will add, among other things, a `__init__()` that looks like:

```py
def __init__(self, name: str, unit_price: float, quantity_on_hand: int = 0):
    self.name = name
    self.unit_price = unit_price
    self.quantity_on_hand = quantity_on_hand
```

**NOTE**:

- This method is automatically added to the class
- It is not directly specified in the `InventoryItem` definition shown above

---

**Usage in `warehouse_sim`**:

- Used in:
    - [`warehouse_sim/agent`](./warehouse_sim/agent/)
    - [`warehouse_sim/engine`](./warehouse_sim/engine/)
- Used for convenience in class definitions

---

> **Reference**: [`dataclasses` - Data Classes, **docs.python.org/3/library**](https://docs.python.org/3/library/dataclasses.html)