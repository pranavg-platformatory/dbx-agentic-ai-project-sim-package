<h1>Databricks Agentic AI Project:<br><i>Simulation Package</i></h1>

*Simulation package for the Databricks agentic AI project.*

<h1>Package</h1>
[`warehouse_sim`](./warehouse_sim/)

<h1>Devlog</h1>
[`devlog.md`](./devlog.md)

<h1>Implementation Notes</h1>

**Contents**:

- [Notes on Certain Packages Used](#notes-on-certain-packages-used)
  - [Pydantic](#pydantic)

## Notes on Certain Packages Used
### Pydantic
Pydantic is the most widely used data validation library for Python. It enables defining models you can use (and reuse) to verify that data conforms to the format you expect before you store or process it. Pydantic supports several methods for validation, but at its base, the package uses Python type hints to ensure data conforms to a specific type, such as an integer, string, or date.

> **References**:
>
> - [*Welcome to Pydantic*, **pydantic.dev/docs/validation/latest**](https://pydantic.dev/docs/validation/latest/get-started/)
> - [*What is Pydantic? Validating Data in Python*, **prefect.io/blog**](https://www.prefect.io/blog/what-is-pydantic-validating-data-in-python)