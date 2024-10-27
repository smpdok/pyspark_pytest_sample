# PySpark Pytest Sample

This repository contains sample PySpark tests using `pytest`. The primary file, `test_etl.py`, demonstrates how to run tests on a local PySpark instance.

## Prerequisites

To get started, make sure you have the following installed:

- [PySpark](https://sparkbyexamples.com/pyspark/how-to-install-and-run-pyspark-on-windows/) (installation guide for Windows)
- `pytest` Python package

You can install `pytest` using pip:

```bash
pip install pytest
```

## Running Tests

To execute individual test files, use the following command:

```bash
pytest <relative_path>/test_file.py
```

### Example

To run the sample test, use:

```bash
pytest tests/test_etl.py
```

## Troubleshooting

### Common Issues

1. **VS Code Testing Module**:
   - If you're using the VS Code Testing module, ensure the following packages are installed. Check the Python Output Panel for any import errors:

     ```bash
     pip install pandas
     pip install pyarrow
     ```

2. **NumPy AttributeError**:
   - If you encounter an `AttributeError` related to `np.NaN` (removed in NumPy 2.0), you can resolve it by downgrading NumPy:

     ```bash
     pip uninstall numpy
     pip install numpy==1.26.3
     ```
