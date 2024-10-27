# pyspark_pytest_sample
 - `test_etl.py` has sample PySpark pytest which can be run on local PySpark instance

### Prerequisites
 - install PySpark on Windows
 - pip install pytest

### Pytest Steps:
 - To test individual test files: 
```bash
pytest <relative_path>\test_file.py
```
Eg:
```bash
pytest tests\test_etl.py
```

### Error Fix:
  - To test using VS Code Testing module we have to install the following packages if not already present, Look at the Python Output Panel to find any import errors:
    - `pip install pandas`
    - `pip install pyarrow`

 - If numpy has AttributeError: `np.NaN` was removed in the NumPy 2.0 release. Use `np.nan` instead issue
   - `pip uninstall numpy`
   - `pip install numpy==1.26.3`