# pytest_sample
 - `test_etl.py` has sample PySpark pytest which can be run on local PySpark instance

### Prerequisites
 - pip install pytest

For VS Code pytest
 - `pip install pandas`
 - `pip install pyarrow`
 - If numpy has AttributeError: `np.NaN` was removed in the NumPy 2.0 release. Use `np.nan` instead issue
   - `pip uninstall numpy`
   - `pip install numpy==1.26.3`