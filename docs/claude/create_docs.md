# create_docs.md

# Complete Guide: Creating Documentation with Sphinx AutoAPI and NumPy Style Docstrings

## Overview

This guide explains how to set up automatic, professional Python documentation using Sphinx AutoAPI with NumPy-style docstrings and the modern Furo theme. The documentation will automatically generate from your Python code's docstrings without manual configuration.

**What you get:**
- Automatic API documentation from your Python code
- Professional, academic-style presentation
- Modern, mobile-responsive design
- Search functionality
- Type hint integration
- Zero manual maintenance

---

## Quick Start (5 Minutes)

### 1. Install Required Packages

```bash
pip install sphinx furo sphinx-autoapi sphinx-autodoc-typehints
```

### 2. Initialize Sphinx

```bash
# Create docs directory
mkdir docs
cd docs

# Initialize Sphinx (answer the prompts)
sphinx-quickstart
```

**Recommended answers:**
- Separate source and build: `y`
- Project name: Your project name
- Author: Your name
- Version: `1.0.0`
- Language: `en`

### 3. Configure Sphinx

Edit `docs/source/conf.py` with this configuration:

```python
import os
import sys

# Add your project to the path
sys.path.insert(0, os.path.abspath('../../'))

# Project information
project = 'Your Project Name'
copyright = '2025, Your Name'
author = 'Your Name'
release = '1.0.0'

# Extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx.ext.intersphinx',
    'sphinx.ext.mathjax',
    'autoapi.extension',
    'sphinx_autodoc_typehints',
]

# AutoAPI Configuration
autoapi_dirs = ['../../your_package_name']  # Path to your Python package
autoapi_type = 'python'
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
]

# NumPy Style Docstrings
napoleon_google_docstring = False
napoleon_numpy_docstring = True
napoleon_use_admonition_for_examples = True
napoleon_use_admonition_for_notes = True
napoleon_use_admonition_for_references = True

# Type Hints
autodoc_typehints = 'description'

# Furo Theme
html_theme = 'furo'
html_title = f"{project} Documentation"

# Theme Customization (optional)
html_theme_options = {
    "sidebar_hide_name": True,
    "light_css_variables": {
        "color-brand-primary": "#1f4e79",
        "color-brand-content": "#1f4e79",
    },
}
```

### 4. Update Index File

Edit `docs/source/index.rst`:

```rst
Welcome to Your Project Documentation
======================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   api/index

Overview
--------

Brief description of your project.

Installation
------------

.. code-block:: bash

   pip install your-project

Indices and Tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
```

### 5. Build Documentation

```bash
cd docs
make html
```

View your docs by opening `docs/build/html/index.html` in a browser.

---

## Writing NumPy Style Docstrings

### Basic Structure

NumPy docstrings follow this format:

```python
def function_name(param1, param2):
    """
    Short one-line summary.
    
    Extended description that can span multiple lines.
    Explain what the function does in detail.
    
    Parameters
    ----------
    param1 : type
        Description of param1
    param2 : type, optional
        Description of param2. Default is value.
        
    Returns
    -------
    return_name : type
        Description of return value
        
    Raises
    ------
    ExceptionType
        When this exception is raised
    """
```

### Complete Examples

#### Function with Full Documentation

```python
from typing import List, Optional, Dict, Any
import numpy as np

def analyze_data(data: np.ndarray, 
                 method: str = 'mean',
                 axis: Optional[int] = None,
                 **kwargs) -> Dict[str, Any]:
    """
    Perform statistical analysis on input data.
    
    This function computes various statistical measures on the input
    dataset using the specified method. Supports both univariate and
    multivariate analysis with automatic handling of missing values.
    
    Parameters
    ----------
    data : numpy.ndarray
        Input data array of shape (n_samples, n_features)
    method : {'mean', 'median', 'std', 'var'}, optional
        Statistical method to apply. Default is 'mean'.
    axis : int or None, optional
        Axis along which to compute statistics. If None, compute
        over flattened array. Default is None.
    **kwargs : dict
        Additional keyword arguments passed to the computation function
        
    Returns
    -------
    results : dict
        Dictionary containing:
        
        * 'value' : float or ndarray
            Computed statistical value
        * 'method' : str
            Method used for computation
        * 'shape' : tuple
            Shape of the result
            
    Raises
    ------
    ValueError
        If method is not recognized or data is empty
    TypeError
        If data is not a numeric array
        
    Examples
    --------
    Compute mean of a 1D array:
    
    >>> data = np.array([1, 2, 3, 4, 5])
    >>> result = analyze_data(data, method='mean')
    >>> print(result['value'])
    3.0
    
    Compute standard deviation along axis:
    
    >>> data = np.random.randn(100, 5)
    >>> result = analyze_data(data, method='std', axis=0)
    >>> print(result['shape'])
    (5,)
    
    Notes
    -----
    The implementation uses numerically stable algorithms for all
    statistical computations. For large datasets (>10^6 elements),
    computation is performed in chunks to optimize memory usage.
    
    See Also
    --------
    preprocess_data : Data preprocessing utilities
    transform_data : Data transformation functions
    
    References
    ----------
    .. [1] Oliphant, T. E. (2006). A guide to NumPy, USA: Trelgol 
           Publishing.
    """
    if data.size == 0:
        raise ValueError("Data array cannot be empty")
        
    if method not in ['mean', 'median', 'std', 'var']:
        raise ValueError(f"Unknown method: {method}")
    
    # Computation logic here
    if method == 'mean':
        value = np.mean(data, axis=axis)
    
    return {
        'value': value,
        'method': method,
        'shape': value.shape if hasattr(value, 'shape') else ()
    }
```

#### Class with Full Documentation

```python
class DataProcessor:
    """
    Advanced data processing and transformation framework.
    
    This class provides a comprehensive suite of tools for data
    preprocessing, transformation, and analysis. Supports multiple
    data formats and processing pipelines with automatic optimization.
    
    Parameters
    ----------
    config : dict, optional
        Configuration parameters for the processor
    verbose : bool, optional
        Enable verbose logging. Default is False.
    n_jobs : int, optional
        Number of parallel jobs. -1 means using all processors.
        Default is 1.
        
    Attributes
    ----------
    config : dict
        Active configuration settings
    data : array_like or None
        Currently loaded dataset
    is_fitted : bool
        Whether the processor has been fitted to data
    transform_history : list
        History of applied transformations
        
    Examples
    --------
    Basic usage:
    
    >>> processor = DataProcessor(verbose=True)
    >>> processor.fit(training_data)
    >>> transformed = processor.transform(new_data)
    
    With custom configuration:
    
    >>> config = {'normalize': True, 'handle_missing': 'mean'}
    >>> processor = DataProcessor(config=config, n_jobs=-1)
    >>> result = processor.fit_transform(data)
    
    Notes
    -----
    The processor automatically detects data types and applies
    appropriate transformations. For optimal performance with
    large datasets, set n_jobs=-1 to use all available CPU cores.
    
    See Also
    --------
    DataValidator : Validate data quality and consistency
    DataAnalyzer : Perform statistical analysis
    """
    
    def __init__(self, 
                 config: Optional[Dict[str, Any]] = None,
                 verbose: bool = False,
                 n_jobs: int = 1) -> None:
        """Initialize the DataProcessor."""
        self.config = config or {}
        self.verbose = verbose
        self.n_jobs = n_jobs
        self.data = None
        self.is_fitted = False
        self.transform_history = []
    
    def fit(self, data: np.ndarray) -> 'DataProcessor':
        """
        Fit the processor to the data.
        
        Learns parameters from the input data required for subsequent
        transformations. This method must be called before transform().
        
        Parameters
        ----------
        data : numpy.ndarray
            Training data of shape (n_samples, n_features)
            
        Returns
        -------
        self : DataProcessor
            Returns self for method chaining
            
        Raises
        ------
        ValueError
            If data has invalid shape or contains non-numeric values
        """
        if data.ndim != 2:
            raise ValueError("Data must be 2-dimensional")
        
        self.data = data
        self.is_fitted = True
        return self
    
    def transform(self, data: np.ndarray) -> np.ndarray:
        """
        Transform data using fitted parameters.
        
        Applies learned transformations to new data. The processor
        must be fitted before calling this method.
        
        Parameters
        ----------
        data : numpy.ndarray
            Data to transform of shape (n_samples, n_features)
            
        Returns
        -------
        transformed : numpy.ndarray
            Transformed data with same shape as input
            
        Raises
        ------
        RuntimeError
            If processor has not been fitted
        ValueError
            If data shape doesn't match fitted data
        """
        if not self.is_fitted:
            raise RuntimeError("Processor must be fitted first")
        
        # Transformation logic
        transformed = data.copy()
        return transformed
```

---

## Project Structure

Your project should be organized like this:

```
your_project/
├── your_package_name/
│   ├── __init__.py
│   ├── module1.py
│   └── module2.py
├── docs/
│   ├── source/
│   │   ├── _static/
│   │   ├── _templates/
│   │   ├── conf.py
│   │   └── index.rst
│   ├── build/
│   └── Makefile
├── tests/
├── requirements.txt
└── README.md
```

---

## Building and Viewing Documentation

### Local Development

```bash
# Standard build
cd docs
make html

# View in browser
open build/html/index.html  # macOS
xdg-open build/html/index.html  # Linux
start build/html/index.html  # Windows
```

### Live Reload (Recommended for Development)

```bash
pip install sphinx-autobuild
cd docs
sphinx-autobuild source build --host 0.0.0.0 --port 8000
```

Then visit http://localhost:8000 - docs auto-refresh on changes!

### Clean Rebuild

```bash
cd docs
make clean
make html
```

---

## NumPy Docstring Sections

### Essential Sections

| Section | Purpose | Required |
|---------|---------|----------|
| Summary | One-line description | Yes |
| Extended | Detailed explanation | Recommended |
| Parameters | Function arguments | If applicable |
| Returns | Return values | If applicable |
| Raises | Exceptions raised | If applicable |

### Optional Sections

| Section | Purpose | When to Use |
|---------|---------|-------------|
| Examples | Usage examples | Always recommended |
| Notes | Additional information | Complex functions |
| See Also | Related functions | Cross-referencing |
| References | Citations | Academic/scientific code |
| Warnings | Important caveats | Dangerous operations |

### Section Syntax

```python
"""
Short summary.

Extended description.

Parameters
----------
param_name : type
    Description

Returns
-------
name : type
    Description

Yields
------
name : type
    Description (for generators)

Raises
------
ExceptionType
    When raised

Warns
-----
WarningType
    When warned

Examples
--------
>>> code_example()
result

Notes
-----
Additional information.

See Also
--------
related_function : Brief description
AnotherClass : Brief description

References
----------
.. [1] Author. "Title." Journal, Year.

Warnings
--------
Important warning message.
"""
```

---

## Type Hints Best Practices

### Always Include Type Hints

```python
from typing import List, Dict, Optional, Union, Tuple, Any

def process(
    data: List[int],
    config: Optional[Dict[str, Any]] = None,
    mode: str = 'fast'
) -> Tuple[List[int], Dict[str, float]]:
    """Process data with configuration."""
    pass
```

### Use typing Module

```python
from typing import (
    List,      # List[int]
    Dict,      # Dict[str, Any]
    Optional,  # Optional[int] = None
    Union,     # Union[int, float]
    Tuple,     # Tuple[int, str]
    Callable,  # Callable[[int], str]
    Any,       # Any type
)
```

### NumPy Types

```python
import numpy as np
from numpy.typing import NDArray

def compute(data: NDArray[np.float64]) -> NDArray[np.float64]:
    """Compute using NumPy arrays."""
    pass
```

---

## Deployment Options

### GitHub Pages (Free)

Create `.github/workflows/docs.yml`:

```yaml
name: Documentation

on:
  push:
    branches: [ main ]

jobs:
  build-docs:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.10'
        
    - name: Install dependencies
      run: |
        pip install sphinx furo sphinx-autoapi sphinx-autodoc-typehints
        
    - name: Build documentation
      run: |
        cd docs
        make html
        
    - name: Deploy to GitHub Pages
      uses: peaceiris/actions-gh-pages@v3
      with:
        github_token: ${{ secrets.GITHUB_TOKEN }}
        publish_dir: ./docs/build/html
```

### Read the Docs (Free)

1. Create `docs/requirements.txt`:
```
sphinx>=5.0.0
furo
sphinx-autoapi
sphinx-autodoc-typehints
```

2. Create `.readthedocs.yaml`:
```yaml
version: 2

build:
  os: ubuntu-22.04
  tools:
    python: "3.10"

sphinx:
  configuration: docs/source/conf.py

python:
  install:
    - requirements: docs/requirements.txt
```

3. Connect your repo at https://readthedocs.org

---

## Tips and Best Practices

### Documentation Writing

1. **Write docstrings first** - Document as you code
2. **Be concise but complete** - One line summary + details
3. **Include examples** - Show typical usage
4. **Document edge cases** - Explain special behavior
5. **Keep it updated** - Review docs with code changes

### Code Organization

1. **Use `__all__`** - Control what's documented
```python
__all__ = ['PublicClass', 'public_function']
```

2. **Module docstrings** - Document each module
```python
"""
Module for data processing utilities.

This module provides functions for cleaning, transforming,
and analyzing data from various sources.
"""
```

3. **Group related functions** - Logical organization helps

### AutoAPI Configuration

```python
# Exclude private members
autoapi_options = [
    'members',
    'undoc-members',
    'show-inheritance',
    'show-module-summary',
]

# Ignore certain modules
autoapi_ignore = ['*/tests/*', '*/migrations/*']

# Customize template directory
autoapi_template_dir = '_templates/autoapi'
```

---

## Troubleshooting

### Common Issues

**Problem:** Module not found during build
```python
# Solution: Fix sys.path in conf.py
sys.path.insert(0, os.path.abspath('../../'))
```

**Problem:** Docstrings not appearing
```python
# Solution: Enable napoleon and check format
napoleon_numpy_docstring = True
```

**Problem:** Type hints cluttering signature
```python
# Solution: Move to description
autodoc_typehints = 'description'
```

**Problem:** Build warnings about missing references
```python
# Solution: Add intersphinx mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3', None),
    'numpy': ('https://numpy.org/doc/stable/', None),
}
```

### Validation

```bash
# Check for broken links
make linkcheck

# Validate docstring coverage
pip install interrogate
interrogate -vv your_package/

# Check documentation coverage
make coverage
```

---

## Summary Checklist

- [ ] Install: `pip install sphinx furo sphinx-autoapi sphinx-autodoc-typehints`
- [ ] Initialize: `cd docs && sphinx-quickstart`
- [ ] Configure: Update `conf.py` with extensions and AutoAPI settings
- [ ] Write: Add NumPy-style docstrings to your code
- [ ] Build: `make html` in docs directory
- [ ] Review: Open `build/html/index.html`
- [ ] Deploy: Set up GitHub Actions or Read the Docs
- [ ] Maintain: Update docstrings with code changes

---

## Additional Resources

- Sphinx Documentation: https://www.sphinx-doc.org
- AutoAPI Guide: https://sphinx-autoapi.readthedocs.io
- NumPy Docstring Guide: https://numpydoc.readthedocs.io
- Furo Theme: https://pradyunsg.me/furo/
- Python Type Hints: https://docs.python.org/3/library/typing.html

---

## Example Requirements File

Create `requirements.txt` for your project:

```
# Documentation
sphinx>=5.0.0
furo>=2023.3.27
sphinx-autoapi>=2.0.0
sphinx-autodoc-typehints>=1.19.0

# Optional but recommended
sphinx-autobuild>=2021.3.14
myst-parser>=0.18.0
```

Install with: `pip install -r requirements.txt`