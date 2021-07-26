# Irish Building Stock Generator (IBSG)

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://share.streamlit.io/energy-modelling-ireland/ibsg/main/app.py)

Generate a standardised building stock at postcode or small area level in your browser.

If you have any problems or questions:
- Chat with us on [Gitter](https://gitter.im/energy-modelling-ireland/ibsg)
- Or raise an issue on our [Github](https://github.com/energy-modelling-ireland/ibsg) 

## Setup

- (Recommended) Setup & activate a local virtual environment:
    - `conda` via [Anaconda](https://www.anaconda.com/products/individual), [miniconda](https://docs.conda.io/en/latest/miniconda.html) or [miniforge](https://github.com/conda-forge/miniforge)
    - or [`poetry`](https://github.com/python-poetry/poetry)
    - or `virtualenv` (no installation required, see [guide](https://pythonbasics.org/virtualenv/))

- Install locally:

```bash
pip install .
```

or
```bash
conda create -f ibsg.environment.yml
```

or for developers
```bash
conda create -f ibsg.environment.env.yml
```

- Create folder/file `.streamlit/secrets.toml` and add:

```toml
STREAMLIT_SHARING=""
```

- Run:

```bash
streamlit run app.py
```