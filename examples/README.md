# Examples

This folder contains everything you need to learn how to use the framework, from terminal-based CLI execution to interactive Jupyter Notebooks.

We have included some samples ROOT files (`ntuples/`) so you can run these examples out of the box without downloading any massive datasets.

## Directory Overview

* **`run_analysis.sh`**: The main entry point for the CLI tutorial. A commented bash script that runs the entire pipeline (dumping, filling, and merging).
* **`example.ipynb`**: An interactive Jupyter Notebook demonstrating the programmatic Python API, Dask-Awkward lazy evaluation, and `mplhep` plotting.
* **`yamls/`**: Contains the declarative configuration files (`run_config.yaml`, `filesets.yaml`, etc.) that steer the framework.
* **`*.py` files**: User-defined logic files (`cuts.py`, `histograms.py`, `preprocessors.py`) that are dynamically loaded by the YAML configurations.
* **`ntuples/`**: Sample datasets (`DY` and `Zprime`) used for testing and tutorials.


## 1. The CLI Workflow (`run_analysis.sh`)

The bash script demonstrates the two primary ways to run the YDANA-HEP CLI. 

**Important:** You must run all commands from inside this `examples/` directory so the framework can correctly locate the local python modules!

```bash
cd examples/
```

### Scenario A: Driven by YAML Filesets (Default)
This is the standard workflow. The framework will read `yamls/filesets.yaml` to discover the datasets (DY and Zprime) and process them automatically.

To run it step-by-step:
```bash
./run_analysis.sh --filesets
```

### Scenario B: Explicit CLI Inputs
Sometimes you just want to test a single folder of ROOT files without writing a full `filesets.yaml` configuration. You can override the datasets dynamically using the `--inputs` flag.

To test the explicit inputs feature:
```bash
./run_analysis.sh --inputs
```

### Automation options
By default, the script pauses between major steps (dumping, filling, merging) so you can read the terminal output. If you want to run the entire pipeline without interruptions, add the `--no-pause` flag:
```bash
./run_analysis.sh --filesets --no-pause
```


## 2. The Interactive API (`example.ipynb`)

If you want to use YDANA-HEP inside your own Python scripts, custom batch submission pipelines, or just want to make plots on the fly, check out the Jupyter Notebook!

Fire up your Jupyter server and open `example.ipynb`:
```bash
jupyter notebook example.ipynb
```
The notebook covers:
* Loading lazy `dask_awkward` arrays.
* Applying array-agnostic cuts and preprocessors.
* Filling YDANA-HEP `Histogram` wrappers lazily.
* Plotting publication-ready visuals with `mplhep`.
