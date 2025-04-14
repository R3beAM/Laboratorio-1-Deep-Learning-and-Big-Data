# Neural Networks Examples

Here you will find two notebooks: (1) A neural Network from scratch just using Numpy (2) Same version but in Pytorch as a contrast.

## Steps

### 1. Setup Virtual environment

In the root of the proyect create the environment:
``` bash
python3 -m venv venv
```

Then, activate the environment in your Terminal:
``` bash
source venv/bin/activate
```
Make sure your Terminal start with `(venv)`, that means you are inside the virtual environment and you can run the code.

Install the `requirements.txt` for the Kafka example:
``` bash
pip install -r neural_networks/requirements.txt
```

### 2. Install Jupyter Extension in VsCode

Go to Extensions y VsCode, look for "Jupyter" from Microsoft and install it. The VsCode might recommend you to add other extensions, do it so.

### 3. Select the Virtual Environment as Kernel for Jupyter

Jupyter needs a Python Kernel to be able to run the code cells.  At the top right corner select the Kernel corresponding to the virtual environment you just created, should appear as `venv/bin/python`

