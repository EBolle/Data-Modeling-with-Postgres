## Leverage the Postgres music database

This project contains notebooks and scripts that do the following:
- Create a Postgres table based on .json files containing song and log data 
- Run simple queries against this newly created database

### Install instructions

Before you can run the scripts or work with the project interactively through the notebooks there 2 steps you need to take:
- Activate a virtual environment
- Populate it with required packages

#### Anaconda

Open a terminal, change your directory to this project, and enter the following code. If you want to play around in the notebook I suggest you install jupyter in the 
environment as well. As a default it is commented out.

```bash
conda env create -f environment.yml
```

Next, activate the conda environment.

```bash
conda activate postgres
```

#### Pip

Open a terminal, change your directory to this project, and enter the following:

```bash
python -m venv venv
```

This line of code executes the venv module and created a new folder called `venv`. Next, activate the environment.

```bash
venv\Scripts\activate
```

Your terminal prefix should now have changed from (base) to (venv) (base). Finally, install all required packages
in the new virtual environment. If you want to play around in the notebook I suggest you install jupyter in the 
environment as well. As a default it is commented out.

```bash
pip install -r requirements.txt
```

### Entry point

### Contact

### For reviewers 


