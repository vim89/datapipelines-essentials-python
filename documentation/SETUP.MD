# Datalake ETL Pipeline
Data transformation simplified for any Data platform.

`Features:` The package has complete ETL process - 
1. Uses metadata, transformation & data model information to design ETL pipeline
2. Builds target transformation using both SparkSQL and Spark Dataframes
   \- Developer to choose option
3. Builds source & target Hive DDLs
4. Validates DataFrames, extends core classes, defines DataFrame transformations, and provides UDF SQL functions.
5. Supports below fundamental transformations for ETL pipeline -
   * Filters on source & target dataframes
   * Grouping and Aggregations on source & target dataframes
   * Heavily nested queries / dataframes
6. Has complex and heavily nested XML, JSON, Parquet & ORC parser to nth
   level of nesting
7. Has Unit test cases designed on function/method level & measures
  source code coverage
8. Has information about delpoying to higher environments
9. Has API documentation for customization & enhancement

`Enhancements:` In progress -
1. Integrate Audit and logging - Define Error codes, log process failures, Audit progress & runtime information

## Setup for Python and Pyspark on Windows & Linux machines locally

First please install 64 bit JDK 1.8 or 8 for your operating system from Oracle https://www.oracle.com/java/technologies/javase-jdk8-downloads.html
set below below environment variable pointing to home directory of java (Please remember the path mentioned below may be different for you)
```
JAVA_HOME=C:\Program Files\Java\jdk1.8.0_231
```

Next, Install PyCharm community edition from https://www.jetbrains.com/pycharm/download/


Make sure you have Python >= 3.0 and Do not use Python 3.8; Recommended is Python 3.7 and PySpark 2.3.4
If you do not have pip tool, get it from https://pypi.org/project/pip/ and execute below command
**If you already have Python & pip installed skip the below step** 
```
python get-pip.py
```

Once you have Python and pip follow steps below -
1. Install below libraries for virtual environment (Linux)
   ```
   pip install virtualenvwrapper
   ```
   Windows -
   ```
   pip install virtualenvwrapper-win
   ```   
   Set up the working directory for virtual environments (Linux)
   ```
   export WORKON_HOME=~/Envs
   mkdir -p $WORKON_HOME
   source /usr/local/bin/virtualenvwrapper.sh
   ```
   Windows
   ```
   Create directory C:\Users\<username>\Envs
   WORKON_HOME=C:\Users\<username>\Envs
   ```   

2. Setup below **ENVIRONMENT VARIABLES**
    
    **Unix / Linux / Mac**
    
    `Please note: Your computer path may vary, use your computer path in below given in example `
    - **PYTHONPATH** - Full path to python executable
        ```
        export PYTHONPATH=/usr/python37
        ```
    - **PATH** - Update PATH variable add PYTHONPATH
        ```
        export PATH=$PATH%:$PYTHONPATH
        ```
    - **VIRTUALENV_PYTHON** - To create virtual environments. Path is same as PYTHONPATH
        ```
        export VIRTUALENV_PYTHON=$PYTHONPATH
        ```
    - **VIRTUALENVWRAPPER_VIRTUALENV** - Wrapper for Virtual Environment tools. Path is Scripts folder under PYTHONPATH 
        ```
        VIRTUALENVWRAPPER_VIRTUALENV=$PYTHONPATH/Scripts
        ```
    **Windows**
    
    `Please note: Your computer path may vary, use your computer path in below given in example `
    - **PYTHONPATH** - Full path to python.exe
        ```
        PYTHONPATH=C:\Program Files\Python37
        ```
    - **PATH** - Update PATH variable add PYTHONPATH
        ```
        PATH=%PATH%;%PYTHONPATH%
        ```
    - **VIRTUALENV_PYTHON** - To create virtual environments. Path is same as PYTHONPATH
        ```
        VIRTUALENV_PYTHON=%PYTHONPATH%
        ```
    - **VIRTUALENVWRAPPER_VIRTUALENV** - Wrapper for Virtual Environment tools. Path is Scripts folder under PYTHONPATH 
        ```
        VIRTUALENVWRAPPER_VIRTUALENV=%PYTHONPATH%\Scripts

        ```
4. Install Hadoop Binaries for Windows: winutils.exe; Note this is only for Windows. Linux users Download Spark libraries for Linux -
   - Download binaries from URL https://github.com/steveloughran/winutils
   - Unzip and place in some directory: For Example I'm using C:\winutils-master
   - Within extracted folder we have hadoop-2.7.1 --> Example: C:\winutils-master\hadoop-2.7.1
   - Declare an Environment variable HADOOP_HOME = C:\winutils-master\hadoop-2.7.1 and update PATH Variable
    ```
    HADOOP_HOME=C:\winutils-master\hadoop-2.7.1
    PATH=%PATH%;%HADOOP_HOME%\bin
    ```
5. Check if all the Environment Variables are working -
   - Open Command prompt
   - Type `python` it must open Python 3.7.x
   - Type `winutils`, `hadoop`, `hdfs` it must give help instructions of HDFS

3. Create a spark project - 
   - Create a directory for example `datalake-etl-pipeline` anywhere in your computer; Here I'm using `/home/<username>` and create a new virtual environment using commands below -
   ```
   mkvirtualenv -a <full-path-to-im-learning-spark> -p <full-path-to-python.exe> py37
   workon py37
   cdproject
   ```
   `cdproject` command will switch to `datalake-etl-pipeline` folder
   
   Import required libraries from requirements.txt from `datalake-etl-pipeline` root folder, use command below – 
   ```
   pip install -r requirements.txt
   ```
   To Freeze the existing installed packages use command below -
   ```
   pip freeze > requirements.txt
   ```
# ToDo - Yet to add instructions for deploying into higher environments
## Delpoy to higher environment