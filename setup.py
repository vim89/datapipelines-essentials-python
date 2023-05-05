import subprocess
import sys

from setuptools.command.install import install
from setuptools import setup, find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

tests_require = ['pytest', 'pytest-cov', 'coverage']

with open("docs/ETL_README.md", "r") as f:
    long_description = f.read()


class ShellInstall(install):
    def run(self):
        if not sys.platform.startswith("linux"):
            print('Your platform {} might not be supported'.format(sys.platform))
        else:
            print('Running create_python_venv.sh -n hello-fresh-data-engg')
        subprocess.call(['./sbin/create_python_venv.sh', '-n', 'hello-fresh-data-engg'])
        install.run(self)


setup(
    cmdclass={'install': ShellInstall},
    name='datapipelines-essentials',
    version='2.0',
    author='Vitthal Mirji',
    author_email='vitthalmirji@gmail.com',
    url='https://vitthalmirji.com',
    description='Datalake complex transformations simplified in PySpark',
    long_description='Simplified ETL process in Hadoop using Apache Spark. '
                     'SparkSession extensions, DataFrame validation, Column extensions, SQL functions, and DataFrame '
                     'transformations',
    long_description_content_type="text/markdown",
    install_requires=requirements,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
        'all': requirements + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': []
    },
    license="GNU :: GPLv3",
    include_package_data=True,
    packages=find_packages(where='src', include=['com*']),
    package_dir={"": "src"},
    setup_requires=['setuptools'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: GNU :: GPLv3",
        "Operating System :: Linux",
    ],
    dependency_links=[],
    python_requires='>=3.7,<=3.9.5',
    keywords=['apachespark', 'spark', 'pyspark', 'etl', 'hadoop', 'bigdata', 'apache-spark', 'python', 'python3',
              'data', 'dataengineering', 'datapipelines']
)
