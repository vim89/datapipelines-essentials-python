from setuptools import setup, find_packages

install_requires = ['pytest', 'pytest-cov']
tests_require = ['pytest', 'pytest-cov']

setup(
    name='datalake-etl-pipeline',
    version='0.1',
    author='Vitthal Mirji',
    author_email='vitthalmirji@gmail.com',
    url='http://vitthalmirji.com',
    description='Datalake complex transformations simplified in PySpark',
    long_description='Simplified ETL process in Hadoop using Apache Spark. '
                     'SparkSession extensions, DataFrame validation, Column extensions, SQL functions, and DataFrame '
                     'transformations',
    license='APACHE',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=[],
    python_requires='>=3.5',
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': []
    },
    classifiers=[
        'Programming Language :: Python :: 3.7'
    ],
    dependency_links=[],
    include_package_data=False,
    keywords=['apachespark', 'spark', 'pyspark', 'etl', 'hadoop', 'bigdata', 'apache-spark', 'python', 'python3']
)
