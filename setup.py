from setuptools import setup



setup(
    name='dask_elk',
    version='0.0.1',
    description='Dask connection with Elasticsearch',
    url='https://github.com/avlahop/dask-elk',
    maintainer='Apostolos Vlachopoulos',
    maintainer_email='avlahop@gmail.com',
    keywords='elasticsearch dask parallel',
    licence='GPLv3',
    classifiers=[
          "Programming Language :: Python :: 2",
          "Programming Language :: Python :: 2.7",
    ],
    packages=[
        'dask_elk',
        'dask_elk.elk_entities',
    ],
    python_requires=">=2.7",
    install_requires=[
        'dask[complete]',
        'elasticsearch>=6.2',
        'pandas >= 0.19.0',
    ]
)