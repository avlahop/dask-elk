from setuptools import setup

long_description = ''

with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='dask_elk',
    version='0.1.0',
    description='Dask connection with Elasticsearch',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/avlahop/dask-elk',
    maintainer='Apostolos Vlachopoulos',
    maintainer_email='avlahop@gmail.com',
    keywords='elasticsearch dask parallel',
    licence='GPLv3',
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: GNU General Public License (GPL)',
    ],
    packages=[
        'dask_elk',
        'dask_elk.elk_entities',
    ],
    python_requires=">=2.7",
    install_requires=[
        'dask[complete]',
        'elasticsearch>=6.2',
        'pandas>=0.19.0',
    ],
    tests_require=[
        'mock',
        'nose',
    ],
    test_suite='nose.collector',
    project_urls = {
        'Documentation': 'https://dask-elk.readthedocs.io'
    }
)