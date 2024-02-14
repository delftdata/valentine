import setuptools
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

classifiers = [
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Operating System :: POSIX",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
]

setuptools.setup(
    name='valentine',
    version='0.2.0',
    description='Valentine Matcher',
    classifiers=classifiers,
    license_files=('LICENSE',),
    author='Delft Data',
    author_email='delftdatasystems@gmail.com',
    maintainer='Delft Data',
    maintainer_email='delftdatasystems@gmail.com',
    url='https://delftdata.github.io/valentine/',
    download_url='https://github.com/delftdata/valentine/archive/refs/tags/v0.2.0.tar.gz',
    packages=setuptools.find_packages(exclude=('tests*', 'examples*')),
    install_requires=[
        'setuptools',
        'numpy>=1.22,<2.0',
        'pandas>=1.3,<2.2',
        'nltk>=3.6,<4.0',
        'anytree>=2.9,<3.0',
        'networkx>=2.8,<4.0',
        'chardet>=5.2.0,<6.0.0',
        'jellyfish>=0.9,<1.1',
        'PuLP>=2.5,<3.0',
        'pyemd>=1.0.0,<2.0',
        'python-dateutil>=2.8,<3.0',
    ],
    keywords=['matching', 'valentine', 'schema matching', 'dataset discovery', 'coma', 'cupid', 'similarity flooding'],
    include_package_data=True,
    python_requires='>=3.8,<3.13',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
