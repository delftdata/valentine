import setuptools
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

classifiers = [
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Operating System :: POSIX",
    "Operating System :: MacOS :: MacOS X",
    "Operating System :: Microsoft :: Windows",
]

setuptools.setup(
    name='valentine',
    version='0.1.6',
    description='Valentine Matcher',
    classifiers=classifiers,
    license_files=('LICENSE',),
    author='Delft Data',
    author_email='delftdatasystems@gmail.com',
    maintainer='Delft Data',
    maintainer_email='delftdatasystems@gmail.com',
    url='https://delftdata.github.io/valentine/',
    download_url='https://github.com/delftdata/valentine/archive/refs/tags/v0.1.6.tar.gz',
    packages=setuptools.find_packages(exclude=('tests*', 'examples*')),
    install_requires=[
        'numpy>=1.22,<2.0',
        'pandas>=1.3,<2.0',
        'nltk>=3.6,<4.0',
        'snakecase>=1.0,<2.0',
        'anytree>=2.8,<2.9',
        'six>=1.16,<1.17',
        'strsimpy==0.2.1',
        'networkx>=2.8,<3.0',
        'chardet>=5.0.0,<6.0.0',
        'python-Levenshtein>=0.20.7,<1.0',
        'PuLP>=2.5.1,<3.0',
        'pot>=0.8.2,<1.0',
        'python-dateutil>=2.8,<2.9'
    ],
    keywords=['matching', 'valentine', 'schema matching', 'dataset discovery', 'coma', 'cupid', 'similarity flooding'],
    include_package_data=True,
    python_requires='>=3.8,<3.12',
    long_description=long_description,
    long_description_content_type='text/markdown'
)
