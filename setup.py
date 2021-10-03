import setuptools

setuptools.setup(
    name='valentine',
    version='0.1.0',
    description='Valentine Matcher',
    license_files=('LICENSE',),
    author='Delft Data',
    author_email='delftdatasystems@gmail.com',
    packages=setuptools.find_packages(exclude=('tests*',)),
    install_requires=[
        'numpy>=1.21,<2.0',
        'scipy>=1.7,<1.8',
        'pandas>=1.3,<1.4',
        'nltk>=3.6,<3.7',
        'snakecase>=1.0,<2.0',
        'anytree>=2.8,<2.9',
        'six>=1.16,<1.17',
        'strsim==0.0.3',
        'networkx>=2.6,<2.7',
        'chardet>=4.0.0,<5.0.0',
        'python-Levenshtein==0.12.2',
        'PuLP>=2.5.1,<2.6',
        'pyemd==0.5.1',
        'python-dateutil>=2.8,<2.9'
    ],
    include_package_data=True,
    python_requires='>=3.7'
)
