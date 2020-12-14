import setuptools

with open("README.md", "r", encoding="utf-8") as fp:
    long_description = fp.read()

setuptools.setup(
    name="pygraphetl",
    version="0.0.1a",
    author="Joseph D. Romano",
    author_email="jdromano2@gmail.com",
    description="A command line toolkit for performing ETL on graph databases",
    long_description=long_description,
    url="https://github.com/jdromano2/graphdbetl",
    packages=setuptools.find_packages(),
    scripts=[
        'bin/build-graph-db'
    ],
    install_requires=[
        'mysql',
        'h5py',
        'tqdm',
        'pyyaml'
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Operating System :: Unix",
        "Programming Language :: Python :: 3.8",
        "Topic :: Database",
        "Topic :: Scientific/Engineering :: Artificial Intelligence"
    ],
    python_requires='>=3.8',
)