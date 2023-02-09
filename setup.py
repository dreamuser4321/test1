import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

# pyodbc on linux requires unixODBC-devel (unixodbc-dev)

setuptools.setup(
    name="dwh_miner",
    version="0.1.0",
    author="Catalin Baciu",
    author_email="catalin.baciu@betssongroup.com",
    description="AWS based miner for DWH data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://bitbucketsson.betsson.local/users/caba01/repos/bridget/browse/dwh_miner",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "pandas>=0.25.3",
        "boto3>=1.12.39",
        "pyodbc>=4.0.27",
        "pyarrow>=0.17.0",
    ],
    python_requires='>=3.6',
)
