from setuptools import setup, find_packages

setup(
    name="QuantTrade",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "tiingo",
        "python-dotenv"
    ],
)