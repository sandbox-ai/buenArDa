
from setuptools import setup, find_packages

setup(
    name="buenarda",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'tenacity',
        'requests',
        'trafilatura'
    ],
)