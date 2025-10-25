from setuptools import setup, find_packages
from typing import List


def get_requirements() -> List[str]:
    """Read the requirements from requirements.txt file and return as a list."""

    try:
        with open("requirements.txt", "r") as file:
            lines = file.readlines()
            requirements = [
                line.strip()
                for line in lines
                if line.strip() and not line.startswith("-e .")
            ]
            return requirements
    except FileNotFoundError:
        raise FileNotFoundError("The requirements.txt file was not found.")


setup(
    name="Network_Security",
    version="0.1.0",
    author="Petros Chol",
    packages=find_packages(),
    install_requires=get_requirements(),
)
