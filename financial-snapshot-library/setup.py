"""
Setup script for the Financial Snapshot Library.
"""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

setup(
    name="financial-snapshot-library",
    version="1.0.0",
    author="freeCodeCamp",
    author_email="info@freecodecamp.org",
    description="A robust library for capturing financial position snapshots from Kafka to SQL",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/freeCodeCamp/freecodecamp",
    packages=find_packages(exclude=["tests", "tests.*"]),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business :: Financial",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.8",
    install_requires=[
        "confluent-kafka>=2.3.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
        "pandas>=2.1.0",
        "sqlalchemy>=2.0.0",
        "psycopg2-binary>=2.9.0",
        "pymysql>=1.1.1",
        "pyodbc>=5.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "pytest-mock>=3.12.0",
            "black>=23.0.0",
            "flake8>=6.0.0",
            "mypy>=1.7.0",
            "pylint>=3.0.0",
        ],
        "monitoring": [
            "prometheus-client>=0.19.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "financial-snapshot=financial_snapshot.cli:main",
        ],
    },
)
