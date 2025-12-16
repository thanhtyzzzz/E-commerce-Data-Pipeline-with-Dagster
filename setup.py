from setuptools import find_packages, setup

setup(
    name="dagster_ecommerce",
    version="1.0.0",
    packages=find_packages(exclude=["tests"]),
    install_requires=[
        "dagster>=1.8.0",
        "dagster-webserver>=1.8.0",
        "pandas>=2.1.0",
        "requests>=2.31.0",
        "sqlalchemy>=2.0.0",
        "python-dotenv>=1.0.0",
        "faker>=20.0.0",
        "duckdb>=0.9.0"
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.0.0",
            "ruff>=0.1.0"
        ]
    },
    author="Your Name",
    description="E-commerce Data Pipeline with Dagster",
    python_requires=">=3.8",
)