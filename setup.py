from setuptools import find_packages, setup

setup(
    name="fire_project",
    packages=find_packages(exclude=["fire_project_tests"]),
    install_requires=[
        "dagster",
        "dagster-webserver",
        "pandas",
        "requests",
        "beautifulsoup4",   # We added this
        "odfpy",
        "python-calamine",  # We added this
        "deltalake",
        "dagster-deltalake"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)