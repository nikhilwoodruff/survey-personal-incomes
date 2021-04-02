from setuptools import setup, find_packages

setup(
    name="survey_personal_incomes",
    version="0.0.1",
    description=(
        "A Python package to manage Survey of Personal Incomes microdata."
    ),
    url="http://github.com/nikhilwoodruff/survey_personal_incomes",
    author="Nikhil Woodruff",
    author_email="nikhil.woodruff@outlook.com",
    packages=find_packages(),
    install_requires=["pandas", "openpyxl", "xlrd"],
    entry_points={
        "console_scripts": ["spi-data=survey_personal_incomes.main:main"],
    },
)
