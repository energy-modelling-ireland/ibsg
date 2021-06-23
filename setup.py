from setuptools import setup
import versioneer

install_requires = ["gooey", "pandas"]
dev_requires = ["black", "mypy"]

data_files = ["ibsg/" + file for file in ["dtypes.json"]]

setup(
    name="ibsg",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="A GUI to generate a csv file of individual Irish buildings ",
    license="MIT",
    author="Rowan Molony",
    author_email="rowan.molony@codema.ie",
    url="https://github.com/rdmolony/ibsg",
    packages=["ibsg"],
    entry_points={"console_scripts": ["ibsg=ibsg.cli:cli"]},
    install_requires=install_requires,
    package_data={"geopandas": data_files},
    keywords="ibsg",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
