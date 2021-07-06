from setuptools import setup
import versioneer

install_requires = open("requirements.txt").read().strip().split("\n")
extras = {
    "cron": ["git+https://github.com/codema-dev/ber-api"],
    "dev": ["black", "mypy"],
    "test": ["pytest-cov", "pytest-datadir"],
}
extras["all"] = sum(extras.values(), [])

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
    package_data={"ibsg": ["defaults.json"]},
    entry_points={"console_scripts": ["ibsg=ibsg.cli:cli"]},
    install_requires=install_requires,
    extras_require=extras,
    keywords="ibsg",
    classifiers=[
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
)
