from setuptools import setup
import versioneer

requirements = [
    # package requirements go here
]

setup(
    name='ibsg',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="A GUI to generate a csv file of individual Irish buildings ",
    license="MIT",
    author="Rowan Molony",
    author_email='rowan.molony@codema.ie',
    url='https://github.com/rdmolony/ibsg',
    packages=['ibsg'],
    entry_points={
        'console_scripts': [
            'ibsg=ibsg.cli:cli'
        ]
    },
    install_requires=requirements,
    keywords='ibsg',
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ]
)
