import setuptools
import os
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="galyleo",  # This is what you pip install it as.
    version="2022.10.20",
    author="engageLively, inc",
    author_email="rick.mcgeer@engagelively.com",
    description="A client to create and exchange  Galyleo tables and data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/engageLively/galyleo-client",
    packages=['galyleo',],  # This is what you import: from galyleo import galyleo_table
    install_requires=[
      'pandas',
      'numpy',
      'gviz-api',
      'ipykernel',
      'flask'
      ],
  classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
