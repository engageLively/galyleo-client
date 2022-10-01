import setuptools
import os
with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="galyleo",  # This is what you pip install it as.
    version="2022.05.31",
    author="engageLively, inc",
    author_email="rick.mcgeer@engagelively.com",
    description="A client to exchange Galyleo tables and data with a JupyterLab front end",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
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
