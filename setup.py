import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pyservica",
    version="0.0.1",
    author="Lachlan Glanville",
    author_email="lachlanglanville@gmail.com",
    description="Tools for building Preservica V6 SIPs and interacting with"
    "the API",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/lglanville/Pyservica",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
