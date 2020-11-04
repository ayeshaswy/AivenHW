import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="AivenHW",
    version="1.0.0",
    author="Anantha Yeshaswy",
    author_email="yeshaswy@gmail.com",
    description="Aiven homework project",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ayeshaswy/AivenHW",
    packages=setuptools.find_packages(),
    install_requires=['kafka-python',
                      'psycopg2',
                      'jproperties'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)