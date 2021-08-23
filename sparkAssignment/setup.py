from setuptools import setup
from setuptools import find_namespace_packages

# Load the README file.
with open(file="README.md", mode="r") as readme_handle:
    long_description = readme_handle.read()

setup(

    # Define the library name, this is what is used along with `pip install`.
    name='pyspark-assignment',

    # Define the author of the repository.
    author='Shailja Nupur',

    # Define the Author's email, so people know who to reach out to.
    author_email='shailjanupur.2607@gmail.com',

    # Define the version of this library.

    version='0.1.0',

    # Here is a small description of the library. This appears
    # when someone searches for the library on https://pypi.org/search.
    description='This is an assignment to collate two datasets for better interfcaing with client',

    # I have a long description but that will just be my README
    # file, note the variable up above where I read the file.
    long_description=long_description,

    # This will specify that the long description is MARKDOWN.
    long_description_content_type="text/markdown",

    # Here is the URL where you can find the code, in this case on GitHub.
    url='https://github.com/Shailja2607/ABNAmroAssignment/commits?author=Shailja2607',

    # These are the dependencies the library needs in order to run.
    install_requires=[
        'pyspark'
    ],

    include_package_data=True,

    # Here I can specify the python version necessary to run this library.
    python_requires='>=3.7',

    classifiers=[

        # Here I'll add the audience this library is intended for.
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',

        # Here I'll note that package was written in English.
        'Natural Language :: English',

        # Here I'll note that any operating system can use it.
        'Operating System :: OS Independent',

        # Here I'll specify the version of Python it uses.
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',

        # Here are the topics that my library covers.
        'Topic :: Python',
        'Topic :: Spark',
        'Topic :: SQL,Logging'

    ]
)
