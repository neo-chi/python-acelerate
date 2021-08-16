"""A setuptools based setup module.

See:
    https://github.com/reecechimento/python-acelerate
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    name='python-acelerate',  # Required
    version='0.1.0',  # Required
    description='An electrical engineering project',                        # NOTE: Optional
    long_description=long_description,                                      # NOTE: Optional
    long_description_content_type='text/markdown',                          # NOTE: 'text/plain' | 'text/rst' | 'text/markdown'
    url='https://github.com/reecechimento/python-acelerate',                  # NOTE: Optional
    author='Chimento, Reece',
    author_email='reecechimento@gmail.com',
    classifiers=[
        # How mature is this project? | 3 - Alpha | 4 - Beta | 5 Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Engineers',
        'Topic :: Engineering :: Energy Storage Systems',
        'License :: OSI Approved :: MIT License',
        # NOTE: Specifies the python versions you support.
        'Programming Language :: Python :: 3.9 :: Only',
        'Programming Language :: Python :: 3 :: Only',
        ],
    # NOTE: This field adds keywords for your project which will appear on
    # the project page. What does your project relate to?
    keywords='engineering,electrical,energy-storage,test-engineering',
    # NOTE: When your source code is in a subdirectory under the project
    # root, e.g. `src/`, it is necessary to specifty the `package_dir`
    # argument.
    package_dir={'': 'src'},  # Optional
    # You can just specify your package directories manually here if your
    # project is simple.
    # NOTE: OTHERWISE you can use find_packages().
    # NOTE: Alternatively, if you just want to distribute a single Python
    # file, use the `py_modules` argument instead as follows, which will
    # expect a file called `my_modeule.py` to exist:
    #   py_modules=["my_module"],
    packages=find_packages(where='src'),  # WARN: Required
    # WARN: Specify which Python versions you support. `pip install` will check this
    python_requires='>=3.9, <4',
    # WARN: `install_requires` specifies what a project *minimally* needs to
    # run correctly.
    # NOTE: This is the specification that is used to install its
    # dependencies.
    install_requires=[
        'aiohttp>=3.7.4',
        'ruamel.yaml>=0.17.10'
    ],
    # List of additional groups of dependencies (e.g. development dependencies)
    #   $ pip install python-acelerate[dev]
    extras_require={
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    # If there are any data files included in your packages that need to be
    # installed, specify them here.
    package_data={
        'config': ['init.yml'],
    },
    # Although 'package_data' is preferred approace, in some cases you may
    # need to place data files outside of your packages. See:
    # https://docs.python.org/distutils/setupscript.html#installing-additional-files
    #
    # In this case, 'data_file' will be installed into '<sys.previx>/my_data'
    data_files=[('my_data', ['data/data_file'])],  # Optional

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and
    # allow `pip` to create the appropriate form of executable for the
    # target platform
    #
    #
    # For example, the following would provide a command called `acelerate` which
    # executes the function `main` from this package when invoked:
    entry_points={  # Optional
        'console_scripts': [
            'acelerate=acelerate:main',
        ],
    },
    # List additional URLs that are relevant to your project as a dict.
    #
    # This field corresponds to the "Project-URL" metadata fields:
    # https://packaging.python.org/specifications/core-metadata/#project-url-multiple-use
    #
    # Examples listed include a pattern for specifying where the package tracks
    # issues, where the source is hosted, where to say thanks to the package
    # maintainers, and where to support the project financially. The key is
    # what's used to render the link text on PyPI.
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/reecechimento/g/python-acelerate/issues',
        'Source': 'https://github.com/reecechimento/g/python-acelerate/',
    },
)
