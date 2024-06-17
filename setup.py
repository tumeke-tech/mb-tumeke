import re
from setuptools import setup, find_packages

# Load version from module (without loading the whole module)
with open('src/mbtumeke/__init__.py', 'r') as fo:
    version = re.search(r'^__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        fo.read(), re.MULTILINE).group(1)

# Read in the README.md for the long description.
with open('README.md') as fo:
    content = fo.read()
    long_description = content
    description = "TuMeke Kafka and RabbitMQ message brokers"

setup(
    name='mbtumeke',
    version=version,
    url='https://github.com/tumeke-tech/mb-tumeke',
    author='Sergei Kazakov',
    author_email='kazakov.s.fl@gmail.com',
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    license='GPLv3+',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    test_suite='',
    install_requires=['aiokafka==0.7.0', 'aio-pika==9.3.1'],
    keywords='',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
