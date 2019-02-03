from setuptools import setup, find_packages
import os

HERE = os.path.abspath(os.path.dirname(__file__))
HOME = os.path.expanduser('~')

with open(os.path.join(HERE, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

os.mkdir(os.path.join(HOME, '.qualipy'))

setup(
    name='qualipy',
    version='0.1.0',
    description='A data quality tool for batch ml pipelines',
    long_description_content_type='text/markdown',
    url='https://github.com/baasman/qualipy',
    author='Boudewijn Aasman',
    author_email='boudeyz@gmail.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Machine Learning',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
    ],
    keywords='data quality machine learning',
    packages=find_packages(exclude=['contrib', 'docs', 'tests']),
    install_requires=required,
)