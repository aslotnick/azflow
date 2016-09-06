from setuptools import setup, find_packages

setup(
    name='Azflow',
    author='Andrew C. Slotnick',
    url='https://github.com/aslotnick/azflow',
    description='Translates Airflow DAGs into Azkaban flows.',
    license='MIT',
    version='0.1.0',
    packages=find_packages(exclude=['test'])
    )
