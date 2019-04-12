import os
import sys
from setuptools import setup, find_packages
from tethys_apps.app_installation import custom_develop_command, custom_install_command

# -- Apps Definition -- #
app_package = 'hydroshare_timeseries_manager'
release_package = 'tethysapp-' + app_package
app_class = 'hydroshare_timeseries_manager.app:HydroshareTimeseriesManager'
app_package_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tethysapp', app_package)

# -- Python Dependencies -- #
dependencies = []

setup(
    name=release_package,
    version='0.0.1',
    tags='Time Series, Referenced Time Series, HydroShare, WaterOneFlow, HIS',
    description='The HydroShare Time Series Manager app helps you import time series data into HydroShare from WaterOneFlow services.',
    long_description='',
    keywords='',
    author='Kenneth Lippold',
    author_email='kjlippold@gmail.com',
    url='',
    license='MIT License',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['tethysapp', 'tethysapp.' + app_package],
    include_package_data=True,
    zip_safe=False,
    install_requires=dependencies,
    cmdclass={
        'install': custom_install_command(app_package, app_package_dir, dependencies),
        'develop': custom_develop_command(app_package, app_package_dir, dependencies)
    }
)
