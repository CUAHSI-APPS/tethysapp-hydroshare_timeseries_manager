# HydroShare Time Series Manager

This app is designed to run on Tethys Platform and helps support CUAHSI's HydroShare project. Its purpose is to facilitate the transfer of hydrologic time series data from CUAHSI's HydroClient application to HydroShare. The app can also be used to combine, subset, and edit existing time series content within HydroShare.

## Getting Started

These instructions will help you install and run this app in a Tethys Platform development environment.

### Prerequisites

##### Tethys Platform (Version 2.1.0 [Python 3] or later):
* [Linux and macOS](http://docs.tethysplatform.org/en/stable/installation/linux_and_mac.html)
* [Windows](http://docs.tethysplatform.org/en/stable/installation/windows.html)

##### HydroShare OAuth Backend:
* [HydroShare Social Authentication](http://docs.tethysplatform.org/en/stable/tethys_portal/social_auth.html#hydroshare)

### Installing

Activate the Tethys conda environment:
```
$ t
```

Clone this repository into your Tethys apps folder:
```
$ git clone https://github.com/kjlippold/hydroshare.git
```

Enter the app folder:
```
$ cd /tethysapp-hydroshare_timeseries_manager
```

Install the app:
```
$ python setup.py develop
```

Before continuing, use the [Tethys Portal Admin Console](http://docs.tethysplatform.org/en/stable/installation/web_admin_setup.html) to define custom settings for the app. The HydroShare URL should point to the instance of HydroShare you wish to connect to (e.g. https://www.hydroshare.org). The HydroServer URL should point to a HydroServer associated with that instance of HydroShare (e.g. https://geoserver.hydroshare.org/wds). The Maximum Value Count setting should be an integer that will limit the total value count of time series datasets that users can upload to HydroShare. Finally, this app requires a connection to a [Tethys Persistent Store Database](http://docs.tethysplatform.org/en/stable/tutorials/getting_started/advanced.html#persistent-store-database) for server-side table processing.

After defining the app custom settings, initialize the app database:
```
$ tethys syncstores hydroshare_timeseries_manager
```

The HydroShare Time Series Manager should now be running in you Tethys Portal.

## Built With

* [Tethys Platform](http://www.tethysplatform.org) - Web Application Framework
* [HydroShare](https://www.hydroshare.org/) - Hydrologic Information System

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.txt) file for details
