from tethys_sdk.base import TethysAppBase, url_map_maker
from tethys_sdk.app_settings import CustomSetting, PersistentStoreDatabaseSetting


class HydroshareTimeseriesManager(TethysAppBase):
    """
    Tethys app class for HydroShare Time Series Manager.
    """

    name = 'HydroShare Time Series Manager'
    index = 'hydroshare_timeseries_manager:home'
    icon = 'hydroshare_timeseries_manager/images/cuahsi_logo.png'
    package = 'hydroshare_timeseries_manager'
    root_url = 'hydroshare-timeseries-manager'
    color = '#004d99'
    description = 'The HydroShare Time Series Manager app helps you import time series data into HydroShare from WaterOneFlow services.'
    tags = 'Time Series, Referenced Time Series, HydroShare, WaterOneFlow, HIS'
    enable_feedback = False
    feedback_emails = []

    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (
            UrlMap(
                name='home',
                url='hydroshare-timeseries-manager',
                controller='hydroshare_timeseries_manager.controllers.home'
            ),
            UrlMap(
                name='ajax_login_test',
                url='hydroshare-timeseries-manager/ajax/login-test',
                controller='hydroshare_timeseries_manager.ajax_controllers.login_test'
            ),
            UrlMap(
                name='ajax_load_session_data',
                url='hydroshare-timeseries-manager/ajax/load-session-data',
                controller='hydroshare_timeseries_manager.ajax_controllers.load_session_data'
            ),
            UrlMap(
                name='ajax_prepare_timeseries_data',
                url='hydroshare-timeseries-manager/ajax/prepare-timeseries-data',
                controller='hydroshare_timeseries_manager.ajax_controllers.prepare_timeseries_data'
            ),
            UrlMap(
                name='ajax_check_timeseries_status',
                url='hydroshare-timeseries-manager/ajax/ajax-check-timeseries-status',
                controller='hydroshare_timeseries_manager.ajax_controllers.ajax_check_timeseries_status'
            ),
            UrlMap(
                name='ajax_create_hydroshare_resource',
                url='hydroshare-timeseries-manager/ajax/ajax-create-hydroshare-resource',
                controller='hydroshare_timeseries_manager.ajax_controllers.ajax_create_hydroshare_resource'
            ),
        )

        return url_maps


    def custom_settings(self):
        custom_settings = (
            CustomSetting(
                name='hydroshare_url',
                type=CustomSetting.TYPE_STRING,
                description='HydroShare URL',
                required=True
            ),
        )

        return custom_settings


    def persistent_store_settings(self):
        ps_settings = (
            PersistentStoreDatabaseSetting(
                name='hydroshare_timeseries_manager',
                description='HydroShare Time Series Manager Database',
                initializer='hydroshare_timeseries_manager.model.init_hydroshare_timeseries_manager_db',
                required=True
            ),
        )

        return ps_settings
