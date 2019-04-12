import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, Boolean, DateTime, Integer, PickleType, UniqueConstraint, and_
from .app import HydroshareTimeseriesManager as app


# DB Engine, sessionmaker, and base
Base = declarative_base()


class TimeSeriesCatalog (Base):
    """
    TimeSeriesCatalog SQLAlchemy DB Model
    """
    __tablename__ = "timeseries_catalog"

    # Columns
    id = Column(Integer, primary_key=True)
    session_id = Column(Text)
    timeseries_id = Column(Text)
    status = Column(Text)
    valid_refts = Column(Text)
    refts_status = Column(Text)
    valid_wml = Column(Text)
    odm2_status = Column(Text)
    wml_data = Column(PickleType)
    selected = Column(Boolean)
    date_created = Column(DateTime)
    begin_date = Column(Text)
    end_date = Column(Text)
    value_count = Column(Text)
    sample_medium = Column(Text)
    site_name = Column(Text)
    site_code = Column(Text)
    latitude = Column(Text)
    longitude = Column(Text)
    variable_name = Column(Text)
    variable_code = Column(Text)
    method_description = Column(Text)
    method_link = Column(Text)
    network_name = Column(Text)
    ref_type = Column(Text)
    return_type = Column(Text)
    service_type = Column(Text)
    url = Column(Text)
    cache_uri = Column(Text)

    # Constraints
    __table_args__ = (
    	UniqueConstraint("session_id", "site_code", "variable_code", name="_ts_result"),
    )


def add_timeseries_reference(session_id, timeseries_id, begin_date, end_date, value_count, 
        sample_medium, site_name, site_code, latitude, longitude, variable_name, variable_code, 
        method_description, method_link, network_name, ref_type, return_type, service_type, 
        url, status="WAITING", valid_refts="UNKNOWN", refts_status="UNKNOWN", valid_wml="UNKNOWN", 
        odm2_status="UNKNOWN", wml_data="NONE", selected=False, cache_uri="NONE"):
    """
    Creates a new session object row
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    new_timeseries_reference = TimeSeriesCatalog(
        session_id=session_id,
        timeseries_id=timeseries_id,
        status=status,
        valid_refts=valid_refts,
        refts_status=refts_status,
        valid_wml=valid_wml,
        odm2_status=odm2_status,
        selected=selected,
        date_created=datetime.datetime.now(),
        wml_data=wml_data,
        begin_date=begin_date,
        end_date=end_date,
        value_count=value_count,
        sample_medium=sample_medium,
        site_name=site_name,
        site_code=site_code,
        latitude=latitude,
        longitude=longitude,
        variable_name=variable_name,
        variable_code=variable_code,
        method_description=method_description,
        method_link=method_link,
        network_name=network_name,
        ref_type=ref_type,
        return_type=return_type,
        service_type=service_type,
        url=url,
        cache_uri=cache_uri
    )

    session.add(new_timeseries_reference)

    session.commit()
    session.close()


def update_timeseries_reference(session_id, timeseries_id, status=None, valid_refts=None, 
        refts_status=None, valid_wml=None, odm2_status=None, wml_data=None, selected=None, 
        begin_date=None, end_date=None, value_count=None, sample_medium=None, site_name=None, 
        site_code=None, latitude=None, longitude=None, variable_name=None, variable_code=None, 
        method_description=None, method_link=None, network_name=None, ref_type=None, 
        return_type=None, service_type=None, url=None, cache_uri=None):
    """
    Updates parameter values of a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_reference = session.\
        query(
            TimeSeriesCatalog
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        )

    if status is not None : timeseries_reference.update({TimeSeriesCatalog.status: status})
    if valid_refts is not None : timeseries_reference.update({TimeSeriesCatalog.valid_refts: valid_refts})
    if refts_status is not None : timeseries_reference.update({TimeSeriesCatalog.refts_status: refts_status})
    if valid_wml is not None : timeseries_reference.update({TimeSeriesCatalog.valid_wml: valid_wml})
    if odm2_status is not None : timeseries_reference.update({TimeSeriesCatalog.odm2_status: odm2_status})
    if wml_data is not None : timeseries_reference.update({TimeSeriesCatalog.wml_data: wml_data})
    if selected is not None : timeseries_reference.update({TimeSeriesCatalog.selected: selected})
    if begin_date is not None : timeseries_reference.update({TimeSeriesCatalog.begin_date: begin_date})
    if end_date is not None : timeseries_reference.update({TimeSeriesCatalog.end_date: end_date})
    if value_count is not None : timeseries_reference.update({TimeSeriesCatalog.value_count: value_count})
    if sample_medium is not None : timeseries_reference.update({TimeSeriesCatalog.sample_medium: sample_medium})
    if site_name is not None : timeseries_reference.update({TimeSeriesCatalog.site_name: site_name})
    if site_code is not None : timeseries_reference.update({TimeSeriesCatalog.site_code: site_code})
    if latitude is not None : timeseries_reference.update({TimeSeriesCatalog.latitude: latitude})
    if longitude is not None : timeseries_reference.update({TimeSeriesCatalog.longitude: longitude})
    if variable_name is not None : timeseries_reference.update({TimeSeriesCatalog.variable_name: variable_name})
    if variable_code is not None : timeseries_reference.update({TimeSeriesCatalog.variable_code: variable_code})
    if method_description is not None : timeseries_reference.update({TimeSeriesCatalog.method_description: method_description})
    if method_link is not None : timeseries_reference.update({TimeSeriesCatalog.method_link: method_link})
    if network_name is not None : timeseries_reference.update({TimeSeriesCatalog.network_name: network_name})
    if ref_type is not None : timeseries_reference.update({TimeSeriesCatalog.ref_type: ref_type})
    if return_type is not None : timeseries_reference.update({TimeSeriesCatalog.return_type: return_type})
    if service_type is not None : timeseries_reference.update({TimeSeriesCatalog.service_type: service_type})
    if url is not None : timeseries_reference.update({TimeSeriesCatalog.url: url})
    if cache_uri is not None : timeseries_reference.update({TimeSeriesCatalog.cache_uri: cache_uri})

    session.commit()
    session.close()


def check_timeseries_status(session_id):
    """
    Check the status of all objects of a given session
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_status = session.\
        query(
            TimeSeriesCatalog.timeseries_id,
            TimeSeriesCatalog.status,
            TimeSeriesCatalog.valid_refts,
            TimeSeriesCatalog.refts_status,
            TimeSeriesCatalog.valid_wml,
            TimeSeriesCatalog.odm2_status
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id
            )
        ).all()

    session.close()

    return timeseries_status


def add_wml_data(session_id, timeseries_id, wml_data):
    """
    Adds WaterML data to a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_data = session.\
        query(
            TimeSeriesCatalog
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        ).update({
            TimeSeriesCatalog.wml_data: wml_data
        })

    session.commit()
    session.close()


def get_timeseries_references(session_id):
    """
    Gets all metadata from a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_references = session.\
        query(
            TimeSeriesCatalog.timeseries_id,
            TimeSeriesCatalog.selected,
            TimeSeriesCatalog.begin_date,
            TimeSeriesCatalog.end_date,
            TimeSeriesCatalog.value_count,
            TimeSeriesCatalog.sample_medium,
            TimeSeriesCatalog.site_name,
            TimeSeriesCatalog.site_code,
            TimeSeriesCatalog.latitude,
            TimeSeriesCatalog.longitude,
            TimeSeriesCatalog.variable_name,
            TimeSeriesCatalog.variable_code,
            TimeSeriesCatalog.method_description,
            TimeSeriesCatalog.method_link,
            TimeSeriesCatalog.network_name,
            TimeSeriesCatalog.ref_type,
            TimeSeriesCatalog.return_type,
            TimeSeriesCatalog.service_type,
            TimeSeriesCatalog.url,
            TimeSeriesCatalog.cache_uri
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id
            )
        ).all()

    session.close()

    return timeseries_references


def get_timeseries_reference(session_id, timeseries_id):
    """
    Gets all metadata from a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_reference = session.\
        query(
            TimeSeriesCatalog.timeseries_id,
            TimeSeriesCatalog.selected,
            TimeSeriesCatalog.begin_date,
            TimeSeriesCatalog.end_date,
            TimeSeriesCatalog.value_count,
            TimeSeriesCatalog.sample_medium,
            TimeSeriesCatalog.site_name,
            TimeSeriesCatalog.site_code,
            TimeSeriesCatalog.latitude,
            TimeSeriesCatalog.longitude,
            TimeSeriesCatalog.variable_name,
            TimeSeriesCatalog.variable_code,
            TimeSeriesCatalog.method_description,
            TimeSeriesCatalog.method_link,
            TimeSeriesCatalog.network_name,
            TimeSeriesCatalog.ref_type,
            TimeSeriesCatalog.return_type,
            TimeSeriesCatalog.service_type,
            TimeSeriesCatalog.url,
            TimeSeriesCatalog.cache_uri
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        ).one()

    session.close()

    return timeseries_reference


def get_wml_data(session_id, timeseries_id):
    """
    Gets data from a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    wml_data = session.\
        query(
            TimeSeriesCatalog.wml_data,
            TimeSeriesCatalog.return_type
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        ).one()

    session.close()

    return wml_data


def remove_timeseries_reference(session_id, timeseries_id):
    """
    Removes a session object
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_references = session.\
        query(
            TimeSeriesCatalog
        ).filter_by(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        ).one().delete()

    session.commit()
    session.close()


def remove_timeseries_reference_session(session_id):
    """
    Removes a session
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    timeseries_reference_session = session.\
        query(
            TimeSeriesCatalog
        ).filter_by(
            and_(
                TimeSeriesCatalog.session_id == session_id
            )
        ).all().delete()

    session.commit()
    session.close()


def init_hydroshare_timeseries_manager_db(engine, first_time):
    Base.metadata.create_all(engine)
