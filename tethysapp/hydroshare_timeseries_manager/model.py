import datetime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Text, Boolean, DateTime, Integer, PickleType, UniqueConstraint, and_, or_, desc, asc
from .app import HydroshareTimeseriesManager as app


# DB Engine, sessionmaker, and base
Base = declarative_base()

def init_hydroshare_timeseries_manager_db(engine, first_time):
    """
    Initializes Database
    """

    # Create tables
    Base.metadata.create_all(engine)


# ---------------------- #
#   TABLE DECLARATIONS   #
# ---------------------- #

class TimeSeriesCatalog(Base):
    """
    TimeSeriesCatalog SQLAlchemy DB Model
    """

    __tablename__ = "timeseries_catalog"

    # Columns
    id = Column(Integer, primary_key=True)
    session_id = Column(Text)
    timeseries_id = Column(Text)
    status = Column(Text)
    status_details = Column(Text)
    wml_data = Column(PickleType)
    selected = Column(Boolean)
    date_created = Column(DateTime)
    begin_date = Column(DateTime)
    end_date = Column(DateTime)
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

    # Constraints
    __table_args__ = (
    	UniqueConstraint("session_id", "site_code", "variable_code", name="_ts_result"),
    )


class PendingTimeSeries(Base):
    """
    PendingTimeSeries SQLAlchemy DB Model
    """

    __tablename__ = "pending_timeseries"

    # Columns
    id = Column(Integer, primary_key=True)
    session_id = Column(Text)
    timeseries_id = Column(Text)
    refts_id = Column(Text)

    # Constraints
    __table_args__ = (
        UniqueConstraint("session_id", "timeseries_id", "refts_id", name="_ts_refts"),
    )


# ------------------------------ #
#   TIMESERIES CATALOG ACTIONS   #
# ------------------------------ #


def add_timeseries_reference(session_id, timeseries_id, begin_date, end_date, value_count, 
        sample_medium, site_name, site_code, latitude, longitude, variable_name, variable_code, 
        method_description, method_link, network_name, ref_type, return_type, service_type, 
        url, status="Waiting", status_details="None", wml_data="None", selected=False):
    """
    Creates a new timeseries reference.

    This function will create a new timeseries reference in the timeseries
    catalog.
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
        status_details=status_details,
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
        url=url
    )

    session.add(new_timeseries_reference)

    session.commit()
    session.close()


def get_timeseries_references(session_id, search_value, length, offset, column, order):
    """
    Gets a filtered list of timeseries references.

    This function will generate a filtered list of timeseries references belonging to a session
    given a search value. The length, offset, and order of the list can also be specified.
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    sortable_columns = [
        "status", 
        "site_name",
        "site_code",
        "latitude", 
        "longitude",
        "variable_name",
        "variable_code",
        "sample_medium",
        "begin_date",
        "end_date",
        "value_count",
        "method_link",
        "method_description",
        "network_name",
        "url",
        "service_type",
        "ref_type",
        "return_type"
    ]

    full_query = session.\
        query(
            TimeSeriesCatalog.status,
            TimeSeriesCatalog.status,
            TimeSeriesCatalog.site_name,
            TimeSeriesCatalog.site_code,
            TimeSeriesCatalog.latitude,
            TimeSeriesCatalog.longitude,
            TimeSeriesCatalog.variable_name,
            TimeSeriesCatalog.variable_code,
            TimeSeriesCatalog.sample_medium,
            TimeSeriesCatalog.begin_date,
            TimeSeriesCatalog.end_date,
            TimeSeriesCatalog.value_count,
            TimeSeriesCatalog.method_link,
            TimeSeriesCatalog.method_description,
            TimeSeriesCatalog.network_name,
            TimeSeriesCatalog.url,
            TimeSeriesCatalog.service_type,
            TimeSeriesCatalog.ref_type,
            TimeSeriesCatalog.return_type,
            TimeSeriesCatalog.timeseries_id,
            TimeSeriesCatalog.selected
        ).filter(
            TimeSeriesCatalog.session_id == session_id
        )

    if search_value != "":
        filtered_query = full_query.filter(
            or_(
                TimeSeriesCatalog.status.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.site_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.site_code.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.variable_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.variable_code.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.sample_medium.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.network_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.service_type.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.ref_type.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.return_type.ilike(f"%{search_value}%"),
            )
        )
    else:
        filtered_query = full_query

    if order == "asc":
        ordered_query = filtered_query.order_by(
            asc(getattr(TimeSeriesCatalog, sortable_columns[int(column)]))
        )
    elif order == "desc":
        ordered_query = filtered_query.order_by(
            desc(getattr(TimeSeriesCatalog, sortable_columns[int(column)]))
        )
    else:
        ordered_query = filtered_query.order_by(
            asc(TimeSeriesCatalog.timeseries_id)
        )

    paginated_query = ordered_query.offset(offset).limit(length)

    selected_query = full_query.filter(
        TimeSeriesCatalog.selected == True
    )

    full_query_count = full_query.count()
    filtered_query_count = filtered_query.count()
    selected_query_count = selected_query.count()
    query_results = paginated_query.all()

    session.close()

    return full_query_count, filtered_query_count, selected_query_count, query_results


def update_timeseries_selections(session_id, timeseries_id, search_value, selected):
    """
    Updates selected value of one or more timeseries references.

    This function can update the 'selected' value of a single timeseries reference, given a timeseries ID,
    or of many values, given a search query.
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    full_query = session.\
        query(
            TimeSeriesCatalog
        ).filter(
            TimeSeriesCatalog.session_id == session_id
        )

    if timeseries_id:
        filtered_query = full_query.filter(
            TimeSeriesCatalog.timeseries_id == timeseries_id
        )
    elif search_value != "":
        filtered_query = full_query.filter(
            or_(
                TimeSeriesCatalog.status.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.site_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.site_code.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.variable_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.variable_code.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.sample_medium.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.network_name.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.service_type.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.ref_type.ilike(f"%{search_value}%"),
                TimeSeriesCatalog.return_type.ilike(f"%{search_value}%"),
            )
        )
    else:
        filtered_query = full_query

    filtered_query.update({TimeSeriesCatalog.selected: selected}, synchronize_session='fetch')

    session.commit()
    session.close()



def remove_timeseries_references(session_id, selected, timeseries_id):
    """
    Removes timeseries references from session.

    This function can remove one or more timeseries references from a session.
    It can either remove one timeseries, all selected timeseries, or all timeseries
    in the session.
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    full_query = session.\
        query(
            TimeSeriesCatalog
        ).filter(
            TimeSeriesCatalog.session_id == session_id
        )

    if timeseries_id:
        filtered_query = full_query.filter(
            TimeSeriesCatalog.timeseries_id == timeseries_id
        )
    elif selected is True:
        filtered_query = full_query.filter(
            TimeSeriesCatalog.selected == True
        )
    else:
        filtered_query = full_query

    filtered_query.delete()

    session.commit()
    session.close()


def get_timeseries_request_data(session_id, timeseries_id):
    """
    Gets all metadata for a specific timeseries.

    This function will return necessary data for building
    a WaterOneFlow request for a given timeseries.
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    full_query = session.\
        query(
            TimeSeriesCatalog.timeseries_id,
            TimeSeriesCatalog.selected,
            TimeSeriesCatalog.status,
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
            TimeSeriesCatalog.network_name,
            TimeSeriesCatalog.ref_type,
            TimeSeriesCatalog.return_type,
            TimeSeriesCatalog.service_type,
            TimeSeriesCatalog.url
        ).filter(
            TimeSeriesCatalog.session_id == session_id,
        )

    filtered_query = full_query.filter(
        TimeSeriesCatalog.timeseries_id == timeseries_id
    ).one()

    session.close()

    return filtered_query


def update_timeseries_reference(session_id, timeseries_id, status=None, status_details=None,
        wml_data=None, selected=None, begin_date=None, end_date=None, value_count=None, 
        sample_medium=None, site_name=None, site_code=None, latitude=None, longitude=None, 
        variable_name=None, variable_code=None, method_description=None, method_link=None, 
        network_name=None, ref_type=None, return_type=None, service_type=None, url=None):
    """
    Updates values of a timeseries reference.

    This function can update any parameter for a time series
    reference in the time series catalog.
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
    if status_details is not None : timeseries_reference.update({TimeSeriesCatalog.status_details: status_details})
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

    session.commit()
    session.close()


def get_wml_data(session_id, timeseries_id):
    """
    Gets WaterML data from a timeseries reference.

    This function will retrieve a stored WaterML data object for
    a timeseries reference.
    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    wml_data = session.\
        query(
            TimeSeriesCatalog.wml_data
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.timeseries_id == timeseries_id
            )
        ).one()

    session.close()

    return wml_data


def get_resource_metadata(session_id):
    """

    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    site_query = session.\
        query(
            TimeSeriesCatalog.site_name
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status == "Ready"
            )
        ).distinct()

    site_names = site_query.limit(5).all()
    site_count = site_query.count()

    variable_query = session.\
        query(
            TimeSeriesCatalog.variable_name
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status == "Ready"
            )
        ).distinct()

    variable_names = variable_query.limit(5).all()
    variable_count = variable_query.count()

    sample_medium_query = session.\
        query(
            TimeSeriesCatalog.sample_medium
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status == "Ready"
            )
        ).distinct()

    sample_mediums = sample_medium_query.limit(5).all()

    start_date = session.\
        query(
            TimeSeriesCatalog.begin_date
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status == "Ready"
            )
        ).order_by(TimeSeriesCatalog.begin_date.asc()).first()

    end_date = session.\
        query(
            TimeSeriesCatalog.end_date
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status == "Ready"
            )
        ).order_by(TimeSeriesCatalog.begin_date.desc()).first()

    invalid_selected_count = session.\
        query(
            TimeSeriesCatalog
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True,
                TimeSeriesCatalog.status != "Ready"
            )
        ).count()

    session.close()

    return site_names, site_count, variable_names, variable_count, sample_mediums, start_date, end_date, invalid_selected_count


def get_refts(session_id):
    """

    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    refts = session.\
        query(
            TimeSeriesCatalog.site_name,
            TimeSeriesCatalog.site_code,
            TimeSeriesCatalog.latitude,
            TimeSeriesCatalog.longitude,
            TimeSeriesCatalog.variable_name,
            TimeSeriesCatalog.variable_code,
            TimeSeriesCatalog.sample_medium,
            TimeSeriesCatalog.begin_date,
            TimeSeriesCatalog.end_date,
            TimeSeriesCatalog.value_count,
            TimeSeriesCatalog.method_link,
            TimeSeriesCatalog.method_description,
            TimeSeriesCatalog.network_name,
            TimeSeriesCatalog.url,
            TimeSeriesCatalog.service_type,
            TimeSeriesCatalog.ref_type,
            TimeSeriesCatalog.return_type
        ).filter(
            and_(
                TimeSeriesCatalog.session_id == session_id,
                TimeSeriesCatalog.selected == True
            )
        ).all()

    session.close()

    return refts



# ------------------------------ #
#   PENDING TIMESERIES ACTIONS   #
# ------------------------------ #

def add_pending_timeseries(session_id, refts_id, timeseries_id):
    """

    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    new_pending_timeseries = PendingTimeSeries(
        session_id=session_id,
        refts_id=refts_id,
        timeseries_id=timeseries_id
    )

    session.add(new_pending_timeseries)

    session.commit()
    session.close()

def get_pending_timeseries(session_id, refts_id):
    """

    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    pending_timeseries = session.\
        query(
            PendingTimeSeries.timeseries_id
        ).filter(
            and_(
                PendingTimeSeries.session_id == session_id,
                PendingTimeSeries.refts_id == refts_id
            )
        ).all()

    session.commit()
    session.close()

    return pending_timeseries

def remove_pending_timeseries(session_id, refts_id):
    """

    """

    Session = app.get_persistent_store_database(
        "hydroshare_timeseries_manager", 
        as_sessionmaker=True
    )

    session = Session()

    pending_timeseries = session.\
        query(
            PendingTimeSeries.timeseries_id
        ).filter(
            and_(
                PendingTimeSeries.session_id == session_id,
                PendingTimeSeries.refts_id == refts_id
            )
        )

    pending_timeseries.delete()

    session.commit()
    session.close()


