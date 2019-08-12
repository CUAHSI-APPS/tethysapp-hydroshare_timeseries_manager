import json
import uuid
from datetime import datetime
from django.template.defaultfilters import slugify
from django.http import JsonResponse
from .model import get_timeseries_references, update_timeseries_selections, remove_timeseries_references, \
                   get_timeseries_request_data, update_timeseries_reference, add_pending_timeseries, \
                   get_pending_timeseries, remove_pending_timeseries, get_resource_metadata
from .utilities import get_refts_from_hydroshare, add_refts_to_session, extract_soap_wml, validate_wml, \
                       extract_rest_wml, download_soap_wml, download_rest_wml, get_app_workspace, \
                       create_refts_file


def update_table(request):
    """
    Loads data for Datatables.

    This function handles Datatables server-side processing. Returns filtered paged results
    to the client to be displayed to the user.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('session-id')
    draw = request.POST.get('draw')
    search_value = request.POST.get('search[value]')
    length = request.POST.get('length')
    offset = request.POST.get('start')
    column = request.POST.get('order[0][column]')
    order = request.POST.get('order[0][dir]')
    selected = request.POST.get('selected')
    timeseries_id = request.POST.get('timeseries_id')

    # ---------------------- #
    #   GETS FILTERED DATA   #
    # ---------------------- #

    total_results, filtered_results, selected, results = get_timeseries_references(
        session_id=session_id,
        search_value=search_value,
        length=length,
        offset=offset,
        column=column,
        order=order
    )

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return_obj["draw"] = [int(draw)]
    return_obj["recordsTotal"] = total_results
    return_obj["recordsFiltered"] = filtered_results
    return_obj["recordsSelected"] = selected
    return_obj["data"] = results

    return JsonResponse(return_obj)


def update_selections(request):
    """
    Updates selected timeseries.

    This function toggles selected timeseries in a session. It can update one given a
    timeseries ID, or more given a search value.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('sessionId')
    search_value = request.POST.get('searchValue')
    selected = True if request.POST.get('selected') == 'true' else False
    timeseries_id = request.POST.get('timeseriesId')

    # ------------------------- #
    #   UPDATES SELECTED ROWS   #
    # ------------------------- #

    update_timeseries_selections(
        session_id=session_id, 
        timeseries_id=timeseries_id, 
        search_value=search_value, 
        selected=selected
    )

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return_obj["success"] = True

    return JsonResponse(return_obj)


def remove_timeseries(request):
    """
    Removes timeseries from a session.

    This function can remove one or more or all timeseries from a session
    using a timeseries ID or selected timeseries.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('sessionId')
    selected = True if request.POST.get('selected') == 'true' else False
    timeseries_id = request.POST.get('timeseriesId')

    # ------------------------- #
    #   UPDATES SELECTED ROWS   #
    # ------------------------- #

    remove_timeseries_references(
        session_id=session_id, 
        selected=selected, 
        timeseries_id=timeseries_id
    )

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return_obj["success"] = True

    return JsonResponse(return_obj)


def add_session_data(request):
    """
    Adds REFTS to a session.

    This function will load REFTS from HydroShare or a JSON object into the
    current session.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('sessionId')
    resource_id = request.POST.get('resourceId')
    aggregation_id = request.POST.get('aggregationId') if request.POST.get('aggregationId') != "None" else None
    refts_json = request.POST.get('reftsJson') if request.POST.get('reftsJson') != "None" else None

    # ------------------------- #
    #   ADDS REFTS TO SESSION   #
    # ------------------------- #

    refts_list = get_refts_from_hydroshare(resource_id, aggregation_id)
    timeseries_id_list = []
    try:
        refts_list.append(json.loads(refts_json))
    except:
        pass
    for refts in refts_list:
        timeseries_ids = add_refts_to_session(refts, session_id)
        timeseries_id_list.extend(timeseries_ids)

    # ---------------------------------- #
    #   ADDS TIMESERIES IDS TO PENDING   #
    # ---------------------------------- #

    refts_id = str(uuid.uuid4())
    for timeseries_id in timeseries_id_list:
        add_pending_timeseries(
            session_id=session_id,
            refts_id=refts_id,
            timeseries_id=timeseries_id
        )

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    if (resource_id or refts_json) and not timeseries_id_list:
        return_obj["success"] = False
        return_obj["refts_id"] = None
    elif (resource_id or refts_json) and timeseries_id_list:
        return_obj["success"] = True
        return_obj["refts_id"] = refts_id
    else:
        return_obj["success"] = True
        return_obj["refts_id"] = None

    return JsonResponse(return_obj)


def prepare_session_data(request):
    """
    Prepares session data.

    This function builds WaterOneFlow requests and begins downloading WaterML 
    data for a batch of time series references.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('sessionId')
    refts_id = request.POST.get('reftsId')

    # ----------------------------- #
    #   PREPARES WATERML REQUESTS   #
    # ----------------------------- #

    timeseries_id_list = list(sum(get_pending_timeseries(session_id, refts_id), ()))
    remove_pending_timeseries(session_id, refts_id)

    request_data_list = []
    for timeseries_id in timeseries_id_list:
        request_data = get_timeseries_request_data(session_id=session_id, timeseries_id=timeseries_id)
        update_timeseries_reference(session_id=session_id, timeseries_id=timeseries_id, status="Downloading")
        request_data_list.append(request_data)

    soap_url_list = [
        {
            "timeseries_id": x[0],
            "url": x[17],
            "version": "1.1" if x[15] == "WaterML 1.1" else "1.0",
            "location": x[8],
            "variable": x[12],
            "start_date": x[3],
            "end_date": x[4],
            "auth_token": ""
        } for x in request_data_list if x[16] == "SOAP"
    ]

    rest_url_list = [
        {
            "timeseries_id": x[0],
            "url": f"{x[17]}values/?site_code={x[8]}&variable_code={x[12]}&start_date={x[3]}&end_date={x[4]}"
        } for x in request_data_list if x[16] == "REST"
    ]

    # ---------------------------------------------- #
    #   DOWNLOADS, EXTRACTS, AND VALIDATES WATERML   #
    # ---------------------------------------------- #


    soap_response = download_soap_wml(soap_url_list)
    rest_response = download_rest_wml(rest_url_list)

    for result in soap_response:
        soap_data = result[0]
        request_data = get_timeseries_request_data(session_id=session_id, timeseries_id=result[1])
        wml_version = request_data[15]
        wml_data = extract_soap_wml(soap_data, wml_version, unzip=False)

        update_timeseries_reference(session_id=session_id, timeseries_id=result[1], wml_data=wml_data, status="Validating")

        if not validate_wml(session_id=session_id, timeseries_id=result[1], wml_version=wml_version):
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="Failed")
        else:
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="Ready")

    for result in rest_response:
        rest_data = result[0]
        wml_data = extract_rest_wml(rest_data, unzip=False)

        update_timeseries_reference(session_id=session_id, timeseries_id=result[1], wml_data=wml_data, status="Validating")

        if not validate_wml(session_id=session_id, timeseries_id=result[1], wml_version="WaterML 1.1"):
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="Failed")
        else:
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="Ready")

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return_obj["success"] = True

    return JsonResponse(return_obj)


def update_resource_metadata(request):
    """
    Updates default resource metadata.

    This function generates a default title, abstract, and keywords
    for a set of selected timeseries references.
    """

    return_obj = {}

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["error"] = "Unable to establish a secure connection with the server."

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get('sessionId')

    # ------------------------- #
    #   UPDATES SELECTED ROWS   #
    # ------------------------- #

    site_names, site_count, variable_names, variable_count, sample_mediums, start_date, end_date, invalid = get_resource_metadata(session_id)

    if invalid > 0:
        return_obj["success"] = False
        return_obj["message"] = "Please select only rows that are ready before creating a resource."
        return JsonResponse(return_obj)

    if site_count == 0 or variable_count == 0:
        return_obj["success"] = False
        return_obj["message"] = "Please select at least one row."
        return JsonResponse(return_obj)

    site_names = list(sum(site_names, ()))
    variable_names = list(sum(variable_names, ()))
    sample_mediums = list(sum(sample_mediums, ()))

    site_s = "s" if len(site_names) > 1 else ""
    date = datetime.today().strftime('%b %-d, %Y')
    if site_count > 5:
        sites = ", ".join(site_names) + f", and {site_count - 5} more site{'' if site_count - 5 == 1 else 's'}"
    else:
        sites = ", ".join(site_names)
    if len(variable_names) == 1:
        variables = variable_names[0]
    elif len(variable_names) == 2:
        variables = " and ".join(variable_names)
    elif len(variable_names) > 2 and len(variable_names) < 6:
        variables = ", ".join(variable_names[:-1]) + ", and " + variable_names[-1]
    elif variable_count > 5:
        variables = ", ".join(variable_names) + f", and {variable_count - 5} more variable" 
    else:
        variables = ""

    res_title = f"Time series dataset created on {date} by the HydroShare Time Series Manager"
    res_abstract = f"{variables} data collected from {start_date[0].strftime('%x')} to {end_date[0].strftime('%x')} at the following site{site_s}: {sites}. Data compiled by the HydroShare Time Series Manager on {date}"
    res_keywords = site_names + variable_names + sample_mediums
    res_filename = slugify(f"{variable_names[0]}-at-{site_names[0]}")[0:40]

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return_obj["success"] = True
    return_obj["res_title"] = res_title
    return_obj["res_abstract"] = res_abstract
    return_obj["res_keywords"] = res_keywords
    return_obj["res_filename"] = res_filename

    return JsonResponse(return_obj)


def create_resource(request):
    """
    Creates a HydroShare Resource

    This function creates a HydroShare resource based on user defined parameters.
    """

    print("STARTING")

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get("sessionId")
    res_title = request.POST.get("resTitle")
    res_abstract = request.POST.get("resAbstract")
    res_keywords = request.POST.get("resKeywords").split(", ")
    res_filename = slugify(request.POST.get("resFilename"))[0:40]
    create_ts = request.POST.get("createTs")
    create_refts = request.POST.get("createRefts")
    create_public = request.POST.get("createPublic")

    refts_metadata = {
        "res_title": res_title,
        "res_abstract": res_abstract,
        "res_keywords": res_keywords,
        "res_filename": res_filename
    }

    workspace = get_app_workspace(request)
    try:
        shutil.rmtree("/".join((workspace, session_id,)))
    except:
        pass
    os.mkdir("/".join((workspace, session_id,)))

    # ---------------------- #
    #   CREATES REFTS FILE   #
    # ---------------------- #

    if create_refts:
        try:
            refts_path = create_refts_file(session_id, timeseries_ids, workspace, refts_metadata)
        except:
            refts_path = None
            pass
    else:
        refts_path = None

    # --------------------------------- #
    #   LOADS DATA INTO ODM2 DATABASE   #
    # --------------------------------- #
    '''
    if create_ts:
        if True:
            odm2_path = create_odm2_database(session_id, timeseries_ids, workspace, refts_metadata)
        else:
            odm2_path = None
            pass
    else:
        odm2_path = None

    # ------------------------------- #
    #   CREATES HYDROSHARE RESOURCE   #
    # ------------------------------- #

    hs_api = get_oauth_hs(request)

    if not refts_path and not odm2_path:
        pass
    
    resource_id = hs_api.createResource("CompositeResource", res_title, abstract=res_abstract, keywords=res_keywords)

    if refts_path:
        hs_api.addResourceFile(resource_id, resource_file=refts_path)
        #with open(refts_path, "rb") as res_file:
        #    hs_api.addResourceFile(resource_id, resource_file=res_file)
        #res_file.close()

    if odm2_path:
        hs_api.addResourceFile(resource_id, resource_file=odm2_path)
    '''
    return_obj["success"] = True

    return JsonResponse(return_obj)
    


































'''
import json
import os
import shutil
from django.http import JsonResponse
from django.conf import settings
from tethys_services.backends.hs_restclient_helper import get_oauth_hs
from .utilities import download_wml, download_cached_wml, get_app_workspace, \
    validate_wml, extract_wml, create_refts_file, create_odm2_database
from .model import get_timeseries_references, check_timeseries_status, \
    update_timeseries_reference, get_timeseries_reference, get_wml_data
'''
'''
def login_test(request):
    """
    Login Test AJAX Controller.

    This function checks if a user is logged in to HydroShare.
    """

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # ----------------------------------- #
    #   CHECKS IF USER IS AUTHENTICATED   #
    # ----------------------------------- #

    if request.user.is_authenticated():
        return_obj["success"] = True

    else:
        return_obj["success"] = False

    # -------------------- #
    #   RETURNS RESPONSE   #
    # -------------------- #

    return JsonResponse(return_obj)


def load_session_data(request):
    """
    Loads Session Data.

    Given a session ID, this function returns a list of session data objects 
    not already present in a given client session.
    """

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get("sessionId")
    timeseries_ids = request.POST.get("timeSeriesIds")
    if not timeseries_ids:
        timeseries_ids = []

    # --------------------- #
    #   GETS SESSION DATA   #
    # --------------------- #

    timeseries_references = get_timeseries_references(session_id)
    new_timeseries_references = [{
        "timeseries_id": x[0],
        "selected": x[1],
        "begin_date": x[2],
        "end_date": x[3],
        "value_count": x[4],
        "sample_medium": x[5],
        "site_name": x[6],
        "site_code": x[7],
        "latitude": x[8],
        "longitude": x[9],
        "variable_name": x[10],
        "variable_code": x[11],
        "method_description": x[12],
        "method_link": x[13],
        "network_name": x[14],
        "ref_type": x[15],
        "return_type": x[16],
        "service_type": x[17],
        "url": x[18]
    } for x in timeseries_references if x[0] not in timeseries_ids]

    return_obj["success"] = True
    return_obj["results"] = new_timeseries_references

    return JsonResponse(return_obj)


def prepare_timeseries_data(request):
    """
    Prepare time series data AJAX Controller.

    This function downloads and validates time series data.
    """

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get("sessionId")
    rows = json.loads(request.POST.get("rows"))
    timeseries_ids = [row["timeseries_id"] for row in rows]

    # -------------------------- #
    #   UPDATES CURRENT STATUS   #
    # -------------------------- #

    timeseries_status_list = check_timeseries_status(session_id)
    requested_timeseries_status_list = [i for i in timeseries_status_list if i[0] in timeseries_ids]
    timeseries_process_list = [i for i in requested_timeseries_status_list if i[1] == "WAITING"]

    for ts in timeseries_process_list:
        update_timeseries_reference(session_id=session_id, timeseries_id=ts[0], status="PROCESSING")

    # ----------------------------- #
    #   PREPARES WATERML REQUESTS   #
    # ----------------------------- #

    refts_list = []
    for ts in timeseries_process_list:
        refts = get_timeseries_reference(session_id=session_id, timeseries_id=ts[0])
        refts_list.append(refts)

    refts_list_cached = [i for i in refts_list if i[19]]
    refts_list_uncached = [i for i in refts_list if not i[19]]

    # ----------------------------------------------------- #
    #   DOWNLOADS, EXTRACTS, AND VALIDATES CACHED WATERML   #
    # ----------------------------------------------------- #

    cached_uri_list = [
        {
            "timeseries_id": x[0],
            "cache_uri": x[19]
        } for x in refts_list_cached
    ]

    response = download_cached_wml(cached_uri_list)

    for result in response:
        soap_data = result[0]
        refts = get_timeseries_reference(session_id=session_id, timeseries_id=result[1])
        wml_version = refts[16]
        wml_data = extract_wml(soap_data, wml_version, unzip=True)

        update_timeseries_reference(session_id=session_id, timeseries_id=result[1], wml_data=wml_data)

        if not validate_wml(session_id=session_id, timeseries_id=result[1], wml_version=wml_version):
            refts_list_uncached.append(refts)
        else:
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="SUCCESS")

    # ------------------------------------------------------- #
    #   DOWNLOADS, EXTRACTS, AND VALIDATES UNCACHED WATERML   #
    # ------------------------------------------------------- #

    url_list = [
        {
            "timeseries_id": x[0],
            "url": x[18],
            "version": "1.1" if x[16] == "WaterML 1.1" else "1.0",
            "location": x[7],
            "variable": x[11],
            "start_date": x[2],
            "end_date": x[3],
            "auth_token": ""
        } for x in refts_list_uncached
    ]

    response = download_wml(url_list)

    for result in response:
        soap_data = result[0]
        refts = get_timeseries_reference(session_id=session_id, timeseries_id=result[1])
        wml_version = refts[16]
        wml_data = extract_wml(soap_data, wml_version, unzip=False)

        update_timeseries_reference(session_id=session_id, timeseries_id=result[1], wml_data=wml_data)

        if not validate_wml(session_id=session_id, timeseries_id=result[1], wml_version=wml_version):
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="FAILURE")
        else:
            update_timeseries_reference(session_id=session_id, timeseries_id=result[1], status="SUCCESS")

    # ---------------------------- #
    #   RETURNS STATUS TO CLIENT   #
    # ---------------------------- #

    return_obj["success"] = True

    print("FINISHED")

    return JsonResponse({})


def ajax_check_timeseries_status(request):
    """
    Checks Time Series Statuses.

    This function checks the status of each time series currently being prepared.
    """

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get("sessionId")

    # ------------------------------- #
    #   CHECKS TIME SERIES STATUSES   #
    # ------------------------------- #

    timeseries_statuses = check_timeseries_status(session_id)

    return_obj["success"] = True
    return_obj["results"] = timeseries_statuses

    return JsonResponse(return_obj)


def ajax_create_hydroshare_resource(request):
    """
    Creates a HydroShare Resource

    This function creates a HydroShare resource based on user defined parameters.
    """

    print("STARTING")

    return_obj = {
        "success": False,
        "message": None,
        "results": {}
    }

    # -------------------- #
    #   VERIFIES REQUEST   #
    # -------------------- #

    if not (request.is_ajax() and request.method == "POST"):
        return_obj["success"] = False
        return_obj["message"] = "Unable to communicate with server."
        return_obj["results"] = {}

        return JsonResponse(return_obj)

    # -------------------------- #
    #   GETS DATA FROM REQUEST   #
    # -------------------------- #

    session_id = request.POST.get("sessionId")
    timeseries_ids = json.loads(request.POST.get("timeSeriesIds"))
    res_title = request.POST.get("resTitle")
    res_abstract = request.POST.get("resAbstract")
    res_keywords = request.POST.get("resKeywords").split(", ")
    create_ts = request.POST.get("createTs")
    create_refts = request.POST.get("createRefts")
    create_public = request.POST.get("createPublic")

    refts_metadata = {
        "res_title": res_title,
        "res_abstract": res_abstract,
        "res_keywords": res_keywords
    }

    workspace = get_user_workspace(request)
    try:
        shutil.rmtree("/".join((workspace, session_id,)))
    except:
        pass
    os.mkdir("/".join((workspace, session_id,)))

    # ---------------------- #
    #   CREATES REFTS FILE   #
    # ---------------------- #

    if create_refts:
        try:
            refts_path = create_refts_file(session_id, timeseries_ids, workspace, refts_metadata)
        except:
            refts_path = None
            pass
    else:
        refts_path = None

    # --------------------------------- #
    #   LOADS DATA INTO ODM2 DATABASE   #
    # --------------------------------- #

    if create_ts:
        if True:
            odm2_path = create_odm2_database(session_id, timeseries_ids, workspace, refts_metadata)
        else:
            odm2_path = None
            pass
    else:
        odm2_path = None

    # ------------------------------- #
    #   CREATES HYDROSHARE RESOURCE   #
    # ------------------------------- #

    hs_api = get_oauth_hs(request)

    if not refts_path and not odm2_path:
        pass
    
    resource_id = hs_api.createResource("CompositeResource", res_title, abstract=res_abstract, keywords=res_keywords)

    if refts_path:
        hs_api.addResourceFile(resource_id, resource_file=refts_path)
        #with open(refts_path, "rb") as res_file:
        #    hs_api.addResourceFile(resource_id, resource_file=res_file)
        #res_file.close()

    if odm2_path:
        hs_api.addResourceFile(resource_id, resource_file=odm2_path)

    return_obj["success"] = True

    return JsonResponse(return_obj)
'''