import json
import os
import shutil
from django.http import JsonResponse
from django.conf import settings
from tethys_services.backends.hs_restclient_helper import get_oauth_hs
from .utilities import download_wml, download_cached_wml, get_app_workspace, \
    validate_wml, extract_wml, get_user_workspace, create_refts_file, create_odm2_database
from .model import get_timeseries_references, check_timeseries_status, \
    update_timeseries_reference, get_timeseries_reference, get_wml_data


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
