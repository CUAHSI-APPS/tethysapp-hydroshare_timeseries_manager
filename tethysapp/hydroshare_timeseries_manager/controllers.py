import uuid
import json
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from .app import HydroshareTimeseriesManager as app
from .utilities import process_form_data, add_refts_to_session, get_refts_from_hydroshare
from .model import check_timeseries_status


@csrf_exempt
def home(request):
    """
    Controller for the app home page.
    """

    session_id = str(uuid.uuid4())
    resource_id = ""
    hydroshare_url = app.get_custom_setting("hydroshare_url")

    if request.POST:
        session_id = str(uuid.uuid4())
        resource_id = ""
        form = json.dumps(request.POST)
        refts = process_form_data(form)
        add_refts_to_session(refts, session_id)

    elif request.GET:
        if request.GET.get("session_id") and not request.GET.get("resource_id"):
            session_id = request.GET.get("session_id")
            resource_id = ""
        elif request.GET.get("resource_id") and not request.GET.get("session_id"):
            session_id = str(uuid.uuid4())
            resource_id = request.GET.get("resource_id")
            refts = get_refts_from_hydroshare(resource_id)
            add_refts_to_session(refts, session_id)
        elif request.GET.get("session_id") and request.GET.get("resource_id"):
            session_id = request.GET.get("session_id")
            resource_id = request.GET.get("resource_id")
            refts_statuses = check_timeseries_status(session_id)
            if not refts_statuses:
                refts = get_refts_from_hydroshare(resource_id)
                add_refts_to_session(refts, session_id) 
        else:
            session_id = str(uuid.uuid4())
            resource_id = ""

    context = {
        "session_id": session_id,
        "resource_id": resource_id,
        "hydroshare_url": hydroshare_url
    }

    return render(request, 'hydroshare_timeseries_manager/home.html', context)
