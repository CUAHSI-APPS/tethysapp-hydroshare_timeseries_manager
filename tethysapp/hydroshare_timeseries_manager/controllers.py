import uuid
import json
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from .app import HydroshareTimeseriesManager as app
from .utilities import process_form_data

hydroshare_url = app.get_custom_setting("hydroshare_url")
hydroserver_url = app.get_custom_setting("hydroserver_url")


@csrf_exempt
def home(request):
    """
    Controller for the app home page.

    POST requests can be receieved from the CUAHSI HydroClient when exporting data to
    HydroShare. These requests include a form describing the reference data exported 
    from the HydroClient workspace, but should not include any URL query parameters.

    GET requests can be launched from HydroShare and may contain URL query parameters
    describing the resource or aggregation to retrieve data from.
    """

    session_id = str(uuid.uuid4())
    resource_id = request.GET.get("resource_id")
    aggregation_id = request.GET.get("aggregation_path")
    refts = None

    if request.POST:
        resource_id = None
        aggregation_id = None
        form = json.dumps(request.POST)
        refts = json.dumps(process_form_data(form))

    context = {
        "session_id": session_id,
        "resource_id": resource_id,
        "aggregation_id": aggregation_id,
        "refts": refts
    }

    return render(request, 'hydroshare_timeseries_manager/home.html', context)
