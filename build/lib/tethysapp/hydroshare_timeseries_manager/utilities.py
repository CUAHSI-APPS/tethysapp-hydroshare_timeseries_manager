import json
import uuid
import asyncio
import zipfile
import io
import os
import shutil
import sqlite3
import itertools
from aiohttp import ClientSession
from lxml import etree
from .app import HydroshareTimeseriesManager as app
from .model import add_timeseries_reference, get_wml_data, get_timeseries_reference


class d(dict):
    def __missing__(self, key):
        value = self[key] = type(self)()
        return ""


def get_user_workspace(request):
    """
    Gets user workspace path.
    """

    user_workspace = app.get_user_workspace(request).path

    return user_workspace


def get_app_workspace():
    """
    Gets app workspace path.
    """

    app_workspace = app.get_app_workspace().path

    return app_workspace


def process_form_data(form):
    """
    Converts a POST form to a Referenced Time Series object
    """

    if isinstance(json.loads(form)["timeSeriesReferenceFile"], dict):
        json_form = json.loads(form)
    elif isinstance(json.loads(form)["timeSeriesReferenceFile"], str):
        json_form = {"timeSeriesReferenceFile": json.loads(json.loads(form)["timeSeriesReferenceFile"])}

    refts = {"timeSeriesReferenceFile": {}}
    refts["timeSeriesReferenceFile"]["title"] = None
    refts["timeSeriesReferenceFile"]["abstract"] = None
    refts["timeSeriesReferenceFile"]["keywords"] = None
    refts["timeSeriesReferenceFile"]["fileVersion"] = "1.0.0"
    refts["timeSeriesReferenceFile"]["symbol"] = None
    refts["timeSeriesReferenceFile"]["referencedTimeSeries"] = []
    for entry in json_form["timeSeriesReferenceFile"]["referencedTimeSeries"]:
        ts = {}
        ts["beginDate"] = d(entry)["beginDate"]
        ts["endDate"] = d(entry)["endDate"]
        ts["valueCount"] = d(entry)["valueCount"]
        ts["sampleMedium"] = d(entry)["sampleMedium"]
        ts["site"] = {}
        ts["site"]["siteName"] = d(d(entry)["site"])["siteName"]
        ts["site"]["siteCode"] = d(d(entry)["site"])["siteCode"]
        ts["site"]["latitude"] = d(d(entry)["site"])["latitude"]
        ts["site"]["longitude"] = d(d(entry)["site"])["longitude"]
        ts["variable"] = {}
        ts["variable"]["variableName"] = d(d(entry)["variable"])["variableName"]
        ts["variable"]["variableCode"] = d(d(entry)["variable"])["variableCode"]
        ts["method"] = {}
        ts["method"]["methodDescription"] = d(d(entry)["method"])["methodDescription"]
        ts["method"]["methodLink"] = d(d(entry)["method"])["methodLink"]
        ts["requestInfo"] = {}
        ts["requestInfo"]["networkName"] = d(d(entry)["requestInfo"])["networkName"]
        ts["requestInfo"]["refType"] = d(d(entry)["requestInfo"])["refType"]
        ts["requestInfo"]["returnType"] = d(d(entry)["requestInfo"])["returnType"]
        ts["requestInfo"]["serviceType"] = d(d(entry)["requestInfo"])["serviceType"]
        ts["requestInfo"]["url"] = d(d(entry)["requestInfo"])["url"]
        ts["cacheUri"] = d(d(entry)["wofParams"])["WofUri"]
        refts["timeSeriesReferenceFile"]["referencedTimeSeries"].append(ts)

    return refts


def add_refts_to_session(refts, session_id):
    """
    Adds a referenced time series object to a session
    """

    for ts in refts["timeSeriesReferenceFile"]["referencedTimeSeries"]:
        try:
            add_timeseries_reference(
                session_id=session_id,
                timeseries_id=str(uuid.uuid4()),
                begin_date=str(d(ts)["beginDate"]),
                end_date=str(d(ts)["endDate"]),
                value_count=str(d(ts)["valueCount"]),
                sample_medium=str(d(ts)["sampleMedium"]),
                site_name=str(d(ts)["site"]["siteName"]),
                site_code=str(d(ts)["site"]["siteCode"]),
                latitude=str(d(ts)["site"]["latitude"]),
                longitude=str(d(ts)["site"]["longitude"]),
                variable_name=str(d(ts)["variable"]["variableName"]),
                variable_code=str(d(ts)["variable"]["variableCode"]),
                method_description=str(d(ts)["method"]["methodDescription"]),
                method_link=str(d(ts)["method"]["methodLink"]),
                network_name=str(d(ts)["requestInfo"]["networkName"]),
                ref_type=str(d(ts)["requestInfo"]["refType"]),
                return_type=str(d(ts)["requestInfo"]["returnType"]),
                service_type=str(d(ts)["requestInfo"]["serviceType"]),
                url=str(d(ts)["requestInfo"]["url"]),
                cache_uri=str(d(ts)["cacheUri"])
            )
        except:
            pass


def get_refts_from_hydroshare(resource_id):
    """
    Gets Reference Time Series data from a HydroShare resource
    """

    pass


def download_cached_wml(refts_list):
    async def fetch_all(refts_list):
        tasks = []
        async with ClientSession() as session:
            for refts in refts_list:
                task = asyncio.ensure_future(fetch(refts, session))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    async def fetch(refts, session):
        try:
            async with session.get(f"https://qa-hiswebclient.azurewebsites.net/CUAHSI/HydroClient/WaterOneFlowArchive/{refts['cache_uri']}/") as response:
                response = await response.read()
                return (response, refts["timeseries_id"],)
        except:
            return (b"", refts["timeseries_id"],)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(fetch_all(refts_list))
    result = loop.run_until_complete(future)
    return result


def download_wml(refts_list):
    async def fetch_all(refts_list):
        tasks = []
        async with ClientSession() as session:
            for refts in refts_list:
                task = asyncio.ensure_future(fetch(refts, session))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
            return responses

    async def fetch(refts, session):
        try:
            async with session.post(
                    refts['url'],
                    headers = {
                        "SOAPAction": f"http://www.cuahsi.org/his/{refts['version']}/ws/GetValuesObject",
                        "Content-Type": "text/xml; charset=utf-8"
                    },
                    data = f'''
                        <soap-env:Envelope xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
                          <soap-env:Body>
                            <ns0:GetValuesObject xmlns:ns0="http://www.cuahsi.org/his/{refts['version']}/ws/">
                              <ns0:location>{refts['location']}</ns0:location>
                              <ns0:variable>{refts['variable']}</ns0:variable>
                              <ns0:startDate>{refts['start_date']}</ns0:startDate>
                              <ns0:endDate>{refts['end_date']}</ns0:endDate>
                              <ns0:authToken>{refts['auth_token']}</ns0:authToken>
                            </ns0:GetValuesObject>
                          </soap-env:Body>
                        </soap-env:Envelope>
                    '''
                ) as response:
                response = await response.read()
                return (response, refts["timeseries_id"],)
        except:
            return (b"", refts["timeseries_id"],)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    future = asyncio.ensure_future(fetch_all(refts_list))
    result = loop.run_until_complete(future)
    return result


def extract_wml(soap_data, wml_version, unzip):

    if unzip:
        try:
            extracted_data = zipfile.ZipFile(io.BytesIO(soap_data), "r")
            for finfo in extracted_data.infolist():
                ifile = extracted_data.open(finfo)
                line_list = ifile.readlines()
                soap_data = b''.join(line_list)
        except:
            soap_data = soap_data

    soap_object = etree.fromstring(soap_data)

    if wml_version == "WaterML 1.0":
        wml_data_gen = soap_object.iter("{http://www.cuahsi.org/waterML/1.0/}timeSeriesResponse")

    elif wml_version == "WaterML 1.1":
        wml_data_gen = soap_object.iter("{http://www.cuahsi.org/waterML/1.1/}timeSeriesResponse")

    wml_data = etree.tostring(next(wml_data_gen))

    return wml_data


def validate_wml(session_id, timeseries_id, wml_version):

    wml_data = get_wml_data(session_id=session_id, timeseries_id=timeseries_id)

    wml_version = "1" if "WaterML 1.1" in wml_version else "0"

    wml_schema = etree.XMLSchema(etree.parse(f"{get_app_workspace()}/wml_1_{wml_version}_schema.xsd"))

    try:
        wml_schema.assertValid(etree.fromstring(wml_data[0]))
        return True
    except etree.DocumentInvalid as err:
        error_list = str(err.error_log).split("<string>")[1:]
        error_list = [error for error in error_list if 
            "This element is not expected." not in error and
            "timeOffset" not in error
        ]

        if not error_list:
            return True
        else:
            print("**************************************")
            print("\n".join(error_list))
            print("**************************************")
            return False
    except:
        print("**************************************")
        print("Unknown Validation Error")
        print("**************************************")
        return False


def create_refts_file(session_id, timeseries_ids, workspace, refts_metadata):

    refts_data = [get_timeseries_reference(session_id=session_id, timeseries_id=i) for i in timeseries_ids]
    refts_filepath = "/".join((workspace, session_id, "timeseries.refts.json",))

    refts_list = [
        {
            "beginDate": i[2] if i[2] != "" else None,
            "endDate": i[3] if i[3] != "" else None,
            "sampleMedium": i[5] if i[5] != "" else None,
            "valueCount": i[4] if i[4] != "" else None,
            "site": {
                "siteName": i[6] if i[6] != "" else None,
                "siteCode": i[7] if i[7] != "" else None,
                "latitude": i[8] if i[8] != "" else None,
                "longitude": i[9] if i[9] != "" else None
            },
            "variable": {
                "variableName": i[10] if i[10] != "" else None,
                "variableCode": i[11] if i[11] != "" else None
            },
            "method": {
                "methodLink": i[12] if i[12] != "" else None,
                "methodDescription": i[13] if i[13] != "" else None
            },
            "requestInfo": {
                "refType": i[15] if i[15] != "" else None,
                "serviceType": i[17] if i[17] != "" else None,
                "url": i[18] if i[18] != "" else None,
                "returnType": i[16] if i[16] != "" else None,
                "networkName": i[14] if i[14] != "" else None
            }

        } for i in refts_data
    ]

    refts = {
        "timeSeriesReferenceFile": {
            "fileVersion": "1.0.0",
            "title": refts_metadata["res_title"],
            "keyWords": refts_metadata["res_keywords"],
            "abstract": refts_metadata["res_abstract"],
            "symbol": "https://www.hydroshare.org/static/img/logo-lg.png",
            "referencedTimeSeries": refts_list
        }
    }

    with open(refts_filepath, 'w') as refts_file:
        json.dump(refts, refts_file, sort_keys=True, indent=4, separators=(',', ': '))

    return refts_filepath


def search_wml(unique_code, ns, tag_names, default_value=None, attr=None, get_tree=False, mult=False):
    if unique_code is None:
        return default_value
    if get_tree:
        for tag_name in tag_names:
            if list(unique_code.iter(ns + tag_name)) and mult:
                tree = list(unique_code.iter(ns + tag_name))
            elif list(unique_code.iter(ns + tag_name)) and not mult:
                tree = list(unique_code.iter(ns + tag_name))[0]
            elif not list(unique_code.iter(ns + tag_name)) and mult:
                tree = []
            elif not list(unique_code.iter(ns + tag_name)) and not mult:
                tree = None
            else:
                tree = None
            if tree != None and tree != []:
                return tree
        return tree
    else:
        for tag_name in tag_names:
            if list(unique_code.iter(ns + tag_name)) and not mult and attr == None:
                tag_value = list(unique_code.iter(ns + tag_name))[0].text
            elif list(unique_code.iter(ns + tag_name)) and not mult and attr != None:
                tag_value = list(unique_code.iter(ns + tag_name))[0].get(attr)
            elif list(unique_code.iter(ns + tag_name)) and mult and attr == None:
                tag_value = [i.text for i in list(unique_code.iter(ns + tag_name))]
            elif list(unique_code.iter(ns + tag_name)) and mult and attr != None:
                tag_value = [i.get(attr) for i in list(unique_code.iter(ns + tag_name))]
            elif not list(unique_code.iter(ns + tag_name)) and not mult:
                tag_value = None
            elif not list(unique_code.iter(ns + tag_name)) and mult:
                tag_value = []
            else:
                tag_value = None
            if tag_value != None and tag_value != []:
                return tag_value
        return default_value


def create_odm2_database(session_id, timeseries_ids, workspace, refts_metadata):

    current_path = os.path.dirname(os.path.realpath(__file__))
    odm_filepath = "/".join((workspace, session_id, "timeseries.odm2.sqlite",))
    odm_master = os.path.join(current_path, "workspaces", "app_workspace","ODM2_master.sqlite")
    shutil.copy(odm_master, odm_filepath)
    sql_connect = sqlite3.connect(odm_filepath, isolation_level=None)
    curs = sql_connect.cursor()

    # ------------------------------------ #
    #   Extracts Data for Datasets Table   #
    # ------------------------------------ #

    dataset = (
        str(uuid.uuid4()),
        ("singleTimeSeries" if len(timeseries_ids) == 1 else "multiTimeSeries"),
        1,
        refts_metadata["res_title"],
        refts_metadata["res_abstract"],
    )

    curs.execute(f"""INSERT INTO Datasets (
                        DataSetID, 
                        DataSetUUID, 
                        DataSetTypeCV, 
                        DataSetCode,
                        DataSetTitle, 
                        DataSetAbstract
                    ) VALUES (NULL, ?, ?, ?, ?, ?)""", dataset)

    for timeseries_id in timeseries_ids:
        print(timeseries_id)
        wml_data = get_wml_data(session_id=session_id, timeseries_id=timeseries_id)
        wml_tree = etree.fromstring(wml_data[0])
        ns = "{http://www.cuahsi.org/waterML/1.1/}" if wml_data[1] == "WaterML 1.1" else "{http://www.cuahsi.org/waterML/1.0/}"

        # -------------------------------------------- #
        #   Extracts Data for SamplingFeatures Table   #
        # -------------------------------------------- #

        sf_tree = search_wml(wml_tree, ns, ["sourceInfo"], get_tree=True)
        sampling_feature_code = search_wml(sf_tree, ns, ["siteCode"], default_value=None)
        if sampling_feature_code:
            curs.execute("SELECT * FROM SamplingFeatures WHERE SamplingFeatureCode = ?", (sampling_feature_code,))
            row = curs.fetchone()
            if not row:
                sampling_feature = (
                    str(uuid.uuid4()),
                    "site",
                    sampling_feature_code,
                    search_wml(sf_tree, ns, ["siteName"], default_value=None),
                    None,
                    "point",
                    None,
                    f'POINT ("{search_wml(sf_tree, ns, ["latitude"], default_value=None)}" "{search_wml(sf_tree, ns, ["longitude"], default_value=None)}")',
                    search_wml(sf_tree, ns, ["elevation_m"], default_value=None),
                    search_wml(sf_tree, ns, ["verticalDatum"], default_value=None),
                )
                curs.execute("""INSERT INTO SamplingFeatures (
                                    SamplingFeatureID, 
                                    SamplingFeatureUUID,
                                    SamplingFeatureTypeCV, 
                                    SamplingFeatureCode, 
                                    SamplingFeatureName,
                                    SamplingFeatureDescription, 
                                    SamplingFeatureGeotypeCV, 
                                    FeatureGeometry,
                                    FeatureGeometryWKT, 
                                    Elevation_m, 
                                    ElevationDatumCV
                                ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", sampling_feature)
                sampling_feature_id = curs.lastrowid
            else:
                sampling_feature_id = row[0]
        else:
            print("SF Failed")
            sql_connect.rollback()
            continue

        # --------------------------------------------- #
        #   Extracts Data for SpatialReferences Table   #
        # --------------------------------------------- #

        srs_code = search_wml(sf_tree, ns, ["geogLocation"], default_value="EPSG:4269", attr="srs")
        curs.execute("SELECT * FROM SpatialReferences WHERE SRSCode = ?", (srs_code,))
        row = curs.fetchone()
        if not row:
            spatial_reference = (
                srs_code, 
                srs_code, 
                None,
                None,
            )
            curs.execute("""INSERT INTO SpatialReferences(
                            SpatialReferenceID, 
                            SRSCode, 
                            SRSName,
                            SRSDescription, 
                            SRSLink
                        ) VALUES (NULL, ?, ?, ?, ?)""", spatial_reference)
            spatial_reference_id = curs.lastrowid
        else:
            spatial_reference_id = row[0]

        # --------------------------------- #
        #   Extracts Data for Sites Table   #
        # --------------------------------- #

        curs.execute("SELECT * FROM Sites WHERE SamplingFeatureID = ?", (sampling_feature_id,))
        row = curs.fetchone()
        if not row:
            site = (
                sampling_feature_id,
                "unknown",
                search_wml(sf_tree, ns, ["latitude"], default_value=None),
                search_wml(sf_tree, ns, ["longitude"], default_value=None),
                spatial_reference_id,
            )
            curs.execute("""INSERT INTO Sites(
                            SamplingFeatureID, 
                            SiteTypeCV, 
                            Latitude, 
                            Longitude,
                            SpatialReferenceID
                        ) VALUES (?, ?, ?, ?, ?)""", site)
            site_id = curs.lastrowid
        else:
            site_id = row[0]

        # ------------------------------------- #
        #   Extracts Data for Variables Table   #
        # ------------------------------------- #

        vr_tree = search_wml(wml_tree, ns, ["variable"], get_tree=True)
        variable_code = search_wml(vr_tree, ns, ["variableCode", "VariableCode"], default_value=None)
        if variable_code:
            curs.execute("SELECT * FROM Variables WHERE VariableCode = ?", (variable_code,))
            row = curs.fetchone()
            if not row:
                variable = (
                    "Unknown", 
                    variable_code, 
                    search_wml(vr_tree, ns, ["variableName", "VariableName"], default_value="Unknown"),
                    search_wml(vr_tree, ns, ["variableDescription", "VariableDescription"], default_value=None),
                    search_wml(vr_tree, ns, ["speciation", "Speciation"], default_value=None),
                    search_wml(vr_tree, ns, ["noDataValue", "NoDataValue"], default_value=-9999),
                )
                curs.execute("""INSERT INTO Variables (
                                VariableID, 
                                VariableTypeCV, 
                                VariableCode, 
                                VariableNameCV, 
                                VariableDefinition, 
                                SpeciationCV, 
                                NoDataValue 
                            ) VALUES (NULL, ?, ?, ?, ?, ?, ?)""", variable)
                variable_id = curs.lastrowid
            else:
                variable_id = row[0]
        else:
            print("VR Failed")
            sql_connect.rollback()
            continue

        # --------------------------------- #
        #   Extracts Data for Units Table   #
        # --------------------------------- #

        ut_tree = search_wml(vr_tree, ns, ["unit"], get_tree=True)
        unit_code = search_wml(ut_tree, ns, ["unitCode", "UnitCode", "unitsCode", "UnitsCode"], default_value=9999)
        curs.execute("SELECT * FROM Units WHERE UnitsID = ?", (unit_code,))
        row = curs.fetchone()
        if not row:
            unit = (
                unit_code,
                search_wml(ut_tree, ns, ["unitType", "unitsType", "UnitType", "UnitsType"], default_value="other") if unit_code != 9999 else "other",
                search_wml(ut_tree, ns, ["unitAbbreviation", "unitsAbbreviation", "UnitAbbreviation", "UnitsAbbreviation"], default_value="unknown") if unit_code != 9999 else "unknown",
                search_wml(ut_tree, ns, ["unitName", "unitsName", "UnitName", "UnitsName"], default_value="unknown") if unit_code != 9999 else "unknown",
                search_wml(ut_tree, ns, ["unitLink", "unitsLink", "UnitLink", "UnitsLink"], default_value=None) if unit_code != 9999 else None,
            )
            curs.execute("""INSERT INTO Units (
                            UnitsID, 
                            UnitsTypeCV, 
                            UnitsAbbreviation, 
                            UnitsName,
                            UnitsLink
                        ) VALUES (?, ?, ?, ?, ?)""", unit)
            unit_id = curs.lastrowid
        else:
            unit_id = row[0]

        # ------------------------------------------ #
        #    Extracts Data for Time Spacing Units    #
        # ------------------------------------------ #

        tu_tree = search_wml(vr_tree, ns, ["timeScale"], get_tree=True)
        time_unit_code = search_wml(tu_tree, ns, ["unitCode", "UnitCode", "unitsCode", "UnitsCode"], default_value=9999)
        curs.execute("SELECT * FROM Units WHERE UnitsID = ?", (time_unit_code,))
        row = curs.fetchone()
        if not row:
            time_unit = (
                time_unit_code,
                search_wml(tu_tree, ns, ["unitType", "unitsType", "UnitType", "UnitsType"], default_value="other") if time_unit_code != 9999 else "other",
                search_wml(tu_tree, ns, ["unitAbbreviation", "unitsAbbreviation", "UnitAbbreviation", "UnitsAbbreviation"], default_value="unknown") if time_unit_code != 9999 else "unknown",
                search_wml(tu_tree, ns, ["unitName", "unitsName", "UnitName", "UnitsName"], default_value="unknown") if time_unit_code != 9999 else "unknown",
                search_wml(tu_tree, ns, ["unitLink", "unitsLink", "UnitLink", "UnitsLink"], default_value=None) if time_unit_code != 9999 else None,
            )
            curs.execute("""INSERT INTO Units (
                            UnitsID, 
                            UnitsTypeCV, 
                            UnitsAbbreviation, 
                            UnitsName,
                            UnitsLink
                        ) VALUES (?, ?, ?, ?, ?)""", time_unit)
            time_unit_id = curs.lastrowid
        else:
            time_unit_id = row[0]

        # ------------------------------------------------------------------- #
        #   Extracts Data for People, Organizations, and Affiliations Table   #
        # ------------------------------------------------------------------- #

        sr_tree = search_wml(wml_tree, ns, ["source"], get_tree=True)
        person_name = search_wml(sr_tree, ns, ["contactName"], default_value="unknown")
        curs.execute("SELECT * FROM People WHERE PersonFirstName = ?", (person_name,))
        row = curs.fetchone()
        if not row:
            person = (
                person_name,
                " ",
            )
            curs.execute("""INSERT INTO People (
                            PersonID, 
                            PersonFirstName, 
                            PersonLastName
                        ) VALUES (NULL, ?, ?)""", person)
            person_id = curs.lastrowid
        else:
            person_id = row[0]
        organization_code = search_wml(sr_tree, ns, ["sourceCode"], default_value="unknown")
        curs.execute("SELECT * FROM Organizations WHERE OrganizationCode = ?", (organization_code,))
        row = curs.fetchone()
        if not row:
            organization = (
                "unknown",
                organization_code,
                search_wml(sr_tree, ns, ["organization"], default_value="unknown") if organization_code != "unknown" else "unknown",
                search_wml(sr_tree, ns, ["sourceDescription"], default_value=None) if organization_code != "unknown" else None,
                search_wml(sr_tree, ns, ["sourceLink"], default_value=None) if organization_code != "unknown" else None,
            )
            curs.execute("""INSERT INTO Organizations (
                            OrganizationID, 
                            OrganizationTypeCV,
                            OrganizationCode, 
                            OrganizationName, 
                            OrganizationDescription, 
                            OrganizationLink
                        ) VALUES (NULL, ?, ?, ?, ?, ?)""", organization)
            organization_id = curs.lastrowid
        else:
            organization_id = row[0]
        curs.execute("SELECT * FROM Affiliations WHERE PersonID = ? AND OrganizationID = ?", (person_id, organization_id,))
        row = curs.fetchone()
        if not row:
            affiliation = (
                person_id,
                organization_id,
                "unknown",
                search_wml(sr_tree, ns, ["phone"], default_value=None),
                search_wml(sr_tree, ns, ["email"], default_value="unknown"),
                search_wml(sr_tree, ns, ["address"], default_value=None),
            )
            curs.execute("""INSERT INTO Affiliations (
                            AffiliationID, 
                            PersonID, 
                            OrganizationID,
                            AffiliationStartDate, 
                            PrimaryPhone, 
                            PrimaryEmail,
                            PrimaryAddress
                        ) VALUES (NULL, ?, ?, ?, ?, ?, ?)""", affiliation)
            affiliation_id = curs.lastrowid
        else:
            affiliation_id = row[0]

        # -------------------------------------------- #
        #   Extracts Data for ProcessingLevels Table   #
        # -------------------------------------------- #

        pl_trees = search_wml(wml_tree, ns, ["qualityControlLevel"], get_tree=True, mult=True)
        processing_level_data_list = [{"processing_level_code": search_wml(pl_tree, ns, ["qualityControlLevelCode"], default_value=9999), "processing_level_tree": pl_tree, "processing_level_id": None} for pl_tree in pl_trees] if pl_trees else [{"processing_level_code": 9999, "processing_level_tree": None, "processing_level_id": None}]
        for processing_level_data in processing_level_data_list:
            curs.execute("SELECT * FROM ProcessingLevels WHERE ProcessingLevelCode = ?", (processing_level_data["processing_level_code"],))
            row = curs.fetchone()
            if not row:
                processing_level = (
                    processing_level_data["processing_level_code"],
                    search_wml(processing_level_data["processing_level_tree"], ns, ["definition"], None) if processing_level_data["processing_level_code"] != 9999 else None,
                    search_wml(processing_level_data["processing_level_tree"], ns, ["explanation"], None) if processing_level_data["processing_level_code"] != 9999 else None,
                )
                curs.execute("""INSERT INTO ProcessingLevels (
                                ProcessingLevelID, 
                                ProcessingLevelCode,
                                Definition, 
                                Explanation
                            ) VALUES (NULL, ?, ?, ?)""", processing_level)
                processing_level_data["processing_level_id"] = curs.lastrowid
            else:
                processing_level_data["processing_level_id"] = row[0]

        # -------------------------------------------------------------------------- #
        #   Extracts Data for Methods, Actions, ActionBy, and FeatureActions Table   #
        # -------------------------------------------------------------------------- #

        md_trees = search_wml(wml_tree, ns, ["method"], get_tree=True, mult=True)
        method_data_list = [{"method_code": search_wml(md_tree, ns, ["methodCode", "MethodCode"], default_value=9999), "method_tree": md_tree, "method_id": None, "feature_action_id": None, "start_date": None, "start_date_offset": None, "value_count": None} for md_tree in md_trees] if md_trees else [{"method_code": 9999, "method_tree": None, "method_id": None}]
        for method_data in method_data_list:
            curs.execute("SELECT * FROM Methods WHERE MethodCode = ?", (method_data["method_code"],))
            row = curs.fetchone()
            if not row:
                method = (
                    "observation" if method_data["method_code"] != 9999 else "unknown",
                    method_data["method_code"],
                    method_data["method_code"] if method_data["method_code"] != 9999 else "unknown",
                    search_wml(method_data["method_tree"], ns, ["methodDescription", "MethodDescription"], None) if method_data["method_code"] != 9999 else None,
                    search_wml(method_data["method_tree"], ns, ["methodLink", "MethodLink"], None) if method_data["method_code"] != 9999 else None,
                )
                curs.execute("""INSERT INTO Methods (
                                MethodID, 
                                MethodTypeCV, 
                                MethodCode, 
                                MethodName,
                                MethodDescription, 
                                MethodLink
                            ) VALUES (NULL, ?, ?, ?, ?, ?)""", method)
                method_data["method_id"] = curs.lastrowid
            else:
                method_data["method_id"] = row[0]
            method_code_list = search_wml(wml_tree, ns, ["value"], attr="methodCode", mult=True)
            datetime_list = [i for j, i in enumerate(search_wml(wml_tree, ns, ["value"], attr="dateTime", mult=True)) if method_code_list[j] == method_data["method_code"] or not method_code_list[j]]
            time_offset_list = search_wml(wml_tree, ns, ["value"], attr="timeOffset", mult=True)
            start_date = datetime_list[0]
            value_count = len(datetime_list)
            end_date = datetime_list[-1]
            method_data["start_date"] = start_date[0]
            method_data["start_date_offset"] = time_offset_list[0] if time_offset_list[0] else "+00:00"
            method_data["value_count"] = value_count
            action = (
                "observation",
                method_data["method_id"],
                start_date,
                time_offset_list[0] if time_offset_list[0] else "+00:00",
                end_date,
                time_offset_list[-1] if time_offset_list[-1] else "+00:00",
                "An observation action that generated a time series result.",
            )
            curs.execute("""INSERT INTO Actions (
                            ActionID, 
                            ActionTypeCV, 
                            MethodID, 
                            BeginDateTime,
                            BeginDateTimeUTCOffset, 
                            EndDateTime, 
                            EndDateTimeUTCOffset, 
                            ActionDescription
                        ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)""", action)
            action_id = curs.lastrowid
            action_by = (
                action_id,
                affiliation_id,
                1,
            )
            curs.execute("""INSERT INTO ActionBy (
                            BridgeID, 
                            ActionID, 
                            AffiliationID, 
                            IsActionLead
                        ) VALUES (NULL, ?, ?, ?)""", action_by)
            action_by_id = curs.lastrowid
            feature_action = (
                sampling_feature_id,
                action_id,
            )
            curs.execute("""INSERT INTO FeatureActions (
                            FeatureActionID, 
                            SamplingFeatureID, 
                            ActionID
                        ) VALUES (NULL, ?, ?)""", feature_action)
            method_data["feature_action_id"] = curs.lastrowid

        # ----------------------------------------------------------------------------------------------------- #
        #    Extracts Data for Results, TimeSeriesResults, TimeSeriesResultValues, and DataSetResults Tables    #
        # ----------------------------------------------------------------------------------------------------- #

        result_data_list = list(itertools.product(method_data_list, processing_level_data_list))
        for result_data in result_data_list:
            result = (
                str(uuid.uuid4()),
                result_data[0]["feature_action_id"],
                "timeSeriesCoverage",
                variable_id,
                unit_id,
                result_data[1]["processing_level_id"],
                result_data[0]["start_date"],
                result_data[0]["start_date_offset"],
                None,
                search_wml(vr_tree, ns, ["sampleMedium"], default_value="unknown"),
                result_data[0]["value_count"],
            )
            curs.execute("""INSERT INTO Results (
                            ResultID, 
                            ResultUUID, 
                            FeatureActionID, 
                            ResultTypeCV,
                            VariableID, 
                            UnitsID, 
                            ProcessingLevelID, 
                            ResultDateTime, 
                            ResultDateTimeUTCOffset, 
                            StatusCV, 
                            SampledMediumCV, 
                            ValueCount
                        ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", result)
            result_id = curs.lastrowid
            timeseries_result = (
                result_id,
                "Unknown",
            )
            curs.execute("""INSERT INTO TimeSeriesResults (
                            ResultID, 
                            AggregationStatisticCV
                        ) VALUES (?, ?)""", timeseries_result)
            timeseries_result_values = tuple([(
                result_id,
                i[0],
                i[1],
                i[2] if i[2] else "+00:00",
                i[3] if i[3] else "nc",
                "unknown",
                "unknown",
                "unknown",
            ) for i in list(map(list, zip(*[
                search_wml(wml_tree, ns, ["value"], mult=True),
                search_wml(wml_tree, ns, ["value"], attr="dateTime", mult=True),
                search_wml(wml_tree, ns, ["value"], default_value="+00:00", attr="timeOffset", mult=True),
                search_wml(wml_tree, ns, ["value"], default_value="nc", attr="censorCode", mult=True)
            ])))])
            curs.execute("BEGIN TRANSACTION;")
            curs.executemany("""INSERT INTO TimeSeriesResultValues ( 
                                ValueID, 
                                ResultID, 
                                DataValue, 
                                ValueDateTime,
                                ValueDateTimeUTCOffset, 
                                CensorCodeCV, 
                                QualityCodeCV, 
                                TimeAggregationInterval,
                                TimeAggregationIntervalUnitsID
                            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)""", timeseries_result_values)
            dataset_result = (
                1,
                result_id,
            )
            curs.execute("""INSERT INTO DataSetsResults ( 
                            BridgeID, 
                            DataSetID, 
                            ResultID
                        ) Values (NULL, ?, ?)""", dataset_result)

        # -------------------- #
        #    Commits Changes   #
        # -------------------- #

        sql_connect.commit()

    return odm_filepath
