(function packageHydroShareTimeSeriesManager() {

    'use strict';

    /*****************************************************************************************
     *********************************** GLOBAL VARIABLES ************************************
     *****************************************************************************************/

    var dataTable;
    var dtState;
    var recordsSelected;

    /*****************************************************************************************
     ************************************** FUNCTIONS ****************************************
     *****************************************************************************************/

    /* Initializes HydroShare Time Series Creator App */
    function initApp() {
        //history.pushState(null, "", location.href.split("?")[0]);
        dtState = {
            'top': 0,
            'left': 0
        };
        dataTable = $('#data-table').DataTable({
            'dom': '<"toolbar"><frt><"footer"lip>',
            'ordering': false,
            'serverSide': true,
            'ajax': {
                'url': '/apps/hydroshare-timeseries-manager/ajax/update-table/',
                'type': 'POST',
                'headers': {
                    'X-CSRFToken': getCookie('csrftoken')
                },
                'data': {
                    'session-id': $('#session-id').text()
                },
                'dataSrc': function(json) {
                    recordsSelected = json.recordsSelected
                    for (var i = 0; i < json.data.length; i++) {
                        switch (json.data[i][0]) {
                            case 'Waiting':
                                json.data[i][0] = `<img style="float:left;"class="status-icon" src="/static/hydroshare_timeseries_manager/images/spinner.gif">`
                                break;
                            case 'Downloading':
                                json.data[i][0] = `<img style="float:left;"class="status-icon" src="/static/hydroshare_timeseries_manager/images/spinner.gif">`
                                break;
                            case 'Validating':
                                json.data[i][0] = `<img style="float:left;"class="status-icon" src="/static/hydroshare_timeseries_manager/images/spinner.gif">`
                                break;
                            case 'Ready':
                                json.data[i][0] = `<img style="float:left;"class="status-icon" src="/static/hydroshare_timeseries_manager/images/green_light.svg">`
                                break;
                            case 'Failed':
                                json.data[i][0] = `<img style="float:left;"class="status-icon" src="/static/hydroshare_timeseries_manager/images/red_light.svg">`
                                break;
                        };
                    };
                    return json.data
                }
            },
            'drawCallback': function() {
                $(dataTable.settings()[0].nScrollBody).scrollTop(dtState.top);
                $(dataTable.settings()[0].nScrollBody).scrollLeft(dtState.left);
                dataTable.rows().eq(0).each(function(index){
                    var row = dataTable.row(index);
                    var data = row.data();
                    if (data[20] === true) {
                        row.select();
                    };
                });
                if (recordsSelected > 0) {
                    $('#data-table_info').append(` (${recordsSelected} selected)`);
                };
            },
            'scrollX': true,
            'paging': true,
            'columnDefs': [
                {'defaultContent': '', 'targets': '_all'},
                {'width': '5px', 'targets': 0},
                {'width': '80px', 'targets': 1},
                {'width': '80px', 'targets': 2},
                {'width': '80px', 'targets': 3},
                {'width': '40px', 'targets': 4},
                {'width': '40px', 'targets': 5},
                {'width': '80px', 'targets': 6},
                {'width': '80px', 'targets': 7},
                {'width': '80px', 'targets': 8},
                {'width': '80px', 'targets': 9},
                {'width': '80px', 'targets': 10},
                {'width': '40px', 'targets': 11},
                {'width': '80px', 'targets': 12},
                {'width': '80px', 'targets': 13},
                {'width': '80px', 'targets': 14},
                {'width': '80px', 'targets': 15},
                {'width': '40px', 'targets': 16},
                {'width': '40px', 'targets': 17},
                {'width': '80px', 'targets': 18},
                {'visible': false, 'targets': 19},
                {'visible': false, 'targets': 20}
            ]
        });
        $("div.toolbar").html(`
            <div class="tools btn-group">
                <button id="btn-select-all" type="button" class="btn btn-primary">
                    <span class="glyphicon tool-glyph glyphicon-check"></span>
                </button>
                <button id="btn-deselect-all" type="button" class="btn btn-primary">
                    <span class="glyphicon tool-glyph glyphicon-unchecked"></span>
                </button>
                <button id="btn-import-data" type="button" class="btn btn-primary" data-toggle="modal" data-target="#modal-import-data-dialog">
                    <span class="glyphicon tool-glyph glyphicon-plus"></span>
                </button>
                <button id="btn-remove-selected" type="button" class="btn btn-primary">
                    <span class="glyphicon tool-glyph glyphicon-trash"></span>
                </button>
            </div>
            <button id="btn-create-resource" class="btn btn-success" type="button">Create Resource</button>
            <div class="loading-bar"></div>
        `);
        window.onbeforeunload = function() {
            removeRows(false);
        };
        window.setInterval(function() {
            updateTable();
        }, 3000);
        var sessionId = $('#session-id').text();
        var postRefts = $('#refts').text();
        var resourceId = $('#resource-id').text();
        var aggregationId = $('#aggregation-id').text();
        addDataToSession(resourceId, aggregationId, postRefts);
        //loginTest();
    };

    function updateTable() {
        dtState = {
            'top': $(dataTable.settings()[0].nScrollBody).scrollTop(),
            'left': $(dataTable.settings()[0].nScrollBody).scrollLeft()
        };
        dataTable.ajax.reload(null, false);
    };

    function selectAll() {
        var timeseriesId = null;
        var selected = true;
        updateSelectedRows(timeseriesId, selected);
    };

    function deselectAll() {
        var timeseriesId = null;
        var selected = false;
        updateSelectedRows(timeseriesId, selected);
    };

    function toggleSelect() {
        var timeseriesId = dataTable.row($(this)).data()[19];
        var selected = !$(this).hasClass('selected');
        updateSelectedRows(timeseriesId, selected);
    };

    function updateSelectedRows(timeseriesId, selected) {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text(),
                'timeseriesId': timeseriesId,
                'searchValue': $('.dataTables_filter input').val(),
                'selected': selected
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/update-selections/',
            success: function(response) {
                updateTable();
            },
            error: function(response) {

            }
        });
    };

    function removeSelectedRows() {
        removeRows(true);
    };

    function removeRows(selected) {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text(),
                'selected': selected
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/remove-timeseries/',
            success: function(response) {
                updateTable();
            },
            error: function(response) {

            }
        });
    };

    function addDataToSession(resourceId, aggregationId, reftsJson) {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text(),
                'resourceId': resourceId,
                'aggregationId': aggregationId,
                'reftsJson': reftsJson
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/add-session-data/',
            success: function(response) {
                updateTable();
                if (response['success'] === true && response['refts_id'] !== null) {
                    prepareSessionData(response['refts_id']);
                };
            },
            error: function(response) {

            }
        });
    };

    function prepareSessionData(reftsId) {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text(),
                'reftsId': reftsId
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/prepare-session-data/',
            success: function(response) {

            },
            error: function(response) {

            }
        });
    };

    function updateResourceMetadata() {
        $('#create-res-message').val('');
        $('#res-title').val('');
        $('#res-abstract').val('');
        $('#res-keywords').val('');
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text()
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/update-resource-metadata/',
            success: function(response) {
                if (response['success'] === true) {
                    $('#res-title').val(response['res_title']);
                    $('#res-abstract').val(response['res_abstract']);
                    $('#res-keywords').val(response['res_keywords']);
                    $('#res-filename').val(response['res_filename']);
                    $('#modal-create-resource-dialog').modal('show');
                } else {
                    alert(response['message']);
                };
            },
            error: function(response) {

            }
        });
    };

    function getCookie(name) {
        var cookieValue = null;
        if (document.cookie && document.cookie != '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = jQuery.trim(cookies[i]);
                if (cookie.substring(0, name.length + 1) == (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                };
            };
        };
        return cookieValue;
    };

    function createHydroShareResource() {
        //loginTest();
        var resTitle = $('#res-title').val();
        var resAbstract = $('#res-abstract').val();
        var resKeywords = $('#res-keywords').val();
        var resFilename = $('#res-filename').val();
        var createTsdb = $('#chk-ts').is(":checked");
        var createRefts = $('#chk-refts').is(":checked");
        var createPublic = $('#chk-public').is(":checked");
        if (resTitle === '') {
            alert('Please include a resource title.');
            return;
        };
        if (resAbstract === '') {
            alert('Please include a resource abstract.');
            return;
        };
        if (resKeywords === '') {
            alert('Please include at least one resource keyword.');
            return;
        };
        if (resFilename === '') {
            alert('Please include at a resource file name.');
            return;
        };
        if (!createTsdb && !createRefts) {
            alert('Please select either "Create Time Series Database" or "Create Reference Time Series".');
            return;
        };
        var data = {
            'sessionId': $('#session-id').text(),
            'resTitle': resTitle,
            'resAbstract': resAbstract,
            'resKeywords': resKeywords,
            'resFilename': resFilename,
            'createTs': createTsdb,
            'createRefts': createRefts,
            'createPublic': createPublic
        };
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: data,
            url: '/apps/hydroshare-timeseries-manager/ajax/create-resource/',
            success: function(response) {
                if (response['success'] == true) {
                    alert("success");
                } else {
                    console.log('Create Resource Failed');
                };
            },
            error: function(response) {
                console.log('Create Resource Failed');
            }
        });
    }




































    /*
    function loginTest() {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            url: '/apps/hydroshare-timeseries-manager/ajax/login-test/',
            success: function(response) {
                if (response['success'] !== true) {
                    $('#modal-login-dialog').modal({backdrop: 'static', keyboard: false});
                };
            },
            error: function(response) {
                console.log('Login Test Failed')
            }
        });
    };

    function hydroShareLogin() {
        window.onbeforeunload = null;
        var hydroshareUrl = $('#hydroshare-url').text();
        var sessionId = $('#session-id').text();
        var resourceId = $('#resource-id').text();
        var baseUrl = '/apps/hydroshare-timeseries-manager/';
        var query = (sessionId || resourceId) ? '?' : '';
        var separator = (sessionId && resourceId) ? '&' : '';
        var resourceQuery = (resourceId) ? `resource_id=${resourceId}` : '';
        var sessionQuery = (sessionId) ? `session_id=${sessionId}`: '';
        var beta = (hydroshareUrl.includes('beta')) ? '_beta' : '';
        var callbackUrl = `${baseUrl}${query}${resourceQuery}${separator}${sessionQuery}`;
        var oauthUrl = `/oauth2/login/hydroshare${beta}/?next=${callbackUrl}`;
        var loginTab = window.open(oauthUrl, "_self");
        if (!loginTab) {
            alert('To use this application, please allow popups for this website.');
        };
    };

    function loadSessionData() {
        var sessionId = $('#session-id').text();
        var timeSeriesIds = dataTable.column(18).data().toArray();
        var data = {
            'sessionId': sessionId,
            'timeSeriesIds': timeSeriesIds
        };
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: data,
            url: '/apps/hydroshare-timeseries-manager/ajax/load-session-data/',
            success: function(response) {
                if (response['success'] == true) {
                    addRowsToTable(response['results']);
                    prepareTimeSeriesData(response['results']);
                    $('#processing').text('TRUE');
                    checkTimeSeriesStatus($('#session-id').text());
                } else {
                    console.log("LOAD SESSION DATA FAILED");
                };
            },
            error: function(response) {
                console.log('Load Session Failed')
            }
        });
    };


    function addRowsToTable(rows) {
        for (var i = 0; i < rows.length; i++) { 
            var row = [
                `<div id="${rows[i]['timeseries_id']}" class="row-status">
                    <img class="status-icon blink" src="/static/hydroshare_timeseries_manager/images/yellow_light.svg">
                    <div class="status-message">Loading</div>
                </div>`,
                rows[i]['site_name'],
                rows[i]['site_code'],
                rows[i]['latitude'],
                rows[i]['longitude'],
                rows[i]['variable_name'],
                rows[i]['variable_code'],
                rows[i]['sample_medium'],
                rows[i]['begin_date'],
                rows[i]['end_date'],
                rows[i]['value_count'],
                rows[i]['method_link'],
                rows[i]['method_description'],
                rows[i]['network_name'],
                rows[i]['url'],
                rows[i]['service_type'],
                rows[i]['ref_type'],
                rows[i]['return_type'],
                rows[i]['timeseries_id']
            ];
            dataTable.row.add(row).draw( false )
        };
    };

    function prepareTimeSeriesData(data) {
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: {
                'sessionId': $('#session-id').text(),
                'rows': JSON.stringify(data)
            },
            url: '/apps/hydroshare-timeseries-manager/ajax/prepare-timeseries-data/',
            success: function(response) {
                if (response['success'] == true) {
                    console.log("DONE")
                } else {
                    console.log("LOAD SESSION DATA FAILED");
                };
            },
            error: function(response) {
                console.log('Prepare TimeSeries Failed')
            }
        });
    };

	function sleep(ms) {
	    return new Promise(resolve => setTimeout(resolve, ms));
	};

    async function checkTimeSeriesStatus(sessionId) {
        while ($('#processing').text() === 'TRUE') {
            $.ajax({
                headers: {
                    'X-CSRFToken': getCookie('csrftoken')
                },
                type: 'POST',
                data: {
                    'sessionId': sessionId,
                    'requestType': 'timeSeriesCheck'
                },
                url: '/apps/hydroshare-timeseries-manager/ajax/ajax-check-timeseries-status/',
                success: function(response) {
                    if (response['success'] == true) {
                        for (var i = 0; i < response['results'].length; i++) { 
                            if (response['results'][i][1] === 'SUCCESS') {
                                $(`#${response['results'][i][0]}`).children('img').attr('src', '/static/hydroshare_timeseries_manager/images/green_light.svg');
                                $(`#${response['results'][i][0]}`).children('img').removeClass('blink');
                                $(`#${response['results'][i][0]}`).children('div').text('Ready');
                            };
                            if (response['results'][i][1] === 'FAILURE') {
                                $(`#${response['results'][i][0]}`).children('img').attr('src', '/static/hydroshare_timeseries_manager/images/red_light.svg');
                                $(`#${response['results'][i][0]}`).children('img').removeClass('blink');
                                $(`#${response['results'][i][0]}`).children('div').text('Failed')
                            };
                            var results = response['results'].map(result => result[1]);
                            if (!results.includes('PROCESSING') && !results.includes('WAITING')) {
                            	console.log('FINISHED')
                            	$('#processing').text('FALSE');
                            };
                        };
                        if (results === undefined || results.length == 0) {
                            console.log('FINISHED')
                            $('#processing').text('FALSE');
                        };
                        console.log(response['results'])
                    } else {
                        console.log("CHECK TIMESERIES STATUS FAILED");
                    };
                },
                error: function(response) {
                    console.log('Time Series Check Failed')
                }
            });
            await sleep(3000);
        };
    };

    function createHydroShareResource() {
        loginTest();
        var resTitle = $('#res-title').val();
        var resAbstract = $('#res-abstract').val();
        var resKeywords = $('#res-keywords').val();
        var createTsdb = $('#chk-ts').is(":checked");
        var createRefts = $('#chk-refts').is(":checked");
        var createPublic = $('#chk-public').is(":checked");
        var selectedRows = dataTable.rows({selected: true}).data().toArray().map(x => x[18]);
        var timeSeriesStatusList = selectedRows.map(x => $(`#${x}`).children('div').text());
        if (timeSeriesStatusList.length == 0) {
            alert('Please select at least one time series.');
            return;
        };
        if (!timeSeriesStatusList.every(x => x === 'Ready')) {
            alert('Some of the time series datasets you selected failed or are not valid.');
            return;
        };
        if (resTitle === '') {
            alert('Please include a resource title.');
            return;
        };
        if (resAbstract === '') {
            alert('Please include a resource abstract.');
            return;
        };
        if (resKeywords === '') {
            alert('Please include at least one resource keyword.');
            return;
        };
        if (!createTsdb && !createRefts) {
            alert('Please select either "Create Time Series Database" or "Create Reference Time Series".');
            return;
        };
        var data = {
            'sessionId': $('#session-id').text(),
            'timeSeriesIds': JSON.stringify(selectedRows),
            'resTitle': resTitle,
            'resAbstract': resAbstract,
            'resKeywords': resKeywords,
            'createTs': createTsdb,
            'createRefts': createRefts,
            'createPublic': createPublic
        };
        $.ajax({
            headers: {
                'X-CSRFToken': getCookie('csrftoken')
            },
            type: 'POST',
            data: data,
            url: '/apps/hydroshare-timeseries-manager/ajax/ajax-create-hydroshare-resource/',
            success: function(response) {
                if (response['success'] == true) {
                    alert("success");
                } else {
                    console.log('Create Resource Failed');
                };
            },
            error: function(response) {
                console.log('Create Resource Failed');
            }
        });
    }

    function tableDeselectAll() {
        dataTable.rows({
            search: 'applied'
        }).deselect();
    };

    function tableRemoveData() {
        dataTable.rows('.selected').remove().draw();
    };

    function updateDefaultResourceMetadata() {
        var timeSeriesData = dataTable.rows({selected: true}).data().toArray().map(x => [x[1],x[5],x[7],x[8],x[9]]);
        var generationDateShort = moment().format('MMMM Do, YYYY');
        var generationDateLong = moment().format();
        var siteList = timeSeriesData.map(x => x[0].replace(/,/g, '')).filter(function(item, i, l){return l.indexOf(item) === i;});
        if (siteList.length === 1) {
            var siteListFormatted = siteList[0];
        } else if (siteList.length === 2) {
            var siteListFormatted = siteList.join(' and ');
        } else if (siteList.length <= 5) {
            var siteListFormatted = `${siteList.slice(0, -1).join(', ')}, and ${siteList.slice(-1)[0]}`;
        } else {
            var siteListFormatted = `${siteList.slice(0, 6).join(', ')}, and ${(siteList.length - 5).toString()} more site${(siteList.length >= 7) ? 's' : ''}`
        };
        var varList = timeSeriesData.map(x => x[1].replace(/,/g, '')).filter(function(item, i, l){return l.indexOf(item) === i;});
        var varListFormatted = [varList[0]].concat(varList.slice(1).map(x => (x.charAt(1) == x.charAt(1).toUpperCase()) ? x : x.charAt(0).toLowerCase() + x.slice(1)));
        if (varListFormatted.length === 1) {
            varListFormatted = varListFormatted[0];
        } else if (varListFormatted.length === 2) {
            varListFormatted = varListFormatted.join(' and ');
        } else {
            varListFormatted = varListFormatted.slice(0, -1).join(', ') + ', and ' + varListFormatted.slice(-1)[0];
        };
        var beginDateList = timeSeriesData.map(x => moment(x[8]));
        var endDateList = timeSeriesData.map(x => moment(x[8]));
        var beginDate = moment.min(beginDateList).format();
        var endDate = moment.max(endDateList).format();
        var keywordList = timeSeriesData.map(x => [x[1].replace(/,/g, ''), x[2].replace(/,/g, '')]).flat().filter(function(item, i, l){return l.indexOf(item) === i;});
        var resTitle = `Time series dataset created on ${generationDateShort} by the HydroShare Time Series Manager`;
        var resAbstract = `${varListFormatted} ` +
                          `data collected from ` +
                          `${beginDate} to ${endDate} ` +
                          `from the following site${(siteList.length > 1) ? 's' : ''}: ` +
                          `${siteListFormatted}. ` +
                          `Data compiled by the HydroShare Time Series Manager on ${generationDateLong}.`;
        var resKeywords = keywordList.join(', ');
        if (timeSeriesData.length > 0) {
            $('#res-title').val(resTitle);
            $('#res-abstract').val(resAbstract);
            $('#res-keywords').val(resKeywords);
        } else {
            $('#res-title').val('');
            $('#res-abstract').val('');
            $('#res-keywords').val('');
        };
    };

    function changeDataImportOption() {
        $('#import-resource').hide();
        $('#import-refts-file').hide();
        $('#' + $("#data-import-method").val()).show();
    };

    function getCookie(name) {
        var cookieValue = null;
        if (document.cookie && document.cookie != '') {
            var cookies = document.cookie.split(';');
            for (var i = 0; i < cookies.length; i++) {
                var cookie = jQuery.trim(cookies[i]);
                if (cookie.substring(0, name.length + 1) == (name + '=')) {
                    cookieValue = decodeURIComponent(cookie.substring(name.length + 1));
                    break;
                };
            };
        };
        return cookieValue;
    };*/


    /*****************************************************************************************
     *********************************** EVENT LISTENERS *************************************
     *****************************************************************************************/

    /* Listener for initializing data and checking login status */
    $(document).ready(initApp);

    /* Listener for logging in to HydroShare */
    //$(document).on('click', '#login-link', hydroShareLogin);

    /* Listener for toggling a single row */
    $(document).on('click', 'tr', toggleSelect);

    /* Listener for selecting all filtered data in table */
    $(document).on('click', '#btn-select-all', selectAll);

    /* Listener for deselecting all filtered data in table */
    $(document).on('click', '#btn-deselect-all', deselectAll);

    /* Listener for removing all selected data in table */
    $(document).on('click', '#btn-remove-selected', removeSelectedRows);

    /* Listener for updating default resource metadata */
    $(document).on('click', '#btn-create-resource', updateResourceMetadata);

    /* Listener for creating a HydroShare resource */
    $(document).on('click', '#btn-create-timeseries-resource', createHydroShareResource);

    /* Listener for updating default resource metadata */
    //$(document).on('click', '#create-resource-btn', updateDefaultResourceMetadata);

    /* Listener for changing data import option */
    //$(document).on('change', '#data-import-method', changeDataImportOption);

    /* Listener for creating a HydroShare resource */
    //$(document).on('click', '#btn-create-timeseries-resource', createHydroShareResource);

    /* Listener for updating default resource metadata */
    //$(document).on('click', '#create-resource-btn', updateDefaultResourceMetadata);

}());
