(function () {

    var myConnector = tableau.makeConnector();

    // --- Init recomendado ---
    myConnector.init = function (initCallback) {
        initCallback();
    };

    // 1. Definir esquema dinámico tomando el primer registro
    myConnector.getSchema = function (schemaCallback) {

        var connData = JSON.parse(tableau.connectionData);
        var apiKey = connData.apiKey;
        var baqUrl = connData.baqUrl;
        var user = connData.user;
        var password = connData.password;

        // Aseguramos que solo pidamos 1 registro para el esquema
        var schemaUrl = baqUrl;
        if (schemaUrl.indexOf("$top=") === -1) {
            schemaUrl += (schemaUrl.indexOf("?") === -1 ? "?" : "&") + "$top=1";
        }

        var headers = {
            "X-API-Key": apiKey,
            "Accept": "application/json"
        };

        // Agregamos Basic Auth si hay user/pass
        if (user && password) {
            var token = btoa(user + ":" + password);
            headers["Authorization"] = "Basic " + token;
        }

        fetch(schemaUrl, { headers: headers })
            .then(function (r) { return r.json(); })
            .then(function (data) {

                if (!data.value || data.value.length === 0) {
                    tableau.abortWithError("El BAQ no regresó registros para definir el esquema.");
                    return;
                }

                var sample = data.value[0];
                var cols = [];

                for (var key in sample) {
                    if (sample.hasOwnProperty(key)) {
                        cols.push({
                            id: key,
                            alias: key,
                            dataType: tableau.dataTypeEnum.string // todo como texto por ahora
                        });
                    }
                }

                var tableSchema = {
                    id: "EpicorBAQ",
                    alias: "Epicor BAQ Data",
                    columns: cols
                };

                schemaCallback([tableSchema]);
            })
            .catch(function (err) {
                tableau.abortWithError("Error al obtener esquema: " + err);
            });
    };

    //// 2. Descarga de datos con soporte para Extract Incremental

    // myConnector.getData = function(table, doneCallback) {

    //     let baqUrl = tableau.connectionData;

    //     fetch(baqUrl, {
    //         headers: {
    //             "X-API-Key": document.getElementById("apiKey").value,
    //             "Accept": "application/json"
    //         }
    //     })
    //     .then(r => r.json())
    //     .then(data => {
    //         table.appendRows(data.value);
    //         doneCallback();
    //     });
    // };

    // 2. Descarga de datos con soporte para Extract Incremental
    myConnector.getData = function (table, doneCallback) {

        var connData = JSON.parse(tableau.connectionData);
        var apiKey = connData.apiKey;
        var baseUrl = connData.baqUrl; // sin filtros incrementales fijos
        var user = connData.user;
        var password = connData.password;

        // campo incremental en Tableau
        var incrFieldName = "InvcHead_CreatedOn"; //debe existir en el BAQ

        // valor incremental que Tableau quiere usar
        var lastIncrValue = table.incrementValue;  // puede ser null en carga inicial

        // Construimos la URL final para este fetch
        var url = baseUrl;

        // Si viene valor incremental, agregamos $filter
        if (lastIncrValue) {
            // OData: LastChanged gt {valor}
            // OJO con el formato: si LastChanged es datetime completo, Tableau te lo pasa tipo '2025-01-15T00:00:00'
            // Dependiendo del formato, quizá requiera comillas. Si LastChanged es datetime,
            // normalmente Tableau te manda algo tipo 2025-01-15T00:00:00.
            var filter = incrFieldName + " gt datetime '" + lastIncrValue + "'";

            // si ya trae ?, usamos &; si no, usamos ?
            url += (url.indexOf("?") === -1 ? "?" : "&") + "$filter=" + filter;
        }

        var headers = {
            "X-API-Key": apiKey,
            "Accept": "application/json"
        };

        if (user && password) {
            var token = btoa(user + ":" + password);
            headers["Authorization"] = "Basic " + token;
        }

        fetch(url, { headers: headers })
            .then(function (r) { return r.json(); })
            .then(function (data) {

                if (!data.value) {
                    tableau.abortWithError("Respuesta sin 'value' desde Epicor.");
                    return;
                }

                table.appendRows(data.value);
                doneCallback();
            })
            .catch(function (err) {
                tableau.abortWithError("Error al obtener datos: " + err);
            });
    };


    tableau.registerConnector(myConnector);

})();

function submitWDC() {
    var user = document.getElementById("user").value.trim();
    var password = document.getElementById("password").value.trim();
    var apiKey = document.getElementById("apiKey").value.trim();
    var baqUrl = document.getElementById("baqUrl").value.trim()

    if (!apiKey || !baqUrl) {
        alert("Debes ingresar API Key y URL.");
        return;
    }

    var connData = {
        user: user,
        password: password,
        apiKey: apiKey,
        baqUrl: baqUrl
    };

    
    tableau.connectionName = "Epicor BAQ Connector";
    tableau.connectionData = JSON.stringify(connData);
    tableau.submit();
}

