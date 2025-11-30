(function () {

    var myConnector = tableau.makeConnector();

    // Init básico
    myConnector.init = function (initCallback) {
        initCallback();

        // Para Extract Refresh en Server/Cloud
        if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            tableau.submit();
        }
    };

    // --- 1) Definir esquema leyendo un sample de la función ---
    myConnector.getSchema = function (schemaCallback) {

        var connData = JSON.parse(tableau.connectionData);
        var url = connData.functionUrl;   // URL completa de la Azure Function

        fetch(url)
            .then(function (r) { return r.json(); })
            .then(function (data) {

                if (!data.value || data.value.length === 0) {
                    tableau.abortWithError(
                        "La Azure Function no devolvió registros para definir el esquema."
                    );
                    return;
                }

                var sample = data.value[0];
                var cols = [];

                for (var key in sample) {
                    if (!sample.hasOwnProperty(key)) continue;

                    var value = sample[key];
                    var type = tableau.dataTypeEnum.string;

                    // Heurística sencilla para tipos
                    if (key === "InvcHead_CreatedOn") {
                        type = tableau.dataTypeEnum.datetime;
                    } else if (typeof value === "number") {
                        type = tableau.dataTypeEnum.float;
                    }

                    cols.push({
                        id: key,
                        alias: key,
                        dataType: type
                    });
                }

                var tableSchema = {
                    id: "EpicorBAQ",
                    alias: connData.baqFriendlyName || "Epicor BAQ Data (Azure Function)",
                    columns: cols
                };

                schemaCallback([tableSchema]);
            })
            .catch(function (err) {
                tableau.abortWithError(
                    "Error al obtener esquema desde Azure Function: " + err
                );
            });
    };

    // --- 2) Descarga de datos (full extract por ahora) ---
    myConnector.getData = function (table, doneCallback) {

        var connData = JSON.parse(tableau.connectionData);
        var url = connData.functionUrl;

        fetch(url)
            .then(function (r) { return r.json(); })
            .then(function (data) {

                if (!data.value) {
                    tableau.abortWithError("Respuesta sin 'value' desde Azure Function.");
                    return;
                }

                table.appendRows(data.value);
                doneCallback();
            })
            .catch(function (err) {
                tableau.abortWithError(
                    "Error al obtener datos desde Azure Function: " + err
                );
            });
    };

    tableau.registerConnector(myConnector);

})();

// --- UI: leer datos del formulario y crear la conexión ---
function submitWDC() {
    var fnUrl = document.getElementById("functionUrl").value.trim();
    var baqName = document.getElementById("baqName").value.trim();

    if (!fnUrl) {
        alert("Debes ingresar la URL de la Azure Function.");
        return;
    }

    var connData = {
        functionUrl: fnUrl,
        baqFriendlyName: baqName || "Epicor BAQ"
    };

    tableau.connectionName =
        "Epicor BAQ via Azure Function – " + (baqName || "Sin nombre");
    tableau.connectionData = JSON.stringify(connData);

    tableau.submit();
}
