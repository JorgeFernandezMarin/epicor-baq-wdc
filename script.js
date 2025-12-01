(function () {
    var myConnector = tableau.makeConnector();

    // -------- INIT ----------
    myConnector.init = function (initCallback) {
        // Si ya hay datos guardados, repoblamos inputs para que sea más cómodo
        if (tableau.connectionData) {
            try {
                var saved = JSON.parse(tableau.connectionData);
                if (saved.funcUrl) {
                    document.getElementById("funcUrl").value = saved.funcUrl;
                }
                if (saved.baqName) {
                    document.getElementById("baqName").value = saved.baqName;
                }
                if (saved.incrField) {
                    document.getElementById("incrField").value = saved.incrField;
                }
            } catch (e) {
                console.warn("No se pudo parsear connectionData:", e);
            }
        }

        initCallback();

        // Si estamos en fase de obtención de datos, Tableau llama directamente sin UI
        if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            tableau.submit();
        }
    };

    // -------- SCHEMA ----------
    myConnector.getSchema = function (schemaCallback) {
        var connData = JSON.parse(tableau.connectionData);
        var funcUrl = connData.funcUrl;
        var baqName = connData.baqName;
        var incrField = connData.incrField;

        // Pedimos solo 1 registro para deducir tipos
        var url = funcUrl + buildQueryString({
            baq: baqName,
            "$top": 1
        });

        fetch(url)
            .then(function (r) { return r.json(); })
            .then(function (data) {
                if (!data.value || data.value.length === 0) {
                    tableau.abortWithError("La función no regresó registros para definir el esquema.");
                    return;
                }

                var sample = data.value[0];
                var cols = [];

                Object.keys(sample).forEach(function (key) {
                    var value = sample[key];
                    var type = tableau.dataTypeEnum.string;

                    // Heurística de tipos
                    if (key === incrField) {
                        type = tableau.dataTypeEnum.datetime;
                    } else if (value instanceof Date) {
                        type = tableau.dataTypeEnum.datetime;
                    } else if (typeof value === "number") {
                        type = Number.isInteger(value) ? tableau.dataTypeEnum.int : tableau.dataTypeEnum.float;
                    } else if (typeof value === "boolean") {
                        type = tableau.dataTypeEnum.bool;
                    } else if (looksLikeDateTime(value)) {
                        type = tableau.dataTypeEnum.datetime;
                    }

                    cols.push({
                        id: key,
                        alias: key,
                        dataType: type
                    });
                });

                var tableSchema = {
                    id: "EpicorBAQ",
                    alias: "Epicor BAQ via Azure Function",
                    columns: cols
                };

                schemaCallback([tableSchema]);
            })
            .catch(function (err) {
                tableau.abortWithError("Error al obtener esquema desde Azure Function: " + err);
            });
    };

    // -------- DATA ----------
    myConnector.getData = function (table, doneCallback) {
        var connData = JSON.parse(tableau.connectionData);
        var funcUrl = connData.funcUrl;
        var baqName = connData.baqName;
        var incrField = connData.incrField;

        var lastValue = table.incrementValue;  // valor incremental que Tableau nos pasa
        var params = { baq: baqName };

        // Si viene valor incremental, agregamos filtro OData
        if (lastValue) {
            // OData datetime literal
            var filter = incrField + " gt datetime '" + lastValue + "'";
            params["$filter"] = filter;
        }

        var url = funcUrl + buildQueryString(params);

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
                tableau.abortWithError("Error al obtener datos desde Azure Function: " + err);
            });
    };

    tableau.registerConnector(myConnector);

    // -------- Helpers --------
    function buildQueryString(obj) {
        var parts = [];
        for (var k in obj) {
            if (!obj.hasOwnProperty(k)) continue;
            if (obj[k] === undefined || obj[k] === null || obj[k] === "") continue;
            parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(obj[k]));
        }
        return parts.length ? "?" + parts.join("&") : "";
    }

    function looksLikeDateTime(v) {
        if (typeof v !== "string") return false;
        // Ejemplos: "2025-11-30T00:00:00-06:00", "2025-11-30T00:00:00"
        return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(v);
    }
})();

// -------- Submit desde la UI --------
function submitWDC() {
    var funcUrl = document.getElementById("funcUrl").value.trim();
    var baqName = document.getElementById("baqName").value.trim();
    var incrField = document.getElementById("incrField").value.trim() || "InvcHead_CreatedOn";

    if (!funcUrl || !baqName) {
        alert("Debes ingresar la URL de la Azure Function y el nombre del BAQ.");
        return;
    }

    var connData = {
        funcUrl: funcUrl,
        baqName: baqName,
        incrField: incrField
    };

    tableau.connectionName = "Epicor BAQ via Azure Function";
    tableau.connectionData = JSON.stringify(connData);

    // Indicamos a Tableau qué campo se usará para incremental extract
    tableau.incrementalExtractColumn = incrField;

    tableau.submit();
}
