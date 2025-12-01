(function () {
    var myConnector = tableau.makeConnector();

    // -------- INIT ----------
    myConnector.init = function (initCallback) {
        // Rellenar campos si ya había conexión guardada
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

        // Cuando Tableau está en gatherDataPhase y ya hay conexión, solo enviamos
        if (tableau.phase === tableau.phaseEnum.gatherDataPhase) {
            tableau.submit();
        }
    };

    // -------- SCHEMA ----------
    myConnector.getSchema = function (schemaCallback) {
        var connData = JSON.parse(tableau.connectionData);
        var funcUrl   = connData.funcUrl;
        var baqName   = connData.baqName;
        var incrField = connData.incrField;   // por ej. "InvcHead_CreatedOn"

        // Pedimos solo 1 registro para deducir tipos
        var url = buildUrl(funcUrl, {
            baq: baqName,
            "$top": 1
        });

        console.log("getSchema URL:", url);

        fetch(url)
            .then(function (r) {
                return r.text();   // primero texto, por si hay que debuggear
            })
            .then(function (txt) {
                if (!txt) {
                    throw new Error("Respuesta vacía desde Azure Function (URL: " + url + ")");
                }

                var data;
                try {
                    data = JSON.parse(txt);
                } catch (e) {
                    console.error("Respuesta no-JSON desde Azure Function:", txt);
                    throw new Error("Respuesta no-JSON desde Azure Function. Detalle: " + e.message);
                }

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
                        // nuestra columna incremental (datetime)
                        type = tableau.dataTypeEnum.datetime;
                    } else if (value instanceof Date) {
                        type = tableau.dataTypeEnum.datetime;
                    } else if (typeof value === "number") {
                        type = Number.isInteger(value)
                            ? tableau.dataTypeEnum.int
                            : tableau.dataTypeEnum.float;
                    } else if (typeof value === "boolean") {
                        type = tableau.dataTypeEnum.bool;
                    } else if (looksLikeDateTime(value)) {
                        type = tableau.dataTypeEnum.datetime;
                    }

                    cols.push({
                        id: key,        // ojo: aquí el id REAL del campo (p.ej. "InvcHead_CreatedOn")
                        alias: key,
                        dataType: type
                    });
                });

                // 👇 CLAVE para incremental: incrementColumnId
                var tableSchema = {
                    id: "EpicorBAQ",
                    alias: "Epicor BAQ via Azure Function",
                    columns: cols,
                    incrementColumnId: incrField   // <-- habilita incremental en Tableau【tableInfo.incrementColumnId】
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
        var funcUrl   = connData.funcUrl;
        var baqName   = connData.baqName;
        var incrField = connData.incrField;

        // valor incremental que Tableau nos pasa (último valor de incrementColumnId)
        var lastValue = table.incrementValue;
        var params = { baq: baqName };

        if (lastValue) {
            // OData filter: InvcHead_CreatedOn gt datetime 'YYYY-MM-DDTHH:MM:SS'
            var filter = incrField + " gt datetime '" + lastValue + "'";
            params["$filter"] = filter;
        }

        var url = buildUrl(funcUrl, params);
        console.log("getData URL:", url, "lastValue:", lastValue);

        fetch(url)
            .then(function (r) {
                return r.text();
            })
            .then(function (txt) {
                if (!txt) {
                    throw new Error("Respuesta vacía desde Azure Function (URL: " + url + ")");
                }

                var data;
                try {
                    data = JSON.parse(txt);
                } catch (e) {
                    console.error("Respuesta no-JSON desde Azure Function:", txt);
                    throw new Error("Respuesta no-JSON desde Azure Function. Detalle: " + e.message);
                }

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

    function buildUrl(base, obj) {
        var parts = [];
        for (var k in obj) {
            if (!obj.hasOwnProperty(k)) continue;
            if (obj[k] === undefined || obj[k] === null || obj[k] === "") continue;
            parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(obj[k]));
        }
        if (!parts.length) return base;

        var sep = base.indexOf("?") === -1 ? "?" : "&";
        return base + sep + parts.join("&");
    }

    function looksLikeDateTime(v) {
        if (typeof v !== "string") return false;
        // 2025-11-30T19:17:29
        return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/.test(v);
    }
})();

// -------- Submit desde la UI --------
function submitWDC() {
    var funcUrl = document.getElementById("funcUrl").value.trim();
    var baqName = document.getElementById("baqName").value.trim();
    var incrField = (document.getElementById("incrField").value.trim() || "InvcHead_CreatedOn");

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

    // Ya NO usamos tableau.incrementalExtractColumn; lo controla incrementColumnId en getSchema
    tableau.submit();
}
