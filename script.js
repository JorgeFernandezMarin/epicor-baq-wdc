(function () {

  var myConnector = tableau.makeConnector();

  myConnector.init = function (initCallback) {
    initCallback();
  };

  // 1. Definir esquema leyendo el primer registro de value[]
  myConnector.getSchema = function (schemaCallback) {

    var connData = JSON.parse(tableau.connectionData);
    var funcUrl = connData.funcUrl;

    fetch(funcUrl)
      .then(function (r) { return r.json(); })
      .then(function (data) {

        if (!data.value || data.value.length === 0) {
          tableau.abortWithError("La función no regresó registros en 'value'.");
          return;
        }

        var sample = data.value[0];
        var cols = [];

        for (var key in sample) {
          if (sample.hasOwnProperty(key)) {

            var val = sample[key];
            var type = tableau.dataTypeEnum.string;

            // Heurísticas simples de tipo:
            if (typeof val === "number") {
              type = Number.isInteger(val)
                ? tableau.dataTypeEnum.int
                : tableau.dataTypeEnum.float;
            } else if (val instanceof Date) {
              type = tableau.dataTypeEnum.datetime;
            } else if (typeof val === "string") {
              // ejemplo específico:
              if (key === "InvcHead_CreatedOn" || key.endsWith("Date") || key.endsWith("On")) {
                // Tableau normalmente te manda '2025-11-30T00:00:00'
                type = tableau.dataTypeEnum.datetime;
              }
            }

            cols.push({
              id: key,
              alias: key,
              dataType: type
            });
          }
        }

        var tableSchema = {
          id: "EpicorBAQ",
          alias: "Epicor BAQ Data (Azure Function)",
          columns: cols
        };

        schemaCallback([tableSchema]);
      })
      .catch(function (err) {
        tableau.abortWithError("Error al obtener esquema desde Azure Function: " + err);
      });
  };

  // 2. Obtener datos completos desde la Function
  myConnector.getData = function (table, doneCallback) {

    var connData = JSON.parse(tableau.connectionData);
    var funcUrl = connData.funcUrl;

    fetch(funcUrl)
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

})();

function submitWDC() {
  var funcUrl = document.getElementById("funcUrl").value.trim();

  if (!funcUrl) {
    alert("Debes ingresar la URL de la Azure Function.");
    return;
  }

  var connData = {
    funcUrl: funcUrl
  };

  tableau.connectionName = "Epicor BAQ via Azure Function";
  tableau.connectionData = JSON.stringify(connData);
  tableau.submit();
}
