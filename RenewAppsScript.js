function doGet() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sheet = ss.getSheetByName("Sheet");

  if (!sheet) {
    return ContentService.createTextOutput(JSON.stringify({ error: "Sheet 'Sheet' not found" }))
      .setMimeType(ContentService.MimeType.JSON);
  }

  var data = sheet.getDataRange().getValues();
  if (data.length < 2) {
    return ContentService.createTextOutput(JSON.stringify([]))
      .setMimeType(ContentService.MimeType.JSON);
  }

  var headers = data[0];
  var jsonResult = [];

  for (var i = 1; i < data.length; i++) {
    var row = data[i];
    var obj = {};
    for (var j = 0; j < headers.length; j++) {
      var header = headers[j].toString().trim();
      var value = row[j];

      // Convert dates to string for JSON if they are Date objects
      if (value instanceof Date) {
        obj[header] = value.toISOString();
      } else {
        obj[header] = value;
      }
    }
    jsonResult.push(obj);
  }

  return ContentService.createTextOutput(JSON.stringify(jsonResult))
    .setMimeType(ContentService.MimeType.JSON);
}
