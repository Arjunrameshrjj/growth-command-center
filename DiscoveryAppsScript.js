/**
 * DISCOVERY APPS SCRIPT - Hardcoded Sheet List
 * Add new course sheet names to the COURSE_SHEETS array below.
 * Uses trimmed-name matching so tabs with trailing spaces still work.
 */

function doGet() {
    try {
        var ss = SpreadsheetApp.getActiveSpreadsheet();
        var result = [];

        // ---- ADD / REMOVE SHEET NAMES HERE ----
        var COURSE_SHEETS = [
            "OET (MVK)",
            "IELTS",
            "PTE",
            "FLUENCY",
            "GERMAN",
            "MEDIA SCHOOL (MVK)",
            "NCLEX RN (MVK)",
            "DIGITAL MARKETING (TVLA)",
            "PROMETRIC(TVLA)",
            "YOGA",
            "ZUMBA"
        ];
        // ----------------------------------------

        // Build a map: trimmedName -> sheet object (handles tabs with trailing spaces)
        var sheetMap = {};
        ss.getSheets().forEach(function (s) {
            sheetMap[s.getName().trim()] = s;
        });

        var COLUMNS = [
            "Funnel Stage", "Segmentation", "Content Tone & Themes", "Content Topic",
            "Media", "Scheduled Date", "Content Type", "Owner/TUTOR", "Assigned By",
            "Assigned Date", "Status", "Published Date", "REMARKS",
            "Link YT", "Link INSTA", "Link FB"
        ];

        COURSE_SHEETS.forEach(function (sheetName) {
            var sheet = sheetMap[sheetName]; // trim-matched lookup
            if (!sheet) return; // skip if sheet doesn't exist

            var lastRow = sheet.getLastRow();
            if (lastRow < 2) return;

            // Map headers dynamically
            var headerRow = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
            var colIndex = {};
            headerRow.forEach(function (h, i) {
                colIndex[h.toString().trim()] = i;
            });

            var data = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getValues();

            data.forEach(function (row) {
                var isEmpty = row.every(function (cell) { return cell === "" || cell === null; });
                if (isEmpty) return;

                var record = { "Sheet": sheetName }; // always use clean trimmed name
                COLUMNS.forEach(function (col) {
                    var idx = colIndex[col];
                    if (idx !== undefined) {
                        var val = row[idx];
                        if (val instanceof Date) {
                            val = Utilities.formatDate(val, Session.getScriptTimeZone(), "yyyy-MM-dd");
                        }
                        record[col] = (val !== null && val !== undefined) ? val.toString().trim() : "";
                    } else {
                        record[col] = "";
                    }
                });
                result.push(record);
            });
        });

        return ContentService
            .createTextOutput(JSON.stringify(result))
            .setMimeType(ContentService.MimeType.JSON);

    } catch (err) {
        return ContentService
            .createTextOutput(JSON.stringify({ error: err.message }))
            .setMimeType(ContentService.MimeType.JSON);
    }
}
