@echo off
echo [%date% %time%] Starte Ordnungsamt Tracker...

:: JSON herunterladen
powershell -Command "Invoke-WebRequest -Uri 'https://ordnungsamt.berlin.de/frontend.webservice.opendata/api/meldungen' -OutFile 'C:\Users\larsw\OneDrive\OA+\muell-monitor\meldungen.json'"

:: Tracker ausfuehren
python "C:\Users\larsw\OneDrive\OA+\muell-monitor\tracker.py"

:: HTML exportieren
python "C:\Users\larsw\OneDrive\OA+\muell-monitor\export_html.py"

echo [%date% %time%] Fertig!
