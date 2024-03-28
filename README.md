# IDFM-live-map

## Get latest timetable
`cd process-live-data` and run `jupyter nbconvert --execute --to notebook parse_static_gtfs.ipynb --output logs/parse_static_gtfs`

## Extract map

Use http://bboxfinder.com/#48.045038,1.340332,49.353756,3.718872 to extract boundaries of Ile-de-France.

Download pmtiles from protomaps. See https://docs.protomaps.com/guide/getting-started.

Run `pmtiles extract https://build.protomaps.com/20240316.pmtiles idf.pmtiles --bbox=1.340332,48.045038,3.718872,49.353756`