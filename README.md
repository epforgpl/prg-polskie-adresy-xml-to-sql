## Polish address points PRG dataset XML to SQL importer.

This repository holds a script for importing Polish address points
obtained from PRG (Państwowy Rejestr Granic) from XML format into
a MySQL table.

As of 11/2018, the input files for this script can be downloaded
[here](http://www.gugik.gov.pl/pzgik/dane-bez-oplat/dane-z-panstwowego-rejestru-granic-i-powierzchni-jednostek-podzialow-terytorialnych-kraju-prg).

They are the `PRG – punkty adresowe` dataset in *.GML format.

The script creates a table `punkty_adresowe` in the given DB & schema.
