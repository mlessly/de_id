""" Converts TSV to CSV.

Adapted from http://stackoverflow.com/a/2535337.

Examples:
    $ python tsv_to_csv.py < input.tsv > output.csv
"""
import sys
import csv

tabin = csv.reader(sys.stdin, dialect=csv.excel_tab)
commaout = csv.writer(sys.stdout, dialect=csv.excel)
for row in tabin:
    commaout.writerow(row)
