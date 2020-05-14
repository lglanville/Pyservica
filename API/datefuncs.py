from datetime import datetime, timedelta
from calendar import monthrange
import re
FORMATS = ["%d %B %Y", "%d %b %Y", "%d %b %y", "%d %B %y"]
MFORMATS = ["%B %Y", "%b %Y", "%b %y", "%B %y"]
DELIMS = r'[./\, ]'
PUNC = ['.,?']


def from_string(datestring, latest=False):
    datestring = datestring.title()
    date_comp = datestring.split()
    dt = None
    circa = False
    if datestring.startswith('C.'):
        circa = True
    if len(date_comp[0]) == 1 and date_comp[0].isnumeric():
        datestring = '0'+datestring
    if len(datestring.split()) == 3:
        for fm in FORMATS:
            try:
                dt = datetime.strptime(datestring, fm)
                if latest:
                    dt = dt + timedelta(hours=23, minutes=59, seconds=59)
            except Exception:
                pass
    elif len(datestring.split()) == 2:
        for fm in MFORMATS:
            try:
                dt = datetime.strptime(datestring, fm)
                if latest:
                    dt = upper_month(dt)
            except Exception:
                pass
    elif len(datestring.split()) == 1:
        years = re.findall(r'\d{4}', datestring)
        if len(years) == 1:
            year = int(years[0])
            if latest:
                dt = datetime(year, 12, 31, 23, 59, 59)
            else:
                dt = datetime(year, 1, 1)
    if circa:
        if latest:
            dt = dt+timedelta(days=365*5+1)
        else:
            dt = dt-timedelta(days=365*5+1)
    return dt


def upper_month(dt):
    delta = monthrange(dt.year, dt.month)[1] - dt.day
    dt = dt + timedelta(days=delta, hours=23, minutes=59, seconds=59)
    return dt


def upper_year(dt):
    dt = datetime(dt.year, 12, 31, 23, 59, 59)
    return dt


def from_delim(datestring, latest=False):
    date_comp = re.split(DELIMS, datestring)
    dt = None
    if len(date_comp) == 3:
        try:
            date_comp = [int(d) for d in date_comp]
            date_comp.reverse()
            dt = datetime(*date_comp)
            if latest:
                dt = dt + timedelta(hours=23, minutes=59, seconds=59)
        except Exception:
            pass
    elif len(date_comp) == 2:
        try:
            date_comp = [int(d) for d in date_comp]
            date_comp.reverse()
            date_comp.append(1)
            dt = datetime(*date_comp)
            if latest:
                dt = upper_month(dt)
        except Exception:
            pass
    elif len(date_comp) == 1:
        try:
            date_comp = [int(d) for d in date_comp]
            date_comp.reverse()
            date_comp.extend([1, 1])
            dt = datetime(*date_comp)
            if latest:
                dt = upper_year(dt)
        except Exception:
            pass
    return dt


def string_to_date(datestring, latest=False):
    date_comp = re.split(DELIMS, datestring)
    bools = [d.isnumeric() for d in date_comp]
    dt = None
    if bools == [True, False, True]:
        dt = from_string(datestring, latest=latest)
    elif bools == [True, True, True]:
        dt = from_delim(datestring, latest=latest)
    elif bools == [False, True]:
        dt = from_string(datestring, latest=latest)
    elif bools == [True, True]:
        dt = from_delim(datestring, latest=latest)
    elif bools == [True]:
        dt = from_delim(datestring, latest=latest)
    elif bools == [False]:
        dt = from_string(datestring, latest=latest)
    return dt


def reformat_date(datestring, latest=False):
    date_comp = re.split(DELIMS, datestring)
    dt = string_to_date(datestring)
    if dt is not None:
        if len(date_comp) == 3:
            return dt.strftime("%d %B %Y").lstrip('0')
        elif len(date_comp) == 2:
            return dt.strftime("%B %Y")
        elif len(date_comp) == 1:
            return dt.strftime("%Y")


def get_iso(coverage):
    iso_dates = []
    datestrings = coverage.split('-')
    earliest = string_to_date(datestrings[0].strip())
    latest = string_to_date(datestrings[-1].strip(), latest=True)
    if earliest is not None:
        iso_dates.append(earliest.isoformat())
    if latest is not None:
        iso_dates.append(latest.isoformat())
    return iso_dates


def reformat_coverage(coverage, latest=False):
    datestrings = coverage.split('-')
    datelist = []
    years = []
    for datestring in datestrings:
        datestring = datestring.strip()
        for p in PUNC:
            datestring = datestring.replace(p, '')
        date_comp = re.split(DELIMS, datestring)
        dt = string_to_date(datestring)
        if dt is not None:
            years.append(dt.year)
            if len(date_comp) == 3:
                datelist.append(dt.strftime("%d %B %Y").lstrip('0'))
            elif len(date_comp) == 2:
                datelist.append(dt.strftime("%B %Y"))
            elif len(date_comp) == 1:
                datelist.append(dt.strftime("%Y"))
    print(years)
    return [' - '.join(datelist), min(years), max(years)]


if __name__ == '__main__':
    import sys
    print(get_iso(sys.argv[1]))
