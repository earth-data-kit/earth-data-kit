from dateutil.relativedelta import relativedelta
import logging
from dateutil.rrule import rrule, MONTHLY
import copy

logger = logging.getLogger(__name__)

year_patterns = ["!YYYY!", "!yyyy!"]
month_patterns = ["!MM!", "!mm!"]
# Order is important, don't change
date_patterns = ["!ddd!", "!DDD!", "!dd!", "!DD!", "!d!", "!D!"]


def resolve_pattern(pattern, dates):
    pl = []
    for dt in dates:
        pat = copy.copy(pattern)
        year = dt.year
        month = dt.month

        # Iterates over all dates and replaces year and month patterns with all values
        for yp in year_patterns:
            if yp in pat:
                if yp.upper() == yp:
                    # Add trailing zeros
                    pat = pat.replace(yp, f"{year:04d}")
                else:
                    pat = pat.replace(yp, f"{year}")

        for mp in month_patterns:
            if mp in pat:
                if mp.upper() == mp:
                    # Add trailing zeros
                    pat = pat.replace(mp, f"{month:02d}")
                else:
                    pat = pat.replace(mp, f"{month}")

        for dp in date_patterns:
            if dp in pat:
                # Replacing date with * as we can sync parallely based on month
                pat = pat.replace(dp, "*")
        pl.append(pat)
    return pl


def resolve_time_filters(pattern, date_range):
    # print (pattern, date_range)
    if not (date_range and len(date_range) == 2):
        return [pattern]

    start = date_range[0]
    end = date_range[1]

    pl = []

    # Starts iterating monthly and gets all possible dates
    dates = [dt for dt in rrule(MONTHLY, dtstart=start, until=end)]
    pl = resolve_pattern(pattern, dates)
    return list(set(pl))
