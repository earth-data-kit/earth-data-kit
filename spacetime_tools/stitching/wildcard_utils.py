from dateutil.relativedelta import relativedelta
import logging

logger = logging.getLogger(__name__)

year_patterns = ["YYYY", "yyyy"]
month_patterns = ["MM", "mm"]
day = ["d", "dd", "ddd", "D", "DD", "DDD"]


def resolve_month(pattern, start, end):
    pl = []
    for m in month_patterns:
        if f"!{m}" in pattern:
            diff_in_months = relativedelta(end, start).months
            logger.info (diff_in_months)

    return pl
def resolve_year(pattern, start, end):
    pl = []
    for y in year_patterns:
        if f"!{y}" in pattern:
            diff_in_years = relativedelta(end, start).years
            if diff_in_years < 0:
                raise Exception("end can't be less than start")
            cur_year = start.year
            end_year = end.year
            for i in range(cur_year, end_year+1):
                # TODO: Add code to handle trailing zeroes
                pl = pl + resolve_month(pattern.replace(f"!{y}", str(i)), start, end)

    return pl

def resolve_date_range(pattern, date_range):
    print (pattern, date_range)
    if not (date_range and len(date_range) == 2):
        return [pattern]

    start = date_range[0]
    end = date_range[1]

    pl = []

    pl = resolve_year(pattern, start, end)
    print (pl)
