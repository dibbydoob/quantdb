from dateutil.relativedelta import relativedelta


class InvalidPeriodConfig(Exception):
    pass


def get_mongo_datetime_filter(period_start, period_end):
    if not period_start and not period_end:
        return {}
    if not period_start and period_end:
        return {'datetime': {"$lte": period_end}}
    if period_start and not period_end:
        return {'datetime': {"$gte": period_start}}
    if period_start and period_end:
        return {'datetime': {"$gte": period_end, "$lte": period_end}}


def map_dfreq_to_frequency(dfreq):
    assert (dfreq in ['t', 's', 'm', 'h', 'd', 'M', 'Y'])
    if dfreq == 't' or dfreq == 's':
        return 'seconds'
    if dfreq == 'm':
        return 'minutes'
    if dfreq == 'h' or dfreq == 'd' or dfreq == 'M' or dfreq == 'Y':
        return 'hours'


def map_granularity_to_relativedelta(granularity, duration):
    if granularity == "s":
        deltatime = relativedelta(seconds=duration)
    if granularity == "m":
        deltatime = relativedelta(minutes=duration)
    if granularity == "h":
        deltatime = relativedelta(hours=duration)

    if granularity == "d":
        deltatime = relativedelta(days=duration)

    if granularity == "M":
        deltatime = relativedelta(months=duration)

    if granularity == "y":
        deltatime = relativedelta(years=duration)

    assert (deltatime)

    return deltatime


def get_span(granularity, period_start=None, period_end=None, duration=None):
    assert (not duration or duration > 0)

    assert (not (period_start and period_end) or period_end > period_start)

    deltatime = None

    if duration:
        deltatime = map_granularity_to_relativedelta(granularity, duration)

    if not period_start and not period_end and not duration:
        return None, None

    if not period_start and not period_end and duration:
        raise InvalidPeriodConfig("cannot map period specifications to time window")

    if not period_start and period_end and not duration:
        return None, period_end

    if not period_start and period_end and duration:
        return period_end - deltatime, period_end

    if period_start and not period_end and not duration:
        return period_start, None

    if period_start and not period_end and duration:
        return period_start, period_start + deltatime

    if period_start and period_end and not duration:
        return period_start, period_end

    if period_start and period_end and duration:
        raise InvalidPeriodConfig("cannot map period specifications to time window")
