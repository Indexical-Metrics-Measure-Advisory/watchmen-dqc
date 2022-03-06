from enum import Enum


class RuleCode(str, Enum):
    # Topic Rule
    ROWS_NOT_EXISTS = "rows-not-exists"
    ROWS_NO_CHANGE = "rows-no-change"
    ROWS_COUNT_MISMATCH_AND_ANOTHER = "rows-count-mismatch-and-another"

    # Factor Rule mismatch
    FACTOR_MISMATCH_TYPE = "factor-mismatch-type"
    FACTOR_MISMATCH_ENUM = "factor-mismatch-enum"
    FACTOR_MISMATCH_DATE_TYPE = "factor-mismatch-date-type"
    FACTOR_STRING_LENGTH_MISMATCH = "factor-string-length-mismatch"

    # Factor Rule Over
    FACTOR_COMMON_VALUE_OVER_COVERAGE = "factor-common-value-over-coverage"
    FACTOR_EMPTY_OVER_COVERAGE = "factor-empty-over-coverage"

    # Factor Rule Value
    FACTOR_IS_EMPTY = "factor-is-empty"
    FACTOR_IS_BLANK = "factor-is-blank"
    FACTOR_USE_CAST = "factor-use-cast"

    # Factor Rule Regexp
    FACTOR_MATCH_REGEXP = "factor-match-regexp"
    FACTOR_MISMATCH_REGEXP = "factor-mismatch-regexp"

    # Factor Rule Another
    FACTOR_AND_ANOTHER = "factor-and-another"

    # Factor Rule range
    FACTOR_COMMON_VALUE_NOT_IN_RANGE = "factor-common-value-not-in-range"
    FACTOR_STRING_LENGTH_NOT_IN_RANGE = "factor-string-length-not-in-range"
    FACTOR_NOT_IN_RANGE = "factor-not-in-range"
    FACTOR_MAX_NOT_IN_RANGE = "factor-max-not-in-range"
    FACTOR_MIN_NOT_IN_RANGE = "factor-min-not-in-range"
    FACTOR_AVG_NOT_IN_RANGE = "factor-avg-not-in-range"
    FACTOR_MEDIAN_NOT_IN_RANGE = "factor-median-not-in-range"
    FACTOR_QUANTILE_NOT_IN_RANGE = "factor-quantile-not-in-range"
    FACTOR_STDEV_NOT_IN_RANGE = "factor-stdev-not-in-range"


class FactorType(str, Enum):
    SEQUENCE = 'sequence'
    NUMBER = 'number'
    UNSIGNED = 'unsigned'  # 0 & positive
    TEXT = 'text'

    # address
    ADDRESS = 'address',
    CONTINENT = 'continent',
    REGION = 'region',
    COUNTRY = 'country',
    PROVINCE = 'province',
    CITY = 'city',
    DISTRICT = 'district',
    ROAD = 'road',
    COMMUNITY = 'community',
    FLOOR = 'floor',
    RESIDENCE_TYPE = 'residence-type',
    RESIDENTIAL_AREA = 'residential-area',

    # contact electronic
    EMAIL = 'email',
    PHONE = 'phone',
    MOBILE = 'mobile',
    FAX = 'fax',

    # date time related
    DATETIME = 'datetime'  # YYYY - MM - DD HH: mm:ss
    FULL_DATETIME = 'full-datetime'  # YYYY - MM - DD HH: mm:ss.SSS
    DATE = 'date'  # YYYY - MM - DD
    TIME = 'time'  # HH: mm:ss
    YEAR = 'year'  # 4 digits
    HALF_YEAR = 'half-year'  # 1: first half, 2: second half
    QUARTER = 'quarter'  # 1 - 4
    MONTH = 'month'  # 1 - 12
    HALF_MONTH = 'half-month'  # 1: first half, 2: second half
    TEN_DAYS = 'ten-days'  # 1, 2, 3
    WEEK_OF_YEAR = 'week-of-year'  # 0(the partial week that precedes the first Sunday of the year) - 53(leap year)
    WEEK_OF_MONTH = 'week-of-month'  # 0(the partial week that precedes the first Sunday of the year) - 5
    HALF_WEEK = 'half-week'  # 1: first half, 2: second half
    DAY_OF_MONTH = 'day-of-month'  # 1 - 31, according to month / year
    DAY_OF_WEEK = 'day-of-week'  # 1(Sunday) - 7(Saturday)
    DAY_KIND = 'day-kind'  # 1: workday, 2: weekend, 3: holiday
    HOUR = 'hour'  # 0 - 23
    HOUR_KIND = 'hour-kind'  # 1: work time, 2: off hours, 3: sleeping time
    MINUTE = 'minute'  # 0 - 59
    SECOND = 'second'  # 0 - 59
    MILLISECOND = 'millisecond'  # 0 - 999
    AM_PM = 'am-pm'  # 1, 2

    # individual
    GENDER = 'gender'
    OCCUPATION = 'occupation'
    DATE_OF_BIRTH = 'date-of-birth'  # YYYY - MM - DD
    AGE = 'age'
    ID_NO = 'id-no'
    RELIGION = 'religion'
    NATIONALITY = 'nationality'

    # organization
    BIZ_TRADE = 'biz-trade'
    BIZ_SCALE = 'biz-scale'

    BOOLEAN = 'boolean'

    ENUM = 'enum'

    OBJECT = 'object'
    ARRAY = 'array'
