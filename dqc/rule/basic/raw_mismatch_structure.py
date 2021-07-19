import logging

from pandas import DataFrame

log = logging.getLogger("app." + __name__)


##TODO raw_mismatch_structure

def init():
    def raw_mismatch_structure(df: DataFrame, topic, rule=None):
        raise NotImplementedError("raw_mismatch_structure error")

    return raw_mismatch_structure
