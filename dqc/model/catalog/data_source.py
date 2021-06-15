from pydantic import BaseModel


class DataSource(BaseModel):
    dataSourceId: str = None
    name: str = None
    sourceType: str = None
    # topic_dict: Dict = {}
    # pipeline_dict: Dict = {}
    # space_list: List = []
    # user_group_list: List = []
    mlPath: str = None
