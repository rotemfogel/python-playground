import json
from dataclasses import dataclass
from typing import List, Any, Union


@dataclass
class EverFlowRecord:
    subscription_name: str
    user_id: int
    product: str
    subscription_type: str
    rate_plan: str

    @classmethod
    def from_list(cls, record: List[Any]):
        return EverFlowRecord(subscription_name=record[0],
                              user_id=record[1],
                              product=record[2],
                              subscription_type=record[3],
                              rate_plan=record[4])


def to_everflow_record(records: Union[List[List[Any]], str]) -> List[EverFlowRecord]:
    if type(records) == str:
        records = json.loads(records)
    return [EverFlowRecord.from_list(r) for r in records]
