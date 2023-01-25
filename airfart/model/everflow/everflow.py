from dataclasses import dataclass
from typing import List, Any


@dataclass
class EverFlowRecord:
    subscription_name: str
    user_id: int
    product: str
    subscription_type: str
    rate_plan: str
    attribution_affid: int
    attribution_oid: int
    attribution_transaction: str

    @classmethod
    def from_list(cls, record: List[Any]):
        return EverFlowRecord(
            subscription_name=record[0],
            user_id=record[1],
            product=record[2],
            subscription_type=record[3],
            rate_plan=record[4],
            attribution_affid=record[5],
            attribution_oid=record[6],
            attribution_transaction=record[7],
        )


def to_everflow_record(records: List[Any]) -> List[EverFlowRecord]:
    try:
        return list(map(lambda r: EverFlowRecord.from_list(r), records))
    except Exception:
        return []
