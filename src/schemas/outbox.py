from pydantic import BaseModel


class OutboxRelayResult(BaseModel):
    claimed: int
    published: int
    failed: int
