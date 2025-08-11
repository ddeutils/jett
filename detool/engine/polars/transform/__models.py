from pydantic import BaseModel, Field


class ColumnMap(BaseModel):
    """Column Map model."""

    name: str = Field(description="A new column name.")
    source: str = Field(
        description="A source column statement before alias with alias.",
    )
