# standard libraries
import uuid
from typing import Any, List, Optional, Union

# third party libraries
import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct import DirectRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from pydantic import BaseModel, Field, root_validator
from sentence_transformers import SentenceTransformer


def create_beam_pipeline() -> beam.Pipeline:
    try:
        get_ipython  # type: ignore
        return beam.Pipeline(InteractiveRunner(), options=PipelineOptions())
    except NameError:
        return beam.Pipeline(DirectRunner(), options=PipelineOptions())


class Block(BaseModel):
    block_id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4, description="block UUID4")
    source_ids: List[uuid.UUID] = []
    target_ids: List[uuid.UUID] = []
    operation: Union[beam.ParDo, beam.Create] = Field(..., description="the block operation")
    o: beam.pvalue.PCollection = Field(None, description="the output PCollection for the above operation")

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True


class CreateBlock(Block):
    values: List[Any] = Field(..., description="a list of values to create as the Beam input data")

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values["operation"] = beam.Create(values["values"])
        return values


class SentenceEmbeddingBlock(Block):
    model_name: Optional[str] = Field(
        default="all-MiniLM-L6-v2", description="a model name supported by sentence_transformers"
    )

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values["operation"] = beam.Map(
            lambda x: [SentenceTransformer(values.get("model_name", "all-MiniLM-L6-v2")).encode(x)]
        )
        return values


class BlockAssembler:
    def __init__(self, blocks: List[Block], p: beam.pipeline.Pipeline):
        self.blocks = blocks
        self.id_to_block = {block.block_id: block for block in blocks}
        self.p = p

    @classmethod
    def Sequential(cls, blocks: List[Block], p: beam.pipeline.Pipeline):
        # connect all the blocks using the list order
        for i, block in enumerate(blocks):
            if i > 0:
                block.source_ids = [blocks[i - 1].block_id]
            if i < (len(blocks) - 1):
                block.target_ids = [blocks[i + 1].block_id]
        return cls(blocks, p)

    def compile(self):
        def _build_o(o, blocks, parsed_block):
            for block in blocks:
                if block.block_id not in parsed_block:
                    block.o = o | block.operation
                    parsed_block.append(block.block_id)
                if block.target_ids:
                    targets = [self.id_to_block[t] for t in block.target_ids]
                    _build_o(block.o, targets, parsed_block)
            return

        _build_o(self.p, self.blocks, [])
        return

    def show_graph(self):
        ib.show_graph(self.p)

    def block_data(self, block: Block) -> pd.DataFrame:
        return ib.collect(block.o)
