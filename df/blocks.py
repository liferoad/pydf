# standard libraries
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
        get_ipython  # noqa
        return beam.Pipeline(InteractiveRunner(), options=PipelineOptions())
    except NameError:
        return beam.Pipeline(DirectRunner(), options=PipelineOptions())


class Block(BaseModel):
    source: List["Block"] = []
    target: List["Block"] = []
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
        self.p = p

    @classmethod
    def Sequential(cls, blocks: List[Block], p: beam.pipeline.Pipeline):
        # connect all the blocks using the list order
        for i, block in enumerate(blocks):
            if i > 0:
                block.source = [blocks[i - 1]]
            if i < (len(blocks) - 1):
                block.target = [blocks[i + 1]]
        return cls(blocks, p)

    def compile(self):
        def _build_o(o, blocks, parsed_block):
            for block in blocks:
                if hex(id(block)) not in parsed_block:
                    block.o = o | block.operation
                    parsed_block.append(hex(id(block)))
                if block.target:
                    _build_o(block.o, block.target, parsed_block)
            return

        _build_o(self.p, self.blocks, [])
        return

    def show_graph(self):
        ib.show_graph(self.p)

    def block_data(self, block: Block) -> pd.DataFrame:
        return ib.collect(block.o)
