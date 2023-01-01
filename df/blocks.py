# standard libraries
import json
import uuid
from typing import Any, ForwardRef, List, Optional, Union

# third party libraries
import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct import DirectRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from pydantic import BaseModel, Field, root_validator
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as st_util


def create_beam_pipeline() -> beam.Pipeline:
    try:
        get_ipython  # type: ignore
        return beam.Pipeline(InteractiveRunner(), options=PipelineOptions(flags={}))
    except NameError:
        return beam.Pipeline(DirectRunner(), options=PipelineOptions(flags={}))


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


Block = ForwardRef("Block")


class Block(BaseModel):
    block_type: str
    block_id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4, description="block UUID4")
    source_ids: List[uuid.UUID] = []
    target_ids: List[uuid.UUID] = []
    operation: Optional[Union[beam.ParDo, beam.Create]] = Field(None, description="the block operation")
    o: beam.pvalue.PCollection = Field(None, description="the output PCollection for the above operation")
    _sources: List[Block] = []
    _targets: List[Block] = []

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True

    def __call__(self, sources: Union[Block, List[Block]]) -> Block:
        if not isinstance(sources, list):
            sources = [sources]
        self._sources = sources
        for block in sources:
            if block.block_id not in self.source_ids:
                self.source_ids.append(block.block_id)
            if block.block_id not in self.target_ids:
                block.target_ids.append(self.block_id)
            block._targets.append(self)
        return self


Block.update_forward_refs()


class CreateBlock(Block):
    block_type: str = "Create"
    values: List[Any] = Field(..., description="a list of values to create as the Beam input data")

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values["operation"] = beam.Create(values["values"])
        return values

    def __call__(self, sources: List[Block] = None) -> Block:
        raise ValueError("CreateBlock cannot be callable")


class SentenceEmbeddingBlock(Block):
    block_type: str = "SentenceEmbedding"
    model_name: Optional[str] = Field(
        default="all-MiniLM-L6-v2", description="a model name supported by sentence_transformers"
    )

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values["operation"] = beam.Map(
            lambda x: [SentenceTransformer(values.get("model_name", "all-MiniLM-L6-v2")).encode(x)]
        )
        return values


def _cross_join(left, rights):
    for x in rights:
        yield (left, x)


class CrossJoinBlock(Block):
    """on the fly cross join block"""

    block_type: str = "CrossJoin"

    def __call__(self, sources: List[Block] = None) -> Block:
        super().__call__(sources)
        if sources[0].o and sources[1].o:
            self.o = sources[0].o | f"{self.block_type} - {self.block_id}" >> beam.FlatMap(
                _cross_join, rights=beam.pvalue.AsIter(sources[1].o)
            )
        return self


class CosSimilarityBlock(Block):
    block_type: str = "CosSimilarity"
    operation: beam.ParDo = beam.Map(lambda x: float(st_util.cos_sim(x[0], x[1])[0][0]))


class BlockAssembler:
    def __init__(self, blocks: Union[Block, List[Block]], p: beam.pipeline.Pipeline = None):
        if not isinstance(blocks, list):
            if blocks.block_type != "Create":
                raise ValueError("Cannot build")
            blocks = [blocks]

        self.blocks = blocks
        self.id_to_block = {}

        # parse all blocks
        def _get_all_blocks(_blocks):
            for _block in _blocks:
                self.id_to_block[_block.block_id] = _block
                if _block._targets:
                    for s in _block._targets:
                        self.id_to_block[s.block_id] = s
                    _get_all_blocks(_block._targets)
            return

        _get_all_blocks(self.blocks)

        if p:
            self.p = p
        else:
            self.p = create_beam_pipeline()

    @classmethod
    def Sequential(cls, blocks: List[Block], p: beam.pipeline.Pipeline = None):
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
                    if block.operation:
                        block.o = o | f"{block.block_type} - {block.block_id}" >> block.operation
                    else:
                        block(block._sources)
                if block.o:
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

    def to_dict(self) -> dict:
        return {"blocks": [block.dict(exclude={"operation", "o"}) for block in self.blocks]}

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(), cls=UUIDEncoder, **kwargs)

    @classmethod
    def from_json(cls, json_str: str, p: beam.pipeline.Pipeline = None):
        block_dicts = json.loads(json_str)
        blocks = []
        for block_dict in block_dicts["blocks"]:
            if block_dict.get("block_type") == "Create":
                blocks.append(CreateBlock(**block_dict))
            elif block_dict.get("block_type") == "SentenceEmbedding":
                blocks.append(SentenceEmbeddingBlock(**block_dict))
            else:
                raise ValueError(f"wrong block information: {block_dict}")
        return cls(blocks, p)
