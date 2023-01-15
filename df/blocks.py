# standard libraries
import inspect
import json
import types
import uuid
from enum import Enum
from typing import Any, Callable, Dict, ForwardRef, List, Optional, Union

# third party libraries
import apache_beam as beam
import apache_beam.runners.interactive.interactive_beam as ib
import pandas as pd
from apache_beam.dataframe import convert
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.direct import DirectRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
from pydantic import BaseModel, Extra, Field, root_validator
from sentence_transformers import SentenceTransformer
from sentence_transformers import util as st_util


class ModelType(str, Enum):
    SEQUENTIAL = "SEQUENTIAL"
    FUNCTIONAL = "FUNCTIONAL"


def create_beam_pipeline() -> beam.Pipeline:
    try:
        get_ipython  # type: ignore
        return beam.Pipeline(InteractiveRunner(), options=PipelineOptions(flags={}))
    except NameError:
        # always use InteractiveRunner for now.
        # Possible a bug in Beam (_skip_display not set)
        return beam.Pipeline(InteractiveRunner(DirectRunner()), options=PipelineOptions(flags={}))


class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


Block = ForwardRef("Block")


class Block(BaseModel):
    """Base class for any block"""

    block_type: str
    block_id: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4, description="block UUID4")
    source_ids: List[uuid.UUID] = []
    target_ids: List[uuid.UUID] = []
    operation: Optional[Union[beam.ParDo, beam.Create, beam.PTransform]] = Field(
        None, description="the block operation"
    )
    o: beam.pvalue.PCollection = Field(None, description="the output PCollection for the above operation")
    _sources: List[Block] = []
    _targets: List[Block] = []

    class Config:
        arbitrary_types_allowed = True
        underscore_attrs_are_private = True
        extra = Extra.allow

        json_encoders = {
            types.FunctionType: lambda v: {v.__name__: inspect.getsource(v)},
        }

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
    """Block for the create transform"""

    block_type: str = "Create"
    values: List[Any] = Field(..., description="a list of values to create as the Beam input data")

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values["operation"] = beam.Create(values["values"])
        return values

    def __call__(self, sources: List[Block] = None) -> Block:
        raise ValueError("CreateBlock cannot be callable")


class ReadCSVBlock(Block):
    """Block to read a csv file"""

    block_type: str = "ReadCSV"
    path: str = Field(..., description="a path string for a csv file")

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        values_1 = values.copy()
        path = values_1.pop("path")
        values["operation"] = read_csv(path, **values_1)
        return values

    def __call__(self, sources: List[Block] = None) -> Block:
        raise ValueError("ReadCSVBlock cannot be callable")


class SentenceEmbeddingBlock(Block):
    """Block for sentence embedding using transformer-based ML models"""

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
    """On the fly cross join block"""

    block_type: str = "CrossJoin"

    def __call__(self, sources: List[Block] = None) -> Block:
        super().__call__(sources)
        if sources[0].o and sources[1].o:
            self.o = sources[0].o | f"{self.block_type} - {self.block_id}" >> beam.FlatMap(
                _cross_join, rights=beam.pvalue.AsIter(sources[1].o)
            )
        return self


class DataTransformBlock(Block):
    """Block for transforming the data using a list of callable functions"""

    block_type: str = "DataTransform"
    callbacks: Union[List[Callable], List[Dict[str, str]]] = Field(
        ..., description="A list of callable functions to transform the data"
    )

    @root_validator(pre=True)
    def _set_fields(cls, values: dict) -> dict:
        for one_callback in values["callbacks"]:
            if isinstance(one_callback, types.FunctionType):
                one_op = beam.Map(one_callback)
                if not values.get("operation", None):
                    values["operation"] = one_op
                else:
                    values["operation"] = values["operation"] | one_op
            else:
                # parse the dict to create the operation
                # this supports a list of map operations
                for one_func in one_callback:
                    exec(one_callback[one_func])
                    one_op = beam.Map(locals()[one_func])
                    if not values.get("operation", None):
                        values["operation"] = one_op
                    else:
                        values["operation"] = values["operation"] | one_op
        return values


class CosSimilarityBlock(Block):
    """Block for computing cosine similarity for two vectors"""

    block_type: str = "CosSimilarity"
    operation: beam.ParDo = beam.Map(lambda x: float(st_util.cos_sim(x[0], x[1])[0][0]))


class BlockAssembler:
    """Block assembler"""

    def __init__(
        self,
        blocks: Union[Block, List[Block]],
        p: beam.pipeline.Pipeline = None,
        model_type: ModelType = ModelType.FUNCTIONAL,
    ):
        if not isinstance(blocks, list):
            blocks = [blocks]

        if model_type == ModelType.FUNCTIONAL:
            for block in blocks:
                if block.block_type != "Create":
                    raise ValueError(
                        "Cannot build the model with functional API since only Create blocks are supported now"
                    )

        self.blocks = blocks
        self._source_blocks = blocks
        self.model_type = model_type
        self._is_compiled = False

        if model_type == ModelType.SEQUENTIAL:
            self.id_to_block = {block.block_id: block for block in blocks}
        else:
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

            # set all blocks
            self.blocks = [self.id_to_block[id] for id in self.id_to_block]

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
        return cls(blocks, p, ModelType.SEQUENTIAL)

    def compile(self):
        if self._is_compiled:
            raise ValueError("Graph can only be compiled once. Please rebuild the model.")

        def _build_o(o, blocks, parsed_block):
            for block in blocks:
                if block.block_id not in parsed_block:
                    if block.operation:
                        if isinstance(o, beam.dataframe.frames.DeferredDataFrame):
                            block.o = (
                                convert.to_pcollection(o) | f"{block.block_type} - {block.block_id}" >> block.operation
                            )
                        else:
                            block.o = o | f"{block.block_type} - {block.block_id}" >> block.operation
                    else:
                        block(block._sources)
                if block.o is not None:
                    parsed_block.append(block.block_id)
                    if block.target_ids:
                        targets = [self.id_to_block[t] for t in block.target_ids]
                        _build_o(block.o, targets, parsed_block)
            return

        _build_o(self.p, self._source_blocks, [])
        self._is_compiled = True
        return

    def show_graph(self):
        # auto compile
        if not self._is_compiled:
            self.compile()
        ib.show_graph(self.p)

    def block_data(self, block: Block) -> pd.DataFrame:
        # auto compile
        if not self._is_compiled:
            self.compile()
        return ib.collect(block.o)

    def to_dict(self) -> dict:
        return {
            "blocks": [block.dict(exclude={"operation", "o"}) for _, block in self.id_to_block.items()],
            "model_type": self.model_type,
        }

    def to_json(self, **kwargs) -> str:
        return json.dumps(self.to_dict(), cls=UUIDEncoder, **kwargs)

    @classmethod
    def from_json(cls, json_str: str, p: beam.pipeline.Pipeline = None):
        block_dicts = json.loads(json_str)
        blocks = []
        input_blocks = []
        for block_dict in block_dicts["blocks"]:
            if block_dict.get("block_type") == "Create":
                create_b = CreateBlock(**block_dict)
                blocks.append(create_b)
                input_blocks.append(create_b)
            elif block_dict.get("block_type") == "SentenceEmbedding":
                blocks.append(SentenceEmbeddingBlock(**block_dict))
            elif block_dict.get("block_type") == "CrossJoin":
                blocks.append(CrossJoinBlock(**block_dict))
            elif block_dict.get("block_type") == "CosSimilarity":
                blocks.append(CosSimilarityBlock(**block_dict))
            elif block_dict.get("block_type") == "DataTransform":
                blocks.append(DataTransformBlock(**block_dict))
            else:
                raise ValueError(f"wrong block information: {block_dict}")
        if block_dicts["model_type"] == ModelType.FUNCTIONAL.value:
            id_maps = {block.block_id: block for block in blocks}
            for block in blocks:
                block._targets = [id_maps[t_id] for t_id in block.target_ids]
                block._sources = [id_maps[s_id] for s_id in block.source_ids]
            return cls(input_blocks, p, block_dicts["model_type"])
        else:
            return cls(blocks, p, block_dicts["model_type"])
