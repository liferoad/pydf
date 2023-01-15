# standard libraries
from pathlib import Path

# pydf libraries
from df import blocks as blocks

SENTENCES = [
    "This framework generates embeddings for each input sentence",
    "Sentences are passed as a list of string.",
    "The quick brown fox jumps over the lazy dog.",
]

CURRENT_PATH = Path(__file__).absolute().parent


def test_blocks_sequential():

    create = blocks.CreateBlock(values=SENTENCES)
    embed = blocks.SentenceEmbeddingBlock()
    model = blocks.BlockAssembler.Sequential([create, embed])
    model.compile()
    model.show_graph()
    pdf = model.block_data(model.blocks[1])
    assert pdf.shape == (3, 1)
    model_in_json = model.to_json(indent=2)
    assert model_in_json
    new_model = blocks.BlockAssembler.from_json(model_in_json)
    new_model.show_graph()
    pdf_1 = new_model.block_data(new_model.blocks[1])
    assert pdf_1.shape == (3, 1)


def test_blocks_functional_api():
    create_1 = blocks.CreateBlock(values=SENTENCES)
    embed_1 = blocks.SentenceEmbeddingBlock()([create_1])
    target_sentences = ["This framework is for testing"]
    create_2 = blocks.CreateBlock(values=target_sentences)
    embed_2 = blocks.SentenceEmbeddingBlock()([create_2])
    cross = blocks.CrossJoinBlock()([embed_1, embed_2])
    sim = blocks.CosSimilarityBlock()(cross)
    model_1 = blocks.BlockAssembler([create_1, create_2])
    pdf = model_1.block_data(sim)
    assert pdf.shape == (3, 1)
    model_1_config = model_1.to_json(indent=2)
    new_model = blocks.BlockAssembler.from_json(model_1_config)
    for block in new_model.blocks:
        if isinstance(block, blocks.CosSimilarityBlock):
            pdf_1 = new_model.block_data(block)
            assert pdf_1.shape == (3, 1)


def test_read_csv_block():
    csv_b = blocks.ReadCSVBlock(path=str(CURRENT_PATH / "beers.csv"), header=0)
    model = blocks.BlockAssembler.Sequential([csv_b])
    pdf = model.block_data(csv_b)
    assert pdf.shape == (2410, 8)


def _del_unwanted_cols(data):
    """Delete the unwanted columns"""
    del data["ibu"]
    del data["brewery_id"]
    return data


def _discard_incomplete(data):
    """Filters out records based on id"""
    return data["id"] > 100


def test_data_transform_block():

    dt_b = blocks.DataTransformBlock(callbacks=[_del_unwanted_cols, _discard_incomplete])
    assert len(dt_b.callbacks) == 2

    dtb_b_in_json = dt_b.json(exclude={"o", "operation"})
    dt_b1 = blocks.DataTransformBlock.parse_raw(dtb_b_in_json)
    assert len(dt_b1.callbacks) == 2
