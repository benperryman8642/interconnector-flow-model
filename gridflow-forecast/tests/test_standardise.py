from gridflow.etl.silver.standardise import standardise

def test_standardise_identity():
    data = {"a":1}
    assert standardise(data) == data
