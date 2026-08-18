import dlt

@dlt.resource
def parent():
    yield [{"id": 1}, {"id": 2}]
    
@dlt.transformer(data_from=parent)
def child(item):
    print(f"Received item: {type(item)}")
    yield {"child_id": item["id"]}

pipeline = dlt.pipeline(pipeline_name="test", destination="dummy")
pipeline.extract(child)
