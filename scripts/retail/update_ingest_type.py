def update_ingest_type_logic(item, **kwargs):
    from airflow.models.variable import Variable

    # Update variable dengan data yang baru
    list_ingest_type = kwargs['var']['json'].get("retail_ingest_type", {})
    list_ingest_type[item['table']] = "incremental"

    Variable.set(
        key="retail_ingest_type",
        value=list_ingest_type,
        serialize_json=True,
    )
