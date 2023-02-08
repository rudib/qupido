use std::sync::Arc;

use deltalake::{DeltaOps, SchemaField, SchemaDataType, arrow::{record_batch::RecordBatch, datatypes::{Schema, Field, DataType}, array::{Int32Array, StringArray}}, operations::collect_sendable_stream};

fn get_table_columns() -> Vec<SchemaField> {
    vec![
        SchemaField::new(
            String::from("int"),
            SchemaDataType::primitive(String::from("integer")),
            false,
            Default::default(),
        ),
        SchemaField::new(
            String::from("string"),
            SchemaDataType::primitive(String::from("string")),
            true,
            Default::default(),
        ),
    ]
}

fn get_table_batches() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("int", DataType::Int32, false),
        Field::new("string", DataType::Utf8, true),
    ]));

    let int_values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    let str_values = StringArray::from(vec!["A", "B", "A", "B", "A", "A", "A", "B", "B", "A", "A"]);

    RecordBatch::try_new(schema, vec![Arc::new(int_values), Arc::new(str_values)]).unwrap()
}

#[tokio::test]
async fn test_delta_etl() -> Result<(), deltalake::DeltaTableError> {
    let ops = DeltaOps::new_in_memory();

    let table = ops
        .create()
        .with_columns(get_table_columns())
        .with_table_name("my_table")
        .with_comment("A table to show how delta-rs works")
        .await?;

    assert_eq!(table.version(), 0);

    let batch = get_table_batches();
    let table = DeltaOps(table).write(vec![batch.clone()]).await?;

    assert_eq!(table.version(), 1);

    let (_table, stream) = DeltaOps(table).load().await?;
    let data: Vec<RecordBatch> = collect_sendable_stream(stream).await?;

    println!("{:?}", data);

    Ok(())
}