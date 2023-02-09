use datafusion::{prelude::*, error::DataFusionError};


#[tokio::test]
async fn test_delta_etl() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/the_oscar_award.csv", CsvReadOptions::new()).await?;
    println!("schema: {:#?}", df.schema());
    let df = df
               .filter(col("winner").eq(lit(true)))?
               .filter(col("category").eq(lit("BEST PICTURE")))?
               .sort(vec![col("year_ceremony").sort(true, false)])?;
    
    df.show().await?;

    Ok(())
}