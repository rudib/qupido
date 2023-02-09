use datafusion::{prelude::*, error::DataFusionError};
use qupido::{container::Container, node::Node, id, pipeline::Pipeline};

#[tokio::test]
async fn test_oscars_pipeline() -> Result<(), DataFusionError> {
    let ctx = SessionContext::new();
    let df = ctx.read_csv("tests/data/the_oscar_award.csv", CsvReadOptions::new()).await?;
    
    let container = {
        let mut container = Container::new();
        container.insert("oscar_awards", df).unwrap();
        container
    };

    let node_categories = Node::<DataFrame>::new(id("oscar_awards"), id("oscar_categories"), |ctx| {
        let df = ctx.inputs.get("oscar_awards")?;
        let df_categories = df.clone()
                              .select_columns(&["category"]).unwrap()
                              .distinct().unwrap()
                              .sort(vec![col("category").sort(true, false)]).unwrap();
        
        let mut c = Container::new();
        c.insert("oscar_categories", df_categories).unwrap();
        Ok(c)
    });

    let node_categories_clean = Node::<DataFrame>::new(id("oscar_categories"), id("oscar_categories_clean"), |ctx| {
        let df = ctx.inputs.get("oscar_categories")?;
        let df_clean = df.clone()
            .select(vec![regexp_replace(vec![col("category"), lit("\\(.*\\)"), lit("")]).alias("category")]).unwrap()
            .select(vec![upper(trim(col("category"))).alias("clean_category")]).unwrap()
            .distinct().unwrap()
            .sort(vec![col("clean_category").sort(true, false)]).unwrap();

        let mut c = Container::new();
        c.insert("oscar_categories_clean", df_clean).unwrap();
        Ok(c)
    });

    let pipeline = Pipeline::from_nodes(&[node_categories, node_categories_clean]).unwrap();
    let resulting_container = pipeline.run(&container).unwrap();

    let categories = resulting_container.get("oscar_categories").unwrap();
    categories.clone().show().await?;

    let categories_clean = resulting_container.get("oscar_categories_clean").unwrap();
    categories_clean.clone().show().await?;
    

    Ok(())
}