// mod lib;
mod constants;
mod types;
mod buffer;
mod error;
mod record;

use axum::{
    routing::get,
    Router,
};
use rust_api::{create_database, open_database, read_page};

use std::{cell::{Cell, RefCell}, path::Path, rc::Rc, io::Error};

use crate::types::PageHeader;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // let app = Router::new()
    //     .route("/", get(|| async { "Hello World!" }))
    //     .route("/test", get(|| async { "tes world" }));

    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    // axum::serve(listener, app).await.unwrap();
    let path = Path::new("./data/test.db");
    // let database = create_database(path)?;
    let (file_header, file) = open_database(path)?;

    print!("The file header struct is: {:#?}", file_header);
    // println!("got passed here");
    let page_1_bytes = read_page(&file, 1)?;
    let page_2_bytes = read_page(&file, 2)?;


    let table_header = PageHeader::from_bytes(&page_1_bytes);
    let column_header = PageHeader::from_bytes(&page_2_bytes);

    print!("Table header is: {:#?}", table_header);
    print!("Table column is: {:#?}", column_header);






















    // let a: Rc<Cell<u8>> = Rc::new(Cell::new(5));

    // a.set(1);

    // let b = a.clone();

    // b.set(3);

    // println!("{}", a.get());

    // println!("{:?}", b.get());

    // println!("a points to: {:p}", Rc::as_ptr(&a));
    // println!("b points to: {:p}", Rc::as_ptr(&b));

    Ok(())
}
