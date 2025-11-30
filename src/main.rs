mod constants;
mod types;

use axum::{
    routing::get,
    Router,
};

use std::{cell::{Cell, RefCell}, rc::Rc};

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(|| async { "Hello World!" }))
        .route("/test", get(|| async { "tes world" }));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();

    axum::serve(listener, app).await.unwrap();

    // let a: Rc<Cell<u8>> = Rc::new(Cell::new(5));

    // a.set(1);

    // let b = a.clone();

    // b.set(3);

    // println!("{}", a.get());

    // println!("{:?}", b.get());

    // println!("a points to: {:p}", Rc::as_ptr(&a));
    // println!("b points to: {:p}", Rc::as_ptr(&b));

    // Ok(())
}
