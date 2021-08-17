use chatnet::{Result, Server};

#[tokio::main]
async fn main() -> Result {
    let server = Server::new("127.0.0.1:5000").await?;
    server.run().await?;
    Ok(())
}
