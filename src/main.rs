use std::string::ToString;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc};

pub mod cli;
pub mod database;
pub mod server;

use crate::cli::manager::spawn_cli_manager;
use crate::database::manager::{connect_to_db_and_setup, spawn_db_manager};
use crate::database::requests::DatabaseRequest;
use crate::server::manager::accept_connections;
use crate::server::types::{Client, Message, MessageType};

async fn write_str(buf_reader: &mut BufReader<TcpStream>, message: &str) {
    buf_reader.write_all(message.as_bytes()).await.unwrap();
    buf_reader.flush().await.unwrap();
}
#[tokio::main]
async fn main() {
    let server_ip = "0.0.0.0:16831";
    let listener = TcpListener::bind(server_ip)
        .await
        .expect("Failed to bind listener");
    println!("TCPListener:{} success", server_ip);

    let conn = connect_to_db_and_setup().await;
    let (msg_tx, _msg_rx) = broadcast::channel::<(String, Message)>(10);

    let (db_tx, db_rx) = mpsc::channel::<DatabaseRequest>(10);
    spawn_db_manager(conn, db_rx).await;
    spawn_cli_manager(msg_tx.clone()).await;
    accept_connections(listener, msg_tx.clone(), db_tx.clone()).await;
}

async fn handle_socket_input(
    client: &mut Client,
    line: &mut String,
    db_tx: &mpsc::Sender<DatabaseRequest>,
    msg_tx: &broadcast::Sender<(String, Message)>,
) -> bool {
    match client.stream.read_line(line).await {
        Ok(bytes_read) => {
            if bytes_read == 0 {
                println!("{} disconnected!", client.account.ip_addr);
                return false;
            }

            if line.trim().is_empty() {
                line.clear();
                return true;
            }

            msg_tx
                .send((
                    client.account.ip_addr.clone(),
                    Message::new(
                        MessageType::Chat,
                        client.account.uuid.clone(),
                        client.account.username.clone(),
                        line.trim().to_string(),
                    ),
                ))
                .unwrap();

            db_tx
                .send(DatabaseRequest::MessageAddition {
                    requester_uuid: client.account.uuid.clone(),
                    message: Message::new(
                        MessageType::Chat,
                        client.account.uuid.clone(),
                        "error".to_string(),
                        line.trim().to_string(),
                    ),
                })
                .await
                .unwrap();

            line.clear();
            true
        }
        Err(_) => true,
    }
}
