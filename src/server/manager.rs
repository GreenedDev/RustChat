use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{
        broadcast::{self, Sender},
        mpsc,
    },
};

use crate::{
    cli::manager::M_S,
    database::requests::DatabaseRequest,
    handle_socket_input,
    server::{
        auth::{authenticate_user, post_login},
        types::MessageType,
    },
    write_str, Client, Message,
};

pub async fn accept_connections(
    listener: TcpListener,
    msg_tx: Sender<(String, Message)>,
    db_tx: mpsc::Sender<DatabaseRequest>,
) {
    loop {
        match listener.accept().await {
            Ok((socket, socket_address)) => {
                let socket_address = socket_address.to_string();
                println!("{socket_address} connected!");
                let msg_tx = msg_tx.clone();
                let mut msg_rx = msg_tx.subscribe();

                let db_tx = db_tx.clone();

                tokio::spawn(async move {
                    let mut buf_reader = BufReader::new(socket);

                    let mut line = String::new();
                    let Some(account) =
                        authenticate_user(&mut buf_reader, &socket_address, &db_tx).await
                    else {
                        write_str(&mut buf_reader, "invalid account.").await;
                        return;
                    };
                    let mut client = Client {
                        stream: buf_reader,
                        account,
                    };
                    post_login(&mut client, &db_tx, &msg_tx).await;
                    loop {
                        tokio::select! {
                            result = handle_socket_input(
                                &mut client,
                                &mut line,
                                &db_tx,
                                &msg_tx,
                            ) => {
                                if !result {
                                    break;
                                }
                            }

                            result = msg_rx.recv() => {
                                if let Ok((sender_ip, message)) = result {
                                    let keep_alive = handle_broadcast_message(
                                        &mut client,
                                        &msg_tx,
                                        sender_ip,
                                        message,
                                    ).await;

                                    if !keep_alive {
                                        break;
                                    }
                                }
                            }

                        }
                    }
                });
            }
            Err(..) => {
                panic!();
            }
        }
    }
}
async fn handle_broadcast_message(
    client: &mut Client,
    msg_tx: &broadcast::Sender<(String, Message)>,
    sender_ip: String,
    message: Message,
) -> bool {
    let acc = client.account.clone();
    match message.message_type {
        MessageType::Chat => {
            if message.sender_uuid == acc.uuid && sender_ip == *client.account.ip_addr {
                return true;
            }

            let formatted = format!("{}: {}\n", message.sender_username, message.message);
            client.write_string(formatted).await;
        }

        MessageType::Broadcast => {
            client.write_string(message.message).await;
        }

        MessageType::Alert => {
            client
                .write_string(format!("{M_S}Server message: {}\n{M_S}", message.message))
                .await;
        }

        MessageType::Kick => {
            if message.sender_username != acc.username && message.sender_username != acc.ip_addr {
                return true;
            }

            client
                .write_string(format!(
                    "{M_S}You are kicked!\nReason: {}\n{M_S}",
                    message.message
                ))
                .await;

            client.stream.shutdown().await.unwrap();

            msg_tx
                .send((
                    client.account.ip_addr.clone(),
                    Message::new(
                        MessageType::Broadcast,
                        "error".to_string(),
                        "error".to_string(),
                        format!("{} has been kicked.\n", acc.username),
                    ),
                ))
                .unwrap();

            return false;
        }
    }

    true
}
