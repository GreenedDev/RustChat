use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{broadcast, mpsc, oneshot},
};
use uuid::Uuid;

use crate::{
    database::requests::DatabaseRequest, server::types::Account, write_str, Client, Message,
    MessageType,
};

pub async fn authenticate_user(
    buf_reader: &mut BufReader<TcpStream>,
    socket_address: &String,
    db_tx: &mpsc::Sender<DatabaseRequest>,
) -> Option<Account> {
    write_str(buf_reader, "Type R/L to Register/Login.\n").await;

    let mut r_or_l = String::new();

    if buf_reader.read_line(&mut r_or_l).await.unwrap() == 0 {
        return None;
    }

    let r_or_l = r_or_l.trim().to_lowercase();

    if r_or_l != "r" && r_or_l != "l" {
        write_str(buf_reader, "Please enter r/l.\n").await;
        let _ = buf_reader.shutdown().await;
        return None;
    }

    write_str(buf_reader, "Enter your username.\n").await;

    let mut username = String::new();

    if buf_reader.read_line(&mut username).await.unwrap() == 0 {
        return None;
    }

    let username = username.trim().to_string();

    write_str(buf_reader, "Enter your password.\n").await;

    let mut password = String::new();

    if buf_reader.read_line(&mut password).await.unwrap() == 0 {
        return None;
    }

    let password = password.trim().to_string();

    let (resp_tx, resp_rx) = oneshot::channel();

    db_tx
        .send(DatabaseRequest::AccountRequest {
            username: username.clone(),
            resp: resp_tx,
        })
        .await
        .unwrap();

    let account = match resp_rx.await {
        Ok(Some(acc)) => acc,
        Ok(None) => {
            if r_or_l == "l" {
                write_str(buf_reader, "This account doesn't exist.\n").await;
                return None;
            }

            let uuid = Uuid::new_v4().to_string();

            let new_account = Account::new(
                uuid,
                username.clone(),
                socket_address.clone(),
                password.clone(),
            );

            db_tx
                .send(DatabaseRequest::RegisterUser {
                    account: new_account.clone(),
                })
                .await
                .unwrap();

            write_str(buf_reader, "Registered successfully.\n").await;

            new_account
        }
        Err(_) => return None,
    };

    if r_or_l == "l" && account.password != password {
        write_str(buf_reader, "Password incorrect.\n").await;
        return None;
    }

    Some(account)
}
pub async fn post_login(
    client: &mut Client,

    db_tx: &mpsc::Sender<DatabaseRequest>,

    msg_tx: &broadcast::Sender<(String, Message)>,
) {
    client.write_str("You can start typing.\n").await;

    msg_tx
        .send((
            client.account.ip_addr.clone(),
            Message::new(
                MessageType::Broadcast,
                client.account.uuid.clone(),
                client.account.username.clone(),
                format!("{} joined the chat!\n", client.account.username),
            ),
        ))
        .unwrap();

    let (resp_tx, resp_rx) = oneshot::channel();

    db_tx
        .send(DatabaseRequest::GetPreviousMessages { resp: resp_tx })
        .await
        .unwrap();

    if let Ok(messages) = resp_rx.await {
        handle_previous_messages(client, messages).await;
    }
}

async fn handle_previous_messages(client: &mut Client, messages: Vec<Message>) {
    let mut all = String::new();

    for message in messages {
        all.push_str(format!("{}: {}\n", message.sender_username, message.message).as_str());
    }

    client.write_string(all).await;
}
