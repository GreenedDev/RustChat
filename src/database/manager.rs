use crate::{database::requests::DatabaseRequest, server::types::Account, Message, MessageType};
use tokio::sync::mpsc;
use tokio_rusqlite::rusqlite::Connection;
pub async fn spawn_db_manager(conn: Connection, mut db_rx: mpsc::Receiver<DatabaseRequest>) {
    tokio::spawn(async move {
        loop {
            let request = db_rx.recv().await.unwrap();
            match request {
                DatabaseRequest::RegisterUser { account } => {
                    conn.execute(
                        "INSERT INTO accounts (uuid, username, password) VALUES (?1, ?2, ?3)",
                        (account.uuid, account.username, account.password),
                    )
                    .unwrap();
                    continue;
                }
                DatabaseRequest::MessageAddition {
                    requester_uuid,
                    message,
                } => {
                    conn.execute(
                        "INSERT INTO messages (sender, message) VALUES (?1, ?2)",
                        (requester_uuid, message.message),
                    )
                    .unwrap();
                    continue;
                }
                DatabaseRequest::GetPreviousMessages { resp } => {
                    let mut stmt = conn
                        .prepare(
                            "SELECT m.sender, a.username, m.message 
                     FROM messages m 
                     JOIN accounts a ON m.sender = a.uuid 
                     LIMIT 50",
                        )
                        .unwrap();

                    let msgs = stmt
                        .query_map([], |row| {
                            Ok(Message {
                                message_type: MessageType::Chat,
                                sender_uuid: row.get(0)?,
                                sender_username: row.get(1)?,
                                message: row.get(2)?,
                            })
                        })
                        .unwrap()
                        .filter_map(|m| m.ok())
                        .collect();

                    let _ = resp.send(msgs);
                }
                DatabaseRequest::AccountRequest { username, resp } => {
                    let result = conn
                        .prepare(
                            "SELECT uuid, username, password FROM accounts WHERE username = ?1",
                        )
                        .and_then(|mut stmt| {
                            stmt.query_row([&username], |row| {
                                Ok(Account {
                                    uuid: row.get(0)?,
                                    username: row.get(1)?,
                                    password: row.get(2)?,
                                    ip_addr: "0.0.0.0".to_string(), // Placeholder or from DB
                                })
                            })
                        });

                    // Send the Result<Option<Account>, Error> back to the requester

                    let _ = resp.send(result.ok());
                }
            }
        }
    });
}
pub async fn connect_to_db_and_setup() -> Connection {
    let conn = Connection::open("data.db").unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS messages (
            sender TEXT NOT NULL,
            message TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS accounts (
            uuid TEXT NOT NULL PRIMARY KEY,
            username TEXT NOT NULL,
            password TEXT NOT NULL
        )",
        (),
    )
    .unwrap();
    conn
}
