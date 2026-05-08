use std::string::ToString;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_rusqlite::rusqlite::Connection;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
enum MessageType {
    Chat,
    Broadcast,
    Alert,
    Kick,
}

#[derive(Debug, Clone)]
struct Message {
    message_type: MessageType,
    sender_uuid: String,
    sender_username: String,
    message: String,
}

impl Message {
    fn new(
        message_type: MessageType,
        sender_uuid: String,
        sender_username: String,
        message: String,
    ) -> Message {
        Message {
            message_type,
            sender_uuid,
            sender_username,
            message,
        }
    }
}

enum DatabaseRequest {
    RegisterUser {
        account: Account,
    },
    MessageAddition {
        requester_uuid: String,
        message: Message,
    },
    GetPreviousMessages {
        resp: oneshot::Sender<Vec<Message>>,
    },
    AccountRequest {
        username: String,
        resp: oneshot::Sender<Option<Account>>,
    },
}
impl Account {
    fn new(uuid: String, username: String, ip_addr: String, password: String) -> Account {
        Account {
            uuid,
            username,
            ip_addr,
            password,
        }
    }
}

#[derive(Debug, Clone)]
struct Account {
    uuid: String,
    username: String,
    ip_addr: String,
    password: String,
}

#[tokio::main]
async fn main() {
    let server_ip = "0.0.0.0:16831";
    let listener = TcpListener::bind(server_ip)
        .await
        .expect("Failed to bind listener");
    println!("TCPListener:{} success", server_ip);

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
    let (msg_tx, _msg_rx) = broadcast::channel::<(String, Message)>(10);

    let (db_tx, mut db_rx) = mpsc::channel::<DatabaseRequest>(10);
    {
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
    {
        let msg_tx_clone = msg_tx.clone();
        tokio::spawn(async move {
            let mut input_buf_reader = BufReader::new(tokio::io::stdin());

            let mut readed_input_line = String::new();

            'reading_loop: loop {
                input_buf_reader
                    .read_line(&mut readed_input_line)
                    .await
                    .unwrap();
                if !readed_input_line.contains(" ") {
                    print_help_message().await;
                    readed_input_line.clear();
                    continue 'reading_loop;
                }
                let input_line = readed_input_line.trim().to_string();
                let command_name = input_line.split(" ").next().unwrap();
                let rest_of_command = input_line.split_at(command_name.len() + 1).1;
                readed_input_line.clear();
                if command_name.to_lowercase().eq("alert") {
                    msg_tx_clone
                        .send((
                            String::new(),
                            Message::new(
                                MessageType::Alert,
                                "error".to_string(),
                                "error".to_string(),
                                rest_of_command.to_string(),
                            ),
                        ))
                        .unwrap();
                    continue 'reading_loop;
                }
                if command_name.to_lowercase().eq("help") {
                    print_help_message().await;
                    continue 'reading_loop;
                }
                if command_name.to_lowercase().eq("kick") {
                    println!("rame");
                    let mut reason;
                    let mut username_or_ip;
                    if rest_of_command.contains("|") {
                        let mut args = rest_of_command.split("|");
                        username_or_ip = args.next().unwrap().to_string();
                        username_or_ip = username_or_ip
                            .split_at(username_or_ip.len() - 1)
                            .0
                            .to_string();
                        reason = args.next().unwrap().to_string();
                        reason = reason.split_at(1).1.to_string();
                    } else {
                        reason = String::from("No reason provided.");
                        username_or_ip = rest_of_command.to_string();
                    }
                    msg_tx_clone
                        .send((
                            String::new(),
                            Message::new(
                                MessageType::Kick,
                                "error".to_string(),
                                username_or_ip,
                                reason,
                            ),
                        ))
                        .unwrap();
                    continue 'reading_loop;
                }
                println!("Unable to find command. Type /help for list of server commands.");
            }
        });
    }
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
                    let mut account =
                        authenticate_user(&mut buf_reader, &socket_address, &db_tx, &msg_tx).await;
                    loop {
                        tokio::select! {
                            result = handle_socket_input(
                                &mut buf_reader,
                                &mut line,
                                &socket_address,
                                &mut account,
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
                                        &mut buf_reader,
                                        &socket_address,
                                        &account,
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

//this is message separator
const M_S: &str = "------------------------------\n";

async fn write_string(reader: &mut BufReader<TcpStream>, message: String) {
    reader.write_all(message.as_bytes()).await.unwrap();
    reader.flush().await.unwrap();
}

async fn write_str(reader: &mut BufReader<TcpStream>, message: &str) {
    reader.write_all(message.as_bytes()).await.unwrap();
    reader.flush().await.unwrap();
}

async fn print_help_message() {
    print!("{M_S}");
    println!("Chat Commands:");
    println!(" /alert <message> - Broadcast message to all connected users.");
    println!(" /kick <username>/<ip> | <reason> - Kick connected user from the server.");
    println!(" / - \n");
    print!("{M_S}");
}
async fn handle_socket_input(
    buf_reader: &mut BufReader<TcpStream>,
    line: &mut String,
    socket_address: &String,
    account: &mut Option<Account>,
    db_tx: &mpsc::Sender<DatabaseRequest>,
    msg_tx: &broadcast::Sender<(String, Message)>,
) -> bool {
    match buf_reader.read_line(line).await {
        Ok(bytes_read) => {
            if bytes_read == 0 {
                println!("{socket_address} disconnected!");
                return false;
            }

            if line.trim().is_empty() {
                line.clear();
                return true;
            }

            let acc = account.clone().unwrap();

            msg_tx
                .send((
                    socket_address.clone(),
                    Message::new(
                        MessageType::Chat,
                        acc.uuid.clone(),
                        acc.username.clone(),
                        line.trim().to_string(),
                    ),
                ))
                .unwrap();

            db_tx
                .send(DatabaseRequest::MessageAddition {
                    requester_uuid: acc.uuid.clone(),
                    message: Message::new(
                        MessageType::Chat,
                        acc.uuid,
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
async fn authenticate_user(
    buf_reader: &mut BufReader<TcpStream>,
    socket_address: &String,
    db_tx: &mpsc::Sender<DatabaseRequest>,
    msg_tx: &broadcast::Sender<(String, Message)>,
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

    write_str(buf_reader, "You can start typing.\n").await;

    msg_tx
        .send((
            socket_address.clone(),
            Message::new(
                MessageType::Broadcast,
                account.uuid.clone(),
                account.username.clone(),
                format!("{} joined the chat!\n", account.username),
            ),
        ))
        .unwrap();

    let (resp_tx, resp_rx) = oneshot::channel();

    db_tx
        .send(DatabaseRequest::GetPreviousMessages { resp: resp_tx })
        .await
        .unwrap();

    if let Ok(messages) = resp_rx.await {
        handle_previous_messages(buf_reader, messages).await;
    }

    Some(account)
}
async fn handle_broadcast_message(
    buf_reader: &mut BufReader<TcpStream>,
    socket_address: &String,
    account: &Option<Account>,
    msg_tx: &broadcast::Sender<(String, Message)>,
    sender_ip: String,
    message: Message,
) -> bool {
    match message.message_type {
        MessageType::Chat => {
            if message.sender_uuid == account.clone().unwrap().uuid && sender_ip == *socket_address
            {
                return true;
            }

            let formatted = format!("{}: {}\n", message.sender_username, message.message);

            write_string(buf_reader, formatted).await;
        }

        MessageType::Broadcast => {
            write_string(buf_reader, message.message).await;
        }

        MessageType::Alert => {
            write_string(
                buf_reader,
                format!("{M_S}Server message: {}\n{M_S}", message.message),
            )
            .await;
        }

        MessageType::Kick => {
            let acc = account.clone().unwrap();

            if message.sender_username != acc.username && message.sender_username != acc.ip_addr {
                return true;
            }

            write_string(
                buf_reader,
                format!("{M_S}You are kicked!\nReason: {}\n{M_S}", message.message),
            )
            .await;

            buf_reader.shutdown().await.unwrap();

            msg_tx
                .send((
                    socket_address.clone(),
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
async fn handle_previous_messages(buf_reader: &mut BufReader<TcpStream>, messages: Vec<Message>) {
    let mut all = String::new();

    for message in messages {
        all.push_str(format!("{}: {}\n", message.sender_username, message.message).as_str());
    }

    write_string(buf_reader, all).await;
}
