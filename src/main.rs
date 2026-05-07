use std::string::ToString;
use std::time::SystemTime;

use rusqlite::Connection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
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
#[derive(Debug, Clone)]
struct PasswordCheckResponseValues {
    requester: String,        //username of requester
    account: Option<Account>, //response account
    send_date: SystemTime,
}
#[derive(Debug, Clone)]
struct UsernameCheckResponseValues {
    requester: String,        //username of requester
    account: Option<Account>, //response account
    send_date: SystemTime,
}
#[derive(Debug, Clone)]
enum DatabaseResponse {
    PasswordCheck(PasswordCheckResponseValues),
    UsernameCheck(UsernameCheckResponseValues),
}

#[derive(Debug, Clone)]
struct RegisterUserRequestValues {
    account: Account,
    _send_date: SystemTime,
}
#[derive(Debug, Clone)]
struct MessageAdditionRequestValues {
    requester: String,
    message: Message,
    _send_date: SystemTime,
}

#[derive(Debug, Clone)]
struct GetPreviousMessagesRequestValues {
    requester: String,
    _send_date: SystemTime,
}
#[derive(Debug, Clone)]
struct UsernameCheckRequestValues {
    requester: String, //username of requester
    send_date: SystemTime,
}

#[derive(Debug, Clone)]
struct PasswordCheckRequestValues {
    requester: String, //username of requester
    send_date: SystemTime,
}
#[derive(Debug, Clone)]
enum DatabaseRequest {
    RegisterUser(RegisterUserRequestValues),
    MessageAddition(MessageAdditionRequestValues),
    GetPreviousMessages(GetPreviousMessagesRequestValues),
    PasswordCheck(PasswordCheckRequestValues),
    UsernameCheck(UsernameCheckRequestValues),
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
    //sender_ip, msg
    let (msg_tx, _msg_rx) = broadcast::channel::<(String, Message)>(10);
    let (db_tx, _db_rx) = broadcast::channel::<DatabaseRequest>(10);
    //username of all message requester, vec(Message)
    let (vec_of_messages_to_user_tx, _vec_of_messages_to_user_rx) =
        broadcast::channel::<(String, Vec<Message>)>(10);
    let (db_response_tx, _db_response_rx) = broadcast::channel::<DatabaseResponse>(10);
    {
        let db_tx = db_tx.clone();
        let mut db_rx = db_tx.subscribe();

        let vec_of_messages_to_user_tx = vec_of_messages_to_user_tx.clone();
        let db_response_tx = db_response_tx.clone();

        tokio::spawn(async move {
            loop {
                let request = db_rx.recv().await.unwrap();
                match request {
                    DatabaseRequest::RegisterUser(request) => {
                        conn.execute(
                            "INSERT INTO accounts (uuid, username, password) VALUES (?1, ?2, ?3)",
                            (
                                request.account.uuid,
                                request.account.username,
                                request.account.password,
                            ),
                        )
                        .unwrap();
                        continue;
                    }
                    DatabaseRequest::MessageAddition(request) => {
                        conn.execute(
                            "INSERT INTO messages (sender, message) VALUES (?1, ?2)",
                            (request.requester, request.message.message),
                        )
                        .unwrap();
                        continue;
                    }
                    DatabaseRequest::GetPreviousMessages(request) => {
                        let mut stmt = conn
                            .prepare("SELECT sender, message FROM messages")
                            .unwrap();

                        let message_iter = stmt
                            .query_map([], |row| {
                                let uuid: String = row.get(0)?;
                                let mut stmt = conn
                                    .prepare("SELECT uuid, username FROM accounts WHERE uuid = ?")
                                    .unwrap();

                                let mut account_iter = stmt
                                    .query_map([uuid.clone()], |account_row| {
                                        Ok(Account {
                                            uuid: account_row.get(0)?,
                                            username: account_row.get(1)?,
                                            ip_addr: "error".to_string(), // you can improve this
                                            password: "error".to_string(),
                                        })
                                    })
                                    .unwrap();
                                let account = account_iter.next().unwrap().unwrap();
                                Ok(Message {
                                    message_type: MessageType::Chat,
                                    sender_uuid: uuid,
                                    sender_username: account.username,
                                    message: row.get(1)?,
                                })
                            })
                            .unwrap();
                        let mut result = Vec::new();
                        for message in message_iter {
                            result.push(message.unwrap());
                        }
                        vec_of_messages_to_user_tx
                            .send((request.requester, result))
                            .unwrap();
                    }
                    DatabaseRequest::PasswordCheck(request) => {
                        let mut stmt = conn
                            .prepare("SELECT uuid, username, password FROM accounts")
                            .unwrap();

                        let accounts_iter = stmt
                            .query_map([], |row| {
                                Ok(Account {
                                    uuid: row.get(0)?,
                                    username: row.get(1)?,
                                    ip_addr: "error".to_string(),
                                    password: row.get(2)?,
                                })
                            })
                            .unwrap();
                        let mut result = None;
                        for account in accounts_iter {
                            let account_unwrapped = account.unwrap();
                            if account_unwrapped.username != request.requester {
                                continue;
                            }
                            result = Some(account_unwrapped);
                        }
                        db_response_tx
                            .send(DatabaseResponse::PasswordCheck(
                                PasswordCheckResponseValues {
                                    requester: request.requester,
                                    account: result,
                                    send_date: request.send_date,
                                },
                            ))
                            .unwrap();
                    }
                    DatabaseRequest::UsernameCheck(request) => {
                        let mut stmt = conn
                            .prepare("SELECT uuid, username, password FROM accounts")
                            .unwrap();

                        let accounts_iter = stmt
                            .query_map([], |row| {
                                Ok(Account {
                                    uuid: row.get(0)?,
                                    username: row.get(1)?,
                                    ip_addr: "error".to_string(),
                                    password: row.get(2)?,
                                })
                            })
                            .unwrap();
                        let mut result = None;
                        for account in accounts_iter {
                            let account_unwrapped = account.unwrap();
                            if account_unwrapped.username != request.requester {
                                continue;
                            }
                            result = Some(account_unwrapped);
                            break;
                        }
                        db_response_tx
                            .send(DatabaseResponse::UsernameCheck(
                                UsernameCheckResponseValues {
                                    requester: request.requester,
                                    account: result,
                                    send_date: request.send_date,
                                },
                            ))
                            .unwrap();
                    }
                }
            }
        });
    }

    let (new_connections_tx, _new_connections_rx) = broadcast::channel::<String>(10);
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
        let socket: TcpStream;
        let socket_address: String;
        let new_connections_tx = new_connections_tx.clone();
        let mut new_connections_rx = new_connections_tx.subscribe();

        let vec_of_messages_to_user_tx = vec_of_messages_to_user_tx.clone();
        let mut vec_of_messages_to_user_rx = vec_of_messages_to_user_tx.subscribe();

        let db_response_tx = db_response_tx.clone();
        let mut db_response_rx = db_response_tx.subscribe();

        match listener.accept().await {
            Ok((accepted_socket, accepted_address)) => {
                socket = accepted_socket;
                socket_address = accepted_address.to_string();
                println!("{socket_address} connected!");
                new_connections_tx.send(socket_address.clone()).unwrap();
            }
            Err(..) => {
                panic!();
            }
        }

        let msg_tx = msg_tx.clone();
        let mut msg_rx = msg_tx.subscribe();

        let db_tx = db_tx.clone();

        tokio::spawn(async move {
            let mut buf_reader = BufReader::new(socket);

            let mut line = String::new();
            'loop_of_this_connection: loop {
                match new_connections_rx.recv().await {
                    Ok(addr) => {
                        if addr != socket_address {
                            continue;
                        }
                        write_str(&mut buf_reader, "Type R/L to Register/Login.\n").await;
                        let mut r_or_l = String::new();
                        if buf_reader.read_line(&mut r_or_l).await.unwrap() == 0 {
                            continue 'loop_of_this_connection;
                        }
                        r_or_l = r_or_l.trim().to_lowercase();
                        if r_or_l != "r" && r_or_l != "l" {
                            write_str(&mut buf_reader, "Please enter r/l.\n").await;
                            buf_reader.shutdown().await.unwrap();
                            continue 'loop_of_this_connection;
                        }
                        write_str(&mut buf_reader, "Enter your username.\n").await;
                        let mut is_about_to_type_username = true;
                        let mut username = String::new();
                        let mut account = None;
                        loop {
                            tokio::select! {
                                result = handle_socket_input(
                                    &mut buf_reader,
                                    &mut line,
                                    &socket_address,
                                    &mut username,
                                    &mut account,
                                    &mut is_about_to_type_username,
                                    &r_or_l,
                                    &db_tx,
                                    &mut db_response_rx,
                                    &msg_tx,
                                ) => {
                                    if !result {
                                        break 'loop_of_this_connection;
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
                                            break 'loop_of_this_connection;
                                        }
                                    }
                                }

                                result = vec_of_messages_to_user_rx.recv() => {
                                    if let Ok((target_username, messages)) = result {
                                        handle_previous_messages(
                                            &mut buf_reader,
                                            &username,
                                            target_username,
                                            messages,
                                        ).await;
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        });
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
    username: &mut String,
    account: &mut Option<Account>,
    is_about_to_type_username: &mut bool,
    r_or_l: &String,
    db_tx: &broadcast::Sender<DatabaseRequest>,
    db_response_rx: &mut broadcast::Receiver<DatabaseResponse>,
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

            if *is_about_to_type_username {
                *username = line.trim().to_string();

                write_str(buf_reader, "Enter your password.\n").await;

                let mut pass = String::new();

                if buf_reader.read_line(&mut pass).await.unwrap() == 0 {
                    return true;
                }

                pass = pass.trim().to_string();

                if r_or_l == "l" {
                    let now = SystemTime::now();

                    db_tx
                        .send(DatabaseRequest::PasswordCheck(PasswordCheckRequestValues {
                            requester: username.clone(),
                            send_date: now,
                        }))
                        .unwrap();

                    loop {
                        let response = db_response_rx.recv().await.unwrap();

                        let DatabaseResponse::PasswordCheck(response) = response else {
                            continue;
                        };

                        if response.requester != *username || response.send_date != now {
                            continue;
                        }

                        let Some(response_account) = response.account else {
                            write_str(buf_reader, "This account doesn't exist.\n").await;
                            return false;
                        };

                        if pass != response_account.password {
                            write_str(buf_reader, "Password incorrect.\n").await;
                            return false;
                        }

                        *account = Some(Account::new(
                            response_account.uuid,
                            response_account.username,
                            socket_address.clone(),
                            response_account.password,
                        ));

                        write_str(buf_reader, "Login success.\n").await;
                        break;
                    }
                } else {
                    let now = SystemTime::now();

                    db_tx
                        .send(DatabaseRequest::UsernameCheck(UsernameCheckRequestValues {
                            requester: username.clone(),
                            send_date: now,
                        }))
                        .unwrap();

                    loop {
                        let response = db_response_rx.recv().await.unwrap();

                        let DatabaseResponse::UsernameCheck(response) = response else {
                            continue;
                        };

                        if response.requester != *username || response.send_date != now {
                            continue;
                        }

                        if response.account.is_some() {
                            write_str(buf_reader, "Username taken.\n").await;
                            return false;
                        }

                        let uuid = Uuid::new_v4().to_string();

                        *account = Some(Account::new(
                            uuid,
                            username.clone(),
                            socket_address.clone(),
                            pass.clone(),
                        ));

                        db_tx
                            .send(DatabaseRequest::RegisterUser(RegisterUserRequestValues {
                                account: account.clone().unwrap(),
                                _send_date: now,
                            }))
                            .unwrap();

                        write_str(buf_reader, "Registered successfully.\n").await;

                        break;
                    }
                }

                write_str(buf_reader, "You can start typing.\n").await;

                *is_about_to_type_username = false;

                msg_tx
                    .send((
                        socket_address.clone(),
                        Message::new(
                            MessageType::Broadcast,
                            account.clone().unwrap().uuid.clone(),
                            username.clone(),
                            format!("{} Joined the chat!\n", username),
                        ),
                    ))
                    .unwrap();
                db_tx
                    .send(DatabaseRequest::GetPreviousMessages(
                        GetPreviousMessagesRequestValues {
                            requester: username.clone(),
                            _send_date: SystemTime::now(),
                        },
                    ))
                    .unwrap();
            } else {
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
                    .send(DatabaseRequest::MessageAddition(
                        MessageAdditionRequestValues {
                            requester: acc.uuid.clone(),
                            message: Message::new(
                                MessageType::Chat,
                                acc.uuid,
                                "error".to_string(),
                                line.trim().to_string(),
                            ),
                            _send_date: SystemTime::now(),
                        },
                    ))
                    .unwrap();
            }

            line.clear();
            true
        }
        Err(_) => true,
    }
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
async fn handle_previous_messages(
    buf_reader: &mut BufReader<TcpStream>,
    username: &String,
    target_username: String,
    messages: Vec<Message>,
) {
    if target_username != *username {
        return;
    }

    let mut all = String::new();

    for message in messages {
        all.push_str(format!("{}: {}\n", message.sender_username, message.message).as_str());
    }

    write_string(buf_reader, all).await;
}
