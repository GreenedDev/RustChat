use std::string::ToString;
use std::time::SystemTime;

use rusqlite::Connection;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct Message {
    message_type: String,
    sender_uuid: String,
    sender_username: String,
    message: String,
}

impl Message {
    fn new(
        message_type: String,
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
    fn blank() -> Message {
        Message {
            message_type: "error".to_string(),
            sender_uuid: "error".to_string(),
            sender_username: "error".to_string(),
            message: "error".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
struct DatabaseResponse {
    response_type: String,
    requester: String,     //username of
    account: Account,      //response
    send_date: SystemTime, //the time request was sent
}

#[derive(Debug, Clone)]
struct DatabaseRequest {
    request_type: String,  //response type
    requester: String,     //username
    account: Account,      //response
    message: Message,      //message
    send_date: SystemTime, //the time request was sent
}

impl DatabaseRequest {
    fn new(
        request_type: String,
        requester: String,
        account: Account,
        message: Message,
        send_date: SystemTime,
    ) -> DatabaseRequest {
        DatabaseRequest {
            request_type,
            requester, //username
            account,   //response
            message,
            send_date, //the request send date.
        }
    }
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
    fn blank() -> Account {
        Account {
            uuid: "error".to_string(),
            username: "error".to_string(),
            ip_addr: "error".to_string(),
            password: "error".to_string(),
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
                if request.request_type == "register_user" {
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
                } else if request.request_type == "messages" {
                    conn.execute(
                        "INSERT INTO messages (sender, message) VALUES (?1, ?2)",
                        (request.requester, request.message.message),
                    )
                    .unwrap();
                    continue;
                } else if request.request_type == "get_all_messages" {
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
                                message_type: "chat".to_string(),
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
                } else if request.request_type == "password_request" {
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
                    let mut result = Account::blank();
                    for account in accounts_iter {
                        let account_unwrapped = account.unwrap();
                        if account_unwrapped.username != request.requester {
                            continue;
                        }
                        result = account_unwrapped;
                    }
                    db_response_tx
                        .send(DatabaseResponse {
                            response_type: "password_request".to_string(),
                            requester: request.requester,
                            account: result,
                            send_date: request.send_date,
                        })
                        .unwrap();
                } else if request.request_type == "username_check" {
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
                    let mut result = Account::blank();
                    for account in accounts_iter {
                        let account_unwrapped = account.unwrap();
                        if account_unwrapped.username != request.requester {
                            continue;
                        }
                        result = account_unwrapped;
                        break;
                    }
                    db_response_tx
                        .send(DatabaseResponse {
                            response_type: "username_check".to_string(),
                            requester: request.requester,
                            account: result,
                            send_date: request.send_date,
                        })
                        .unwrap();
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
                                "alert".to_string(),
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
                                "kick".to_string(),
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
                        let mut account = Account::blank();
                        'message_reading_loop: loop {
                            tokio::select! {
                                                            result = buf_reader.read_line(&mut line) => {
                                                                match result {
                                                                    Ok(bytes_read) => {
                                                                        if bytes_read == 0 {
                                                                            line.clear();
                                                                            println!("{socket_address} disconnected!");
                                                                            break 'loop_of_this_connection;
                                                                        }
                                                                        if line.trim().is_empty() {
                                                                            line.clear();
                                                                            continue 'message_reading_loop;
                                                                        }

                                                                        if is_about_to_type_username {
                                                                            username = line.trim().to_string();
                                                                            println!("{socket_address} has entered his username as {username}");

                                                                            write_str(&mut buf_reader, "Enter your password.\n").await;
                                                                            let mut pass = String::new();
                                                                            if buf_reader.read_line(&mut pass).await.unwrap() == 0 {
                                                                                continue 'message_reading_loop;
                                                                            }
                                                                            pass = pass.trim().to_string();
                                                                            if r_or_l == "l" {
                                                                                let now = SystemTime::now();
                                                                                db_tx.send(DatabaseRequest::new("password_request".to_string(),username.clone(),Account::blank(), Message::blank(), now)).unwrap();
                                                                                'db_response_loop: loop {
                                                                                    let response = db_response_rx.recv().await.unwrap();
                                                                                    if response.requester != username || response.response_type != "password_request" || response.send_date != now {
                                                                                        continue 'db_response_loop;
                                                                                    }
                                                                                    if pass != response.account.password {
                                                                                        write_str(&mut buf_reader, "Password is incorrect.\n").await;
                                                                                        buf_reader.shutdown().await.unwrap();
                                                                                        continue 'loop_of_this_connection;
                                                                                    }
                                                                                    write_str(&mut buf_reader, "Log-in Success.\n").await;
                                                                                    account = Account::new(response.account.uuid, response.account.username, socket_address.clone(), response.account.password);
                                                                                    break 'db_response_loop;
                                                                                }
                                                                            } else {
                                                                                let now = SystemTime::now();
                                                                                db_tx.send(DatabaseRequest::new("username_check".to_string(), username.clone(), Account::blank(), Message::blank(), now)).unwrap();
                                                                                'db_response_loop: loop {
                                                                                    let response = db_response_rx.recv().await.unwrap();
                                                                                    if response.requester != username || response.response_type != "username_check" || response.send_date != now {
                                                                                        continue 'db_response_loop;
                                                                                    }
                                                                                    if response.account.uuid != "error" { //then user was found
                                                                                        write_str(&mut buf_reader, "Username is already taken.\n").await;
                                                                                        buf_reader.shutdown().await.unwrap();
                                                                                        continue 'loop_of_this_connection;
                                                                                    }
                                                                                    let uuid = Uuid::new_v4().to_string();
                                                                                    account = Account::new(uuid.clone(), username.clone(), socket_address.clone(), pass.clone());
                                                                                    db_tx.send(DatabaseRequest::new("register_user".to_string(), username.clone(), account.clone(), Message::blank(), SystemTime::now())).unwrap();

                                                                                    write_str(&mut buf_reader, "You have successfully registered.\n").await;

                                                                                    break 'db_response_loop;
                                                                                }
                                                                            }
                                                                            write_str(&mut buf_reader, "You can start typing.\n").await;

                                                                            msg_tx.send((socket_address.clone(), Message::new("broadcast_to_everyone".to_string(), account.uuid.clone(), username.clone(), format!("{} Joined the chat!\n", username)) )).unwrap();

                                                                            db_tx.send(DatabaseRequest::new("get_all_messages".to_string(), username.clone(), Account::blank(), Message::blank(), SystemTime::now())).unwrap();


                                                                        } else {
                                                                            msg_tx.send((socket_address.clone(), Message::new("chat".to_string(), account.uuid.clone(), username.clone(), line.trim().to_string()))).unwrap();
                                                                            db_tx.send(DatabaseRequest::new("messages".to_string(), account.uuid.clone(), Account::blank(), Message::new("chat".to_string(), account.uuid.clone(), "error".to_string(), line.trim().to_string()), SystemTime::now())).unwrap();
                                                                            println!("{socket_address} {username} typed: {}", line.trim());
                                                                        }
                            line.clear();
                                                                        is_about_to_type_username = false;
                                                                    }
                                                                    Err(_) => {continue;}
                                                                }
                                                            }
                                                            result = msg_rx.recv() => {
                                                                match result {
                                                                    Ok((sender_ip, message)) => {
                                                                        if message.message_type.eq("chat") {
                                                                            if message.sender_uuid == account.uuid && sender_ip == socket_address {
                                                                                continue 'message_reading_loop;
                                                                            }
                                                                            let broadcast_message = if message.sender_username.is_empty() {
                                                                                format!("{}\n", message.message)
                                                                            } else {
                                                                               format!("{}: {}\n", message.sender_username, message.message)
                                                                            };
                                                                            write_string(&mut buf_reader, broadcast_message).await;
                                                                            buf_reader.flush().await.unwrap();
                                                                        } else if message.message_type.eq("broadcast_to_everyone") {
                                                                            write_string(&mut buf_reader, message.message).await;
                                                                        } else  if message.message_type.eq("alert") {
                                                                            let broadcast_message = format!("{M_S}Server message: {}\n{M_S}", message.message);
                                                                            write_string(&mut buf_reader, broadcast_message).await;
                                                                        } else if message.message_type.eq("kick") {
                                                                            //this is not a bug.
                                                                            if message.sender_username != account.username && message.sender_username != account.ip_addr {
                                                                                continue 'message_reading_loop;
                                                                            }
                                                                            write_string(&mut buf_reader, format!("{M_S}You are kicked from the server! \nReason: {}\n{M_S}", message.message)).await;
                                                                            buf_reader.shutdown().await.expect("TODO: panic message");

                                                                            println!("{} has been kicked from the server!", message.message);
                                                                            msg_tx.send((socket_address.clone(), Message::new("broadcast_to_everyone".to_string(), "error".to_string(), "error".to_string(), format!("{} has been kicked from the server!\n", message.message)))).unwrap();
                                                                            continue 'loop_of_this_connection;
                                                                        }
                                                                    }
                                                                    Err(_) => {continue;}
                                                                }
                                                            }
                                                            result = vec_of_messages_to_user_rx.recv() => {
                                                                match result {
                                                                    Ok((target_username, messages)) => {
                                                                        if target_username != username {
                                                                            continue 'message_reading_loop;
                                                                        }
                                                                        let mut all = String::from("");
                                                                        //probably more optimized
                                                                        for message in messages {
                                                                            all.push_str(&format!("{}: {}\n", message.sender_username, message.message));
                                                                        }
                                                                        write_string(&mut buf_reader, all).await;
                                                                    }
                                                                    Err(_) => {continue 'message_reading_loop;}
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
