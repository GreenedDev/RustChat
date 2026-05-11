use tokio::{
    io::{AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[derive(Debug, Clone, PartialEq)]
pub enum MessageType {
    Chat,
    Broadcast,
    Alert,
    Kick,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub message_type: MessageType,
    pub sender_uuid: String,
    pub sender_username: String,
    pub message: String,
}

impl Message {
    pub fn new(
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
impl Account {
    pub fn new(uuid: String, username: String, ip_addr: String, password: String) -> Account {
        Account {
            uuid,
            username,
            ip_addr,
            password,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Account {
    pub uuid: String,
    pub username: String,
    pub ip_addr: String,
    pub password: String,
}
pub struct Client {
    pub stream: BufReader<TcpStream>,
    pub account: Account,
}
impl Client {
    pub async fn write_string(&mut self, message: String) {
        self.stream.write_all(message.as_bytes()).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    pub async fn write_str(&mut self, message: &str) {
        self.stream.write_all(message.as_bytes()).await.unwrap();
        self.stream.flush().await.unwrap();
    }
}
