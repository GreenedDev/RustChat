use tokio::{
    io::{AsyncBufReadExt, BufReader},
    sync::broadcast::Sender,
};

use crate::{Message, MessageType};

pub async fn spawn_cli_manager(msg_tx: Sender<(String, Message)>) {
    let msg_tx_clone = msg_tx.clone();
    tokio::spawn(async move {
        let mut input_buf_reader = BufReader::new(tokio::io::stdin());

        let mut raw_input_line = String::new();

        'reading_loop: loop {
            input_buf_reader
                .read_line(&mut raw_input_line)
                .await
                .unwrap();
            if !raw_input_line.contains(" ") {
                print_help_message().await;
                raw_input_line.clear();
                continue 'reading_loop;
            }
            let input_line = raw_input_line.trim().to_string();
            let command_name = input_line.split(" ").next().unwrap();
            let rest_of_command = input_line.split_at(command_name.len() + 1).1;
            raw_input_line.clear();
            match command_name.to_lowercase().as_str() {
                "alert" => {
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
                "help" => {
                    print_help_message().await;
                    continue 'reading_loop;
                }
                "kick" => {
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

                _ => {
                    println!("Unable to find command. Type /help for list of server commands.");
                }
            }
        }
    });
}
//this is message separator
pub const M_S: &str = "------------------------------\n";

async fn print_help_message() {
    print!("{M_S}");
    println!("Chat Commands:");
    println!(" /alert <message> - Broadcast message to all connected users.");
    println!(" /kick <username>/<ip> | <reason> - Kick connected user from the server.");
    println!(" / - \n");
    print!("{M_S}");
}
