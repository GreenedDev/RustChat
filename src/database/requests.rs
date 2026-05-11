use tokio::sync::oneshot;

use crate::server::types::{Account, Message};

pub enum DatabaseRequest {
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
