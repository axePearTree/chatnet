use futures::{SinkExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, net::SocketAddr, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{
        broadcast::{self, Sender},
        Mutex,
    },
};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub type AnyError = Box<dyn Error + Send + Sync>;
pub type Result<T = ()> = core::result::Result<T, AnyError>;
pub type Shared<T> = Arc<Mutex<T>>;
pub type Kv<T> = Shared<HashMap<String, T>>;

pub struct Server {
    listener: TcpListener,
    connections: Kv<UserConnection>,
    sender: Shared<Sender<InternalMessage>>,
}

pub struct UserConnection {
    sender: Sender<InternalMessage>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum IncomingMessage {
    Ping,
    Login { username: String },
    BroadcastMessage { message: String },
    PrivateMessage { message: String, to_username: String }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OutgoingMessage {
    Pong,
    MessageReceived {
        from_username: String,
        message: String,
        private: bool,
    },
    LoginSuccessful,
    Error(String),
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum InternalMessage {
    MessageBroadcast {
        from_username: String,
        message: String,
        private: bool,
    },
}

impl Server {
    pub async fn new(addr: &str) -> Result<Self> {
        let (sender, _) = broadcast::channel(10);
        Ok(Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            listener: TcpListener::bind(addr).await?,
            sender: Arc::new(Mutex::new(sender)),
        })
    }

    pub async fn run(self) -> Result {
        loop {
            let (stream, addr) = self.listener.accept().await?;
            let connections = Arc::clone(&self.connections);
            let sender = Arc::clone(&self.sender);
            tokio::spawn(async move {
                handle_connection(stream, addr, connections, sender).await?;
                Ok(()) as Result
            });
        }
    }
}

async fn handle_connection(
    stream: TcpStream,
    _addr: SocketAddr,
    connections: Kv<UserConnection>,
    sender: Shared<Sender<InternalMessage>>,
) -> Result {
    let mut global_receiver = sender.lock().await.subscribe();

    let (read, write) = tokio::io::split(stream);

    let mut deserializer = {
        let length_delimited = FramedRead::new(read, LengthDelimitedCodec::new());
        tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalJson::<IncomingMessage>::default(),
        )
    };

    let mut serializer = {
        let length_delimited = FramedWrite::new(write, LengthDelimitedCodec::new());
        tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            SymmetricalJson::<OutgoingMessage>::default(),
        )
    };

    // handle first message
    let username = match deserializer.try_next().await.unwrap() {
        Some(IncomingMessage::Login { username }) => {
            serializer.send(OutgoingMessage::LoginSuccessful).await?;
            username
        }
        _ => {
            serializer
                .send(OutgoingMessage::Error(String::from(
                    "First message must be a login",
                )))
                .await?;
            return Err(String::from("First message must be a login").into());
        }
    };

    let mut private_receiver = {
        let mut connections = connections.lock().await;
        if let Some(conn) = connections.get(&username) {
            let sender = conn.sender.clone();
            sender.subscribe()
        } else {
            let (sender, receiver) = broadcast::channel(10);
            connections.insert(username.clone(), UserConnection {
                sender: sender.clone(),
            });
            receiver
        }
    };

    loop {
        enum Message {
            InternalMessage(InternalMessage),
            IncomingMessage(IncomingMessage),
        }

        let message = tokio::select! {
            v = deserializer.try_next() =>
                v.map(|v| v.map(|v| Message::IncomingMessage(v))).map_err(|e| e.to_string()),
            v = global_receiver.recv() =>
                v.map(|v| Some(Message::InternalMessage(v))).map_err(|e| e.to_string()),
            v = private_receiver.recv() =>
                v.map(|v| Some(Message::InternalMessage(v))).map_err(|e| e.to_string()),
        };

        if let Some(message) = message? {
            // handle all messages
            match message {
                Message::IncomingMessage(msg) => match msg {
                    IncomingMessage::Ping => {
                        serializer.send(OutgoingMessage::Pong).await?;
                    }
                    IncomingMessage::BroadcastMessage { message } => {
                        let _ = sender
                            .lock()
                            .await
                            .send(InternalMessage::MessageBroadcast {
                                from_username: username.clone(),
                                message,
                                private: false,
                            });
                    }
                    IncomingMessage::PrivateMessage { message, to_username } => {
                        if let Some(conn) = connections.lock().await.get(&to_username) {
                            let _ = conn.sender.send(InternalMessage::MessageBroadcast {
                                from_username: username.clone(),
                                message,
                                private: true,
                            });
                        }
                    }
                    _ => {}
                },
                Message::InternalMessage(msg) => match msg {
                    InternalMessage::MessageBroadcast {
                        from_username,
                        message,
                        private,
                    } => {
                        serializer
                            .send(OutgoingMessage::MessageReceived {
                                from_username,
                                message,
                                private,
                            })
                            .await?;
                    }
                },
            }
        }
    }
}
