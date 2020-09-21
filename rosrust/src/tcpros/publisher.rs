use super::error::{ErrorKind, Result, ResultExt};
use super::header;
use super::util::streamfork::{fork, DataStream, TargetList};
use super::{Message, Topic};
use crate::runtime::{spawn, AsyncRead, AsyncWrite, AsyncWriteExt, TcpListener, ToSocketAddrs};
use crate::util::FAILED_TO_LOCK;
use crate::RawMessageDescription;
use error_chain::bail;
use futures_util::future::{abortable, AbortHandle};
use log::error;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Publisher {
    subscriptions: DataStream,
    pub port: u16,
    pub topic: Topic,
    last_message: Arc<Mutex<Arc<Vec<u8>>>>,
    queue_size: usize,
    abort: AbortHandle,
}

impl Drop for Publisher {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

fn match_headers(
    fields: &HashMap<String, String>,
    topic: &str,
    message_description: &RawMessageDescription,
) -> Result<()> {
    header::match_field(fields, "md5sum", &message_description.md5sum)
        .or_else(|e| header::match_field(fields, "md5sum", "*").or(Err(e)))?;
    header::match_field(fields, "type", &message_description.msg_type)
        .or_else(|e| header::match_field(fields, "type", "*").or(Err(e)))?;
    header::match_field(fields, "topic", topic)?;
    Ok(())
}

async fn read_request<U: AsyncRead + Unpin>(
    mut stream: &mut U,
    topic: &str,
    message_description: &RawMessageDescription,
) -> Result<String> {
    let fields = header::decode_async(&mut stream).await?;
    match_headers(&fields, topic, message_description)?;
    let caller_id = fields
        .get("callerid")
        .ok_or_else(|| ErrorKind::HeaderMissingField("callerid".into()))?;
    Ok(caller_id.clone())
}

async fn write_response<U: AsyncWrite + Unpin>(
    mut stream: &mut U,
    caller_id: &str,
    message_description: &RawMessageDescription,
) -> Result<()> {
    let mut fields = HashMap::<String, String>::new();
    fields.insert(String::from("md5sum"), message_description.md5sum.clone());
    fields.insert(String::from("type"), message_description.msg_type.clone());
    fields.insert(String::from("callerid"), caller_id.into());
    fields.insert(
        String::from("message_definition"),
        message_description.msg_definition.clone(),
    );
    header::encode_async(&mut stream, &fields).await?;
    Ok(())
}

async fn exchange_headers<U>(
    mut stream: &mut U,
    topic: &str,
    pub_caller_id: &str,
    message_description: &RawMessageDescription,
) -> Result<String>
where
    U: AsyncRead + AsyncWrite + Unpin,
{
    let caller_id = read_request(&mut stream, topic, message_description).await?;
    write_response(&mut stream, pub_caller_id, message_description).await?;
    Ok(caller_id)
}

async fn process_subscriber<U>(
    topic: &str,
    mut stream: U,
    targets: &TargetList<U>,
    last_message: &Mutex<Arc<Vec<u8>>>,
    pub_caller_id: &str,
    message_description: &RawMessageDescription,
) where
    U: AsyncRead + AsyncWrite + Unpin + Send,
{
    let result = exchange_headers(&mut stream, topic, pub_caller_id, message_description)
        .await
        .chain_err(|| ErrorKind::TopicConnectionFail(topic.into()));
    let caller_id = match result {
        Ok(caller_id) => caller_id,
        Err(err) => {
            let info = err
                .iter()
                .map(|v| format!("{}", v))
                .collect::<Vec<_>>()
                .join("\nCaused by:");
            error!("{}", info);
            return;
        }
    };

    let message = last_message.lock().expect(FAILED_TO_LOCK).clone();
    if let Err(err) = stream.write_all(&message).await {
        error!("{}", err);
        return;
    }

    let _ = targets.add(caller_id, stream);
}

impl Publisher {
    pub async fn new<U>(
        address: U,
        topic: &str,
        queue_size: usize,
        caller_id: &str,
        message_description: RawMessageDescription,
    ) -> Result<Publisher>
    where
        U: ToSocketAddrs,
    {
        let mut listener = TcpListener::bind(address).await?;
        let socket_address = listener.local_addr()?;

        let port = socket_address.port();
        let (targets, data) = fork(queue_size);
        let last_message = Arc::new(Mutex::new(Arc::new(Vec::new())));

        let abort = {
            let topic = topic.to_string();
            let caller_id = caller_id.to_string();
            let last_message = Arc::clone(&last_message);
            let message_description = message_description.clone();
            let (f, abort) = abortable(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            process_subscriber(
                                &topic,
                                stream,
                                &targets,
                                &last_message,
                                &caller_id,
                                &message_description,
                            )
                            .await
                        }
                        Err(err) => {
                            error!("TCP connection failed at topic '{}': {}", &topic, err);
                        }
                    }
                }
            });
            spawn(f);
            abort
        };

        let topic = Topic {
            name: String::from(topic),
            msg_type: message_description.msg_type,
        };

        Ok(Publisher {
            subscriptions: data,
            port,
            topic,
            last_message,
            queue_size,
            abort,
        })
    }

    pub fn stream<T: Message>(
        &self,
        queue_size: usize,
        message_description: RawMessageDescription,
    ) -> Result<PublisherStream<T>> {
        let mut stream = PublisherStream::new(self, message_description)?;
        stream.set_queue_size_max(queue_size);
        Ok(stream)
    }

    pub fn get_topic(&self) -> &Topic {
        &self.topic
    }
}

// TODO: publisher should only be removed from master API once the publisher and all
// publisher streams are gone. This should be done with a RAII Arc, residing next todo
// the datastream. So maybe replace DataStream with a wrapper that holds that Arc too

#[derive(Clone)]
pub struct PublisherStream<T: Message> {
    stream: DataStream,
    last_message: Arc<Mutex<Arc<Vec<u8>>>>,
    datatype: std::marker::PhantomData<T>,
    latching: bool,
}

impl<T: Message> PublisherStream<T> {
    fn new(
        publisher: &Publisher,
        message_description: RawMessageDescription,
    ) -> Result<PublisherStream<T>> {
        if publisher.topic.msg_type != message_description.msg_type {
            bail!(ErrorKind::MessageTypeMismatch(
                publisher.topic.msg_type.clone(),
                message_description.msg_type,
            ));
        }
        let mut stream = PublisherStream {
            stream: publisher.subscriptions.clone(),
            datatype: std::marker::PhantomData,
            last_message: Arc::clone(&publisher.last_message),
            latching: false,
        };
        stream.set_queue_size_max(publisher.queue_size);
        Ok(stream)
    }

    #[inline]
    pub fn subscriber_count(&self) -> usize {
        self.stream.target_count()
    }

    #[inline]
    pub fn subscriber_names(&self) -> Vec<String> {
        self.stream.target_names()
    }

    #[inline]
    pub fn set_latching(&mut self, latching: bool) {
        self.latching = latching;
    }

    #[inline]
    pub fn set_queue_size(&mut self, queue_size: usize) {
        self.stream.set_queue_size(queue_size);
    }

    #[inline]
    pub fn set_queue_size_max(&mut self, queue_size: usize) {
        self.stream.set_queue_size_max(queue_size);
    }

    pub fn send(&self, message: &T) -> Result<()> {
        let bytes = Arc::new(message.encode_vec()?);

        if self.latching {
            *self.last_message.lock().expect(FAILED_TO_LOCK) = Arc::clone(&bytes);
        }

        // Subscriptions can only be closed from the Publisher side
        // There is no way for the streamfork thread to fail by itself
        self.stream.send(bytes).expect("Connected thread died");
        Ok(())
    }
}
