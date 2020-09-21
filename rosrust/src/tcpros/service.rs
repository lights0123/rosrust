use super::error::{ErrorKind, Result};
use super::header;
use super::{ServicePair, ServiceResult};
use crate::rosmsg::{encode_str, RosMsg};
use crate::runtime::{spawn, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, TcpListener};
use error_chain::bail;
use futures_util::future::{abortable, AbortHandle};
use log::error;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::Cursor;
use std::sync::Arc;

pub struct Service {
    pub api: String,
    pub msg_type: String,
    pub service: String,
    abort: AbortHandle,
}

impl Drop for Service {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

impl Service {
    pub async fn new<T, F, U>(
        hostname: &str,
        bind_address: &str,
        port: u16,
        service: &str,
        node_name: &str,
        handler: F,
    ) -> Result<Service>
    where
        T: ServicePair,
        U: Future<Output = ServiceResult<T::Response>> + Send,
        F: Fn(T::Request) -> U + Send + Sync + 'static,
    {
        let mut listener = TcpListener::bind((bind_address, port)).await?;
        let socket_address = listener.local_addr()?;
        let api = format!("rosrpc://{}:{}", hostname, socket_address.port());

        let abort = {
            let service = String::from(service);
            let node_name = String::from(node_name);
            let handler = Arc::new(handler);
            let (f, abort) = abortable(async move {
                loop {
                    match listener.accept().await {
                        Ok((stream, _)) => {
                            consume_client::<T, _, _, _>(
                                &service,
                                &node_name,
                                Arc::clone(&handler),
                                stream,
                            )
                            .await;
                        }
                        Err(err) => {
                            error!("TCP connection failed at service '{}': {}", &service, err);
                        }
                    }
                }
            });
            spawn(f);
            abort
        };

        Ok(Service {
            api,
            msg_type: T::msg_type(),
            service: String::from(service),
            abort,
        })
    }
}

enum RequestType {
    Probe,
    Action,
}

async fn consume_client<T, U, F, P>(service: &str, node_name: &str, handler: Arc<F>, mut stream: U)
where
    T: ServicePair,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(T::Request) -> P + Send + Sync + 'static,
    P: Future<Output = ServiceResult<T::Response>> + Send,
{
    // Service request starts by exchanging connection headers
    match exchange_headers::<T, _>(&mut stream, service, node_name).await {
        Err(err) => {
            // Connection can be closed when a client checks for a service.
            if !err.is_closed_connection() {
                error!(
                    "Failed to exchange headers for service '{}': {}",
                    service, err
                );
            }
            return;
        }

        // Spawn a thread for handling requests
        Ok(RequestType::Action) => {
            spawn_request_handler::<T, U, F, _>(stream, Arc::clone(&handler)).await
        }
        Ok(RequestType::Probe) => (),
    }
}

async fn exchange_headers<T, U>(
    stream: &mut U,
    service: &str,
    node_name: &str,
) -> Result<RequestType>
where
    T: ServicePair,
    U: AsyncWrite + AsyncRead + Unpin,
{
    let req_type = read_request::<T, U>(stream, service).await?;
    write_response::<T, U>(stream, node_name).await?;
    Ok(req_type)
}

async fn read_request<T: ServicePair, U: AsyncRead + Unpin>(
    stream: &mut U,
    service: &str,
) -> Result<RequestType> {
    let fields = header::decode_async(stream).await?;
    header::match_field(&fields, "service", service)?;
    if fields.get("callerid").is_none() {
        bail!(ErrorKind::HeaderMissingField("callerid".into()));
    }
    if header::match_field(&fields, "probe", "1").is_ok() {
        return Ok(RequestType::Probe);
    }
    header::match_field(&fields, "md5sum", &T::md5sum())?;
    Ok(RequestType::Action)
}

async fn write_response<T, U>(stream: &mut U, node_name: &str) -> Result<()>
where
    T: ServicePair,
    U: AsyncWrite + Unpin,
{
    let mut fields = HashMap::<String, String>::new();
    fields.insert(String::from("callerid"), String::from(node_name));
    fields.insert(String::from("md5sum"), T::md5sum());
    fields.insert(String::from("type"), T::msg_type());
    header::encode_async(stream, &fields).await?;
    Ok(())
}

async fn spawn_request_handler<T, U, F, P>(stream: U, handler: Arc<F>)
where
    T: ServicePair,
    U: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    F: Fn(T::Request) -> P + Send + Sync + 'static,
    P: Future<Output = ServiceResult<T::Response>> + Send + Send,
{
    spawn(async move {
        if let Err(err) = handle_request_loop::<T, U, F, P>(stream, &handler).await {
            if !err.is_closed_connection() {
                let info = err
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join("\nCaused by:");
                error!("{}", info);
            }
        }
    });
}

async fn handle_request_loop<T, U, F, P>(mut stream: U, handler: &F) -> Result<()>
where
    T: ServicePair,
    U: AsyncRead + AsyncWrite + Unpin,
    F: Fn(T::Request) -> P,
    P: Future<Output = ServiceResult<T::Response>> + Send,
{
    // Receive request from client
    // TODO: validate message length
    let len = stream.read_u32_le().await?;
    let mut buf = vec![0; len as usize];
    stream.read_exact(&mut buf).await?;
    // Break out of loop in case of failure to read request
    // TODO: handle retained connections
    if let Ok(req) = RosMsg::decode(Cursor::new(buf)) {
        // Call function that handles request and returns response
        match handler(req).await {
            Ok(res) => {
                // Send True flag and response in case of success
                stream.write_u8(1).await?;
                let mut writer = io::Cursor::new(Vec::with_capacity(128));
                // skip the first 4 bytes that will contain the message length
                writer.set_position(4);

                res.encode(&mut writer)?;

                // write the message length to the start of the header
                let message_length = (writer.position() - 4) as u32;
                writer.set_position(0);
                message_length.encode(&mut writer)?;

                stream.write_all(&writer.into_inner()).await?;
            }
            Err(message) => {
                // Send False flag and error message string in case of failure
                stream.write_u8(0).await?;
                let mut buf = vec![];
                RosMsg::encode(&message, &mut buf)?;
                stream.write_all(&buf).await?;
            }
        };
    }

    // Upon failure to read request, send client failure message
    // This can be caused by actual issues or by the client stopping the connection
    stream.write_u8(0).await?;
    let mut buf = vec![];
    encode_str("Failed to parse passed arguments", &mut buf)?;
    stream.write_all(&buf).await?;
    Ok(())
}
