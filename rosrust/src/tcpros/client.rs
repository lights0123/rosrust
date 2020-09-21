use super::error::{ErrorKind, Result, ResultExt};
use super::{ServicePair, ServiceResult};
use crate::rosmsg::RosMsg;
use crate::runtime::{block_on, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, TcpStream};
use crate::tcpros::header::{decode_async, encode_async};
use error_chain::bail;
use log::error;
use std::collections::HashMap;
use std::io;
use std::io::Cursor;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;
use std::thread;

pub struct ClientResponse<T> {
    handle: thread::JoinHandle<Result<ServiceResult<T>>>,
}

impl<T> ClientResponse<T> {
    pub fn read(self) -> Result<ServiceResult<T>> {
        self.handle
            .join()
            .unwrap_or_else(|_| Err(ErrorKind::ServiceResponseUnknown.into()))
    }
}

impl<T: Send + 'static> ClientResponse<T> {
    pub fn callback<F>(self, callback: F)
    where
        F: FnOnce(Result<ServiceResult<T>>) + Send + 'static,
    {
        thread::spawn(move || callback(self.read()));
    }
}

struct ClientInfo {
    caller_id: String,
    uri: String,
    service: String,
}

#[derive(Clone)]
pub struct Client<T: ServicePair> {
    info: std::sync::Arc<ClientInfo>,
    phantom: std::marker::PhantomData<T>,
}

async fn connect_to_tcp_with_multiple_attempts(
    uri: &str,
    attempts: usize,
) -> io::Result<TcpStream> {
    let mut err = io::Error::new(
        io::ErrorKind::Other,
        "Tried to connect via TCP with 0 connection attempts",
    );
    let mut repeat_delay_ms = 1;
    for _ in 0..attempts {
        let stream_result = TcpStream::connect(uri).await.and_then(|stream| {
            stream.set_linger(None)?;
            Ok(stream)
        });
        match stream_result {
            Ok(stream) => {
                return Ok(stream);
            }
            Err(error) => err = error,
        }
        std::thread::sleep(std::time::Duration::from_millis(repeat_delay_ms));
        repeat_delay_ms *= 2;
    }
    Err(err)
}

impl<T: ServicePair> Client<T> {
    pub fn new(caller_id: &str, uri: &str, service: &str) -> Client<T> {
        Client {
            info: std::sync::Arc::new(ClientInfo {
                caller_id: String::from(caller_id),
                uri: String::from(uri),
                service: String::from(service),
            }),
            phantom: std::marker::PhantomData,
        }
    }

    pub fn req(&self, args: &T::Request) -> Result<ServiceResult<T::Response>> {
        block_on(self.async_req(args))
    }

    pub fn req_async(&self, args: T::Request) -> ClientResponse<T::Response> {
        let info = Arc::clone(&self.info);
        ClientResponse {
            handle: thread::spawn(move || {
                block_on(Self::request_body(
                    &args,
                    &info.uri,
                    &info.caller_id,
                    &info.service,
                ))
            }),
        }
    }

    pub async fn async_req(&self, args: &T::Request) -> Result<ServiceResult<T::Response>> {
        Self::request_body(
            args,
            &self.info.uri,
            &self.info.caller_id,
            &self.info.service,
        )
        .await
    }

    async fn request_body(
        args: &T::Request,
        uri: &str,
        caller_id: &str,
        service: &str,
    ) -> Result<ServiceResult<T::Response>> {
        let trimmed_uri = uri.trim_start_matches("rosrpc://");
        let mut stream = connect_to_tcp_with_multiple_attempts(trimmed_uri, 15)
            .await
            .chain_err(|| ErrorKind::ServiceConnectionFail(service.into(), uri.into()))?;

        // Service request starts by exchanging connection headers
        exchange_headers::<T, _>(&mut stream, caller_id, service).await?;

        let mut writer = io::Cursor::new(Vec::with_capacity(128));
        // skip the first 4 bytes that will contain the message length
        writer.set_position(4);

        args.encode(&mut writer)?;

        // write the message length to the start of the header
        let message_length = (writer.position() - 4) as u32;
        writer.set_position(0);
        message_length.encode(&mut writer)?;

        // Send request to service
        stream.write_all(&writer.into_inner()).await?;

        // Service responds with a boolean byte, signalling success
        let success = read_verification_byte(&mut stream)
            .await
            .chain_err(|| ErrorKind::ServiceResponseInterruption)?;
        let len = stream.read_u32_le().await?;
        Ok(if success {
            // Decode response as response type upon success

            // TODO: validate response length
            let mut buf = vec![0; len as usize];
            stream.read_exact(&mut buf).await?;

            let data = RosMsg::decode(Cursor::new(buf))?;

            let mut dump = vec![];
            if let Err(err) = stream.read_to_end(&mut dump).await {
                error!("Failed to read from TCP stream: {:?}", err)
            }

            Ok(data)
        } else {
            let len = stream.read_u32_le().await?;
            let offset = size_of_val(&len);
            let mut buf = vec![0; offset + len as usize];
            buf[..offset].copy_from_slice(&len.to_le_bytes());
            stream.read_exact(&mut buf[size_of::<u32>()..]).await?;
            // Decode response as string upon failure
            let data = RosMsg::decode(Cursor::new(buf))?;

            let mut dump = vec![];
            if let Err(err) = stream.read_to_end(&mut dump).await {
                error!("Failed to read from TCP stream: {:?}", err)
            }

            Err(data)
        })
    }
}

#[inline]
async fn read_verification_byte<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<bool> {
    reader.read_u8().await.map(|v| v != 0)
}

async fn write_request<T, U>(mut stream: &mut U, caller_id: &str, service: &str) -> Result<()>
where
    T: ServicePair,
    U: AsyncWrite + Unpin,
{
    let mut fields = HashMap::<String, String>::new();
    fields.insert(String::from("callerid"), String::from(caller_id));
    fields.insert(String::from("service"), String::from(service));
    fields.insert(String::from("md5sum"), T::md5sum());
    fields.insert(String::from("type"), T::msg_type());
    encode_async(&mut stream, &fields).await?;
    Ok(())
}

async fn read_response<T, U>(mut stream: &mut U) -> Result<()>
where
    T: ServicePair,
    U: AsyncRead + Unpin,
{
    let fields = decode_async(&mut stream).await?;
    if fields.get("callerid").is_none() {
        bail!(ErrorKind::HeaderMissingField("callerid".into()));
    }
    Ok(())
}

async fn exchange_headers<T, U>(stream: &mut U, caller_id: &str, service: &str) -> Result<()>
where
    T: ServicePair,
    U: AsyncWrite + AsyncRead + Unpin,
{
    write_request::<T, U>(stream, caller_id, service).await?;
    read_response::<T, U>(stream).await
}
