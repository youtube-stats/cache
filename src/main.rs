extern crate byteorder;
extern crate postgres;
extern crate quick_protobuf;
extern crate rand;

pub mod message;
use message::ChannelMessage;

use crate::byteorder::ReadBytesExt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream, TcpListener};
use std::io::Write;
use postgres::{Connection, TlsMode};
use postgres::rows::Rows;
use std::process::exit;
use std::thread::spawn;
use byteorder::LittleEndian;
use rand::rngs::ThreadRng;
use rand::thread_rng;
use rand::seq::SliceRandom;
use std::borrow::Cow;
use quick_protobuf::serialize_into_vec;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct ChannelRow {
    pub id: i32,
    pub serial: String
}

static PORT: u16 = 3334u16;
static SLEEP: u64 = 7200u64;
static POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
static QUERY: &'static str = "SELECT id, serial FROM youtube.stats.channels";

pub fn listen() -> TcpListener {
    let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
    let port: u16 = PORT;
    println!("Listening on port {}", port);

    let addr: SocketAddr = SocketAddr::new(ip, port);
    TcpListener::bind(&addr)
        .expect("unable to bind TCP listener")
}

pub fn get_rows() -> Vec<ChannelRow> {
    println!("Getting channels");

    let conn: Connection = {
        let params: &'static str = POSTGRESQL_URL;
        let tls: TlsMode = TlsMode::None;

        Connection::connect(params, tls)
            .expect("Could not connect to database")
    };
    let query: &'static str = QUERY;

    let results: Rows = conn.query(query, &[])
        .expect("Could not query db");

    let mut rows: Vec<ChannelRow> = Vec::new();
    for row in &results {
        let id: i32 = row.get(0);
        let serial: String = row.get(1);

        let value: ChannelRow = ChannelRow {
            id,
            serial
        };

        rows.push(value);
    }

    println!("Retrieved {} rows", rows.len());
    rows
}

fn get_50(channels: &Vec<ChannelRow>, length: usize) -> Vec<ChannelRow> {
    let mut rng: ThreadRng = thread_rng();
    let amount: usize = 50;

    let collect: &[ChannelRow] = &channels[..length];
    let collect: Vec<ChannelRow> = collect.to_vec();

    collect.choose_multiple(&mut rng, amount).cloned().collect()
}

pub fn get_msg(channels: &Vec<ChannelRow>, length: usize) -> Vec<u8> {
    let sampled: Vec<ChannelRow> = get_50(channels, length);

    let mut message: ChannelMessage = ChannelMessage::default();

    for i in 0..50 {
        let row: &ChannelRow = &sampled[i];

        let value: i32 = row.id;
        message.ids.push(value);

        let value = &row.serial;
        let value: Cow<str> = Cow::from(value);

        message.serials.push(value);
    }

    println!("Sending channel message {:?}", message);
    serialize_into_vec(&message)
        .expect("Could not serialize")
}

fn main() {
    println!("Starting cache service");

    let listener: TcpListener = listen();
    let channels1: Arc<Mutex<Vec<ChannelRow>>> = Arc::new(Mutex::new(get_rows()));
    let channels2: Arc<Mutex<Vec<ChannelRow>>> = Arc::clone(&channels1);

    spawn(move || {
        let dur: Duration = std::time::Duration::from_secs(SLEEP);

        loop {
            println!("Will update channels in {} seconds", SLEEP);
            std::thread::sleep(dur);
            println!("Updating channels");

            let channels = get_rows();
            *channels1.lock().unwrap() = channels;
        }
    });

    for stream in listener.incoming() {
        let channels: Arc<Mutex<Vec<ChannelRow>>> = Arc::clone(&channels2);

        spawn(move || {
            if stream.is_err() {
                eprintln!("Connection is bad: {:?}", stream);
                exit(3);
            }

            let mut stream: TcpStream = stream.unwrap();
            let n_option= stream.read_u32::<LittleEndian>();
            if n_option.is_err() {
                eprintln!("Could not read u32 from socket");
                return;
            }

            let n: u32 = n_option.unwrap();
            println!("Got {}", n);
            let length: usize = n as usize;
            let channels: &Vec<ChannelRow> = &channels.lock().unwrap();

            let buf: Vec<u8> = get_msg(channels, length);
            let mut buf: &[u8] = buf.as_slice();
            stream.write_all(&mut buf).unwrap();
        });
    }
}
