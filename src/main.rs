extern crate byteorder;
extern crate postgres;
extern crate quick_protobuf;
extern crate rand;

use crate::byteorder::ReadBytesExt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream, TcpListener};
use std::io::Write;
use postgres::{Connection, TlsMode};
use postgres::rows::Rows;
use std::process::exit;
use std::thread::spawn;
use byteorder::LittleEndian;

#[derive(Clone, Debug)]
pub struct ChannelRow {
    pub id: i32,
    pub serial: String
}

static PORT: u16 = 3334u16;
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

fn main() {
    println!("Starting cache service");

    let listener: TcpListener = listen();
    let store: Vec<ChannelRow> = get_rows();

    for stream in listener.incoming() {
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

            let buf: &'static str = "Hello world\n";
            let mut buf: &[u8] = buf.as_bytes();

            stream.write_all(&mut buf).unwrap();
        });
    }
}
