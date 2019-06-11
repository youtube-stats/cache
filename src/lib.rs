use rand::seq::SliceRandom;
use rand::rngs::ThreadRng;
use postgres::{Connection, TlsMode};
use postgres::rows::Rows;
use quick_protobuf::serialize_into_vec;
use std::str;

pub mod message;
use message::ChannelMessage;

use std::borrow::Cow;

static POSTGRESQL_URL: &'static str = "postgresql://admin@localhost:5432/youtube";
static QUERY: &'static str = "SELECT id, serial FROM youtube.stats.channels";

#[derive(Clone)]
pub struct ChannelRow {
    pub id: i32,
    pub serial: [u8; 24]
}

#[derive(Clone)]
pub struct Channels {
    pub rows: Vec<ChannelRow>
}

impl Channels {
    pub fn init() -> Channels {
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
            let serial: [u8; 24] = {
                let chars: &[u8] = serial.as_bytes();
                let mut serial: [u8; 24] = [0u8; 24];
                for i in 0..24 {
                    serial[i] = chars[i];
                }

                serial
            };

            let value: ChannelRow = ChannelRow {
                id,
                serial
            };

            rows.push(value);
        }

        println!("Retrieved {} rows", rows.len());

        Channels {
            rows
        }
    }

    pub fn len(self: &Channels) -> usize {
        self.rows.len()
    }

    fn get(self: &Channels, i: usize) -> &ChannelRow {
        &self.rows[i]
    }

    fn get_50(self: &Channels, rng: &ThreadRng, length: usize) -> Channels {
        let mut rng: ThreadRng = rng.clone();
        let amount: usize = 50;

        let collect: &[ChannelRow] = &self.rows[..length];
        let collect: Vec<ChannelRow> = collect.to_vec();

        let rows: Vec<ChannelRow> =
            collect.choose_multiple(&mut rng, amount).cloned().collect();

        Channels {
            rows
        }
    }

    pub fn get_msg(self: &Channels, rng: &ThreadRng, length: usize) -> Vec<u8> {
        let sampled: Channels = self.get_50(rng, length);

        let mut message: ChannelMessage = ChannelMessage::default();

        for i in 0..50 {
            let row: &ChannelRow = sampled.get(i);

            let value: i32 = row.id;
            message.ids.push(value);

            let value: &str = str::from_utf8(row.serial.as_ref())
                .expect("Could not convert str");
            let value: String = value.to_string();
            let value: Cow<str> = Cow::from(value);

            message.serials.push(value);
        }

        println!("Sending channel message {:?}", message);

        let bytes: Vec<u8> = serialize_into_vec(&message)
            .expect("Could not serialize");

        bytes
    }
}