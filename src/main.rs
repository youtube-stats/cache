extern crate postgres;
extern crate quick_protobuf;
extern crate rand;
extern crate tokio;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use crate::tokio::executor::spawn;
use crate::tokio::io::write_all;
use crate::tokio::net::{TcpListener,TcpStream};
use crate::tokio::prelude::{Future,Stream};
use crate::tokio::run;

static PORT: u16 = 3334u16;

fn main() {
    let listener: TcpListener = {
        let ip: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let port: u16 = PORT;

        let addr: SocketAddr = SocketAddr::new(ip, port);
        TcpListener::bind(&addr)
            .expect("unable to bind TCP listener")
    };

    let future = listener.incoming()
        .map_err(|e| eprintln!("accept failed = {:?}", e))
        .for_each(move |a: TcpStream| {
            let buf: &'static str = "Hello world\n";

            let f = write_all(a, buf).then(|result| {
                println!("wrote to stream; success={:?}", result.is_ok());
                Ok(())
            });

            spawn(f)
        });

    run(future);
}
