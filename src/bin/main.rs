extern crate mio;
extern crate bytes;
extern crate tcph;
extern crate workers;

use tcph::{FSM, Event, Return};
use bytes::Bytes;
use bytes::{ByteOrder, BigEndian};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use mio::tcp::TcpStream;
use workers::WorkersPool;

const LADDR: &'static str = "127.0.0.1:1080";

fn main() {
    let mut tcp_handler = tcph::TcpHandler::new(5);
    let addr = LADDR.parse().unwrap();
    fn builder() -> Box<FSM> {
        let s = Socks5{inner: Socks5Inner{next_state: State::Init}};
        Box::new(s)
    };
    tcp_handler.register(&addr, builder);
    tcp_handler.run().unwrap();

}

struct Socks5Inner {
    next_state: State,
}

struct Socks5{
    inner: Socks5Inner,
}

impl FSM for Socks5 {
    fn init(&mut self) -> Return {
        self.inner.next_state = State::ReceiveVsnAndAuthCount;
        Return::ReadExact(0, 2)
    }

    fn handle_event(&mut self, ev: Event) -> Vec<Return> {
        println!("handle event: {:?}", self.inner.next_state);

        match self.inner.next_state {
            State::Init => panic!("invalid state"),
            State::ReceiveVsnAndAuthCount => self.inner.receive_vsm_and_auth_count(ev),
            State::ReceiveAuthMethods => self.inner.receive_auth_methods(ev),
            State::ReceiveAddrType => self.inner.receive_addr_type(ev),
            State::ReceiveAddr => self.inner.receive_addr(ev),
            State::Proxy => self.inner.proxy(ev),
        }
    }
}

#[derive(PartialEq, Debug)]
enum State {
    Init,
    ReceiveVsnAndAuthCount,
    ReceiveAuthMethods,
    ReceiveAddrType,
    ReceiveAddr,
    Proxy,
}

impl Socks5Inner {
    fn receive_vsm_and_auth_count(&mut self, ev: Event) -> Vec<Return> {
        if let Event::Read(0, bytes) = ev {
            assert_eq!(5, bytes[0]); // SOCKS5 vsn
            self.next_state = State::ReceiveAuthMethods;
            return vec![Return::ReadExact(0, bytes[1] as usize)];
        }
        panic!("invalid");
    }

    fn receive_auth_methods(&mut self, ev: Event) -> Vec<Return> {
        if let Event::Read(0, ref buf) = ev {
            // We only support noauth(=0x0)
            let noauth_found = buf.iter().find(|&b| *b == 0x0);
            if noauth_found == None {
                panic!("noauth not found");
            }
            self.next_state = State::ReceiveAddrType;
            let ret: &[u8] = &[5, 0];
            return vec![Return::Write(0, Bytes::from(ret)), Return::ReadExact(0, 4)];
        }
        panic!("invalid");
    }

    fn receive_addr_type(&mut self, ev: Event) -> Vec<Return> {
        if let Event::Read(0, ref buf) = ev {
            assert_eq!(5, buf[0]); // SOCKS5 vsn
            assert_eq!(1, buf[1]); // CMD CONNECT
            // We only support IPv4
            assert_eq!(1, buf[3]); // ATYP IPv4
            self.next_state = State::ReceiveAddr;
            return vec![Return::ReadExact(0, 6)];
        }
        panic!("invalid");
    }

    fn receive_addr(&mut self, ev: Event) -> Vec<Return> {
        if let Event::Read(0, ref buf) = ev {
            let addr = IpAddr::V4(Ipv4Addr::new(buf[0], buf[1], buf[2], buf[3]));
            let port: u16 = BigEndian::read_u16(&[buf[4], buf[5]][..]);
            let target = TcpStream::connect(&SocketAddr::new(addr, port)).unwrap();

            let addr = target.local_addr().unwrap();
            if let IpAddr::V4(ip) = addr.ip() {
                let port = addr.port();
                let ip = ip.octets();
                let reply: &[u8] = &[
                    5, 0, 0, 1,
                    ip[0], ip[1], ip[2], ip[3],
                    (port >> 8) as u8, port as u8
                ];
                self.next_state = State::Proxy;
                return vec![
                    Return::Write(0, Bytes::from(reply)),
                    Return::Read(0),
                    Return::Register(1, target),
                    Return::Read(1),
                ];
            }
        }
        panic!("invalid");
    }

    fn proxy(&mut self, ev: Event) -> Vec<Return> {
        if let Event::Read(0, buf) = ev {
            return vec![
                Return::Write(1, buf),
                Return::Read(0),
            ];
        } else if let Event::Read(1, buf) = ev {
            return vec![
                Return::Write(0, buf),
                Return::Read(1),
            ];
        } else if let Event::Terminate(0, buf) = ev {
            return vec![
                Return::Write(1, buf),
            ];
        } else if let Event::Terminate(1, buf) = ev {
            return vec![
                Return::Write(0, buf),
            ];
        }
        panic!("invalid");
    }
}
