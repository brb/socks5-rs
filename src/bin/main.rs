extern crate mio;
extern crate socks5_rs;
extern crate bytes;

use socks5_rs::thc;
use socks5_rs::thc::{FSM, Event, Return};
use self::bytes::Bytes;

const LADDR: &'static str = "127.0.0.1:1080";

fn main() {
    let mut tcp_handler = thc::TcpHandler::new(4);
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

    fn handle_event(&mut self, ev: Event) -> Return {
        println!("handle event");
        match self.inner.next_state {
            State::Init => panic!("invalid"),
            State::ReceiveVsnAndAuthCount => self.inner.receive_vsm_and_auth_count(ev),
            State::ReceiveAuthMethods => self.inner.receive_auth_methods(ev),
            State::ReceiveAddrType => self.inner.receive_addr_type(ev),
        }
    }
}

//#[derive(PartialEq, Debug)]
enum State {
    // TODO macro?
    Init,
    ReceiveVsnAndAuthCount,
    ReceiveAuthMethods,
    ReceiveAddrType,
}

impl Socks5Inner {
    fn receive_vsm_and_auth_count(&mut self, ev: Event) -> Return {
        if let Event::Read(0, bytes) = ev {
            assert_eq!(5, bytes[0]); // SOCKS5 vsn
            self.next_state = State::ReceiveAuthMethods;
            Return::ReadExact(0, bytes[1] as usize)
        } else {
            panic!("invalid");
        }
    }

    fn receive_auth_methods(&mut self, ev: Event) -> Return {
        if let Event::Read(0, ref bytes) = ev {
            // we only support noauth(=0x0)
            let noauth_found = bytes.iter().find(|&b| *b == 0x0);
            if noauth_found == None {
                panic!("noauth not found");
            }
            self.next_state = State::ReceiveAddrType;
            let ret: &[u8] = &[5, 0];
            Return::WriteAndReadExact(0, Bytes::from(ret), 0, 4)
        } else {
            panic!("invalid");
        }
    }

    fn receive_addr_type(&mut self, ev: Event) -> Return {
        panic!("yey");
    }
}
