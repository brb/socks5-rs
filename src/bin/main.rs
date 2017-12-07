extern crate mio;
extern crate socks5_rs;

use socks5_rs::thc;
use socks5_rs::thc::{FSM, Event, Return};

const LADDR: &'static str = "127.0.0.1:1080";

fn main() {
    let mut tcp_handler = thc::TcpHandler::new(4);

    let addr = LADDR.parse().unwrap();
    fn builder() -> Box<FSM> {
        let s = Socks5{state: 0};
        Box::new(s)
    };
    tcp_handler.register(&addr, builder);

    tcp_handler.run().unwrap();
}

struct Socks5 {
    state: usize,
}

impl FSM for Socks5 {
    fn init(&mut self) -> Return {
        self.state = 1;
        println!("socks5: init");
        Return::ReadExact(0, 2)
    }

    fn handle_event(&mut self, ev: Event) -> Return {
        println!("event: {:?}", ev);
        Return::ReadExact(0, 2)
    }
}
