extern crate mio;
extern crate bytes;

use std::net::SocketAddr;
use std::collections::HashMap;
use self::mio::{Poll, Events, Token}; // TODO self, ugh?
use self::mio::tcp::{TcpListener, TcpStream};
use self::bytes::{BytesMut, Bytes};

pub struct TcpHandler {
    poll: Poll,
    events: Events,
    next_token_index: usize, // for mio poll
    token_to_fsm_ref: HashMap<Token, (Ref, Ref)>,

    // token -> fsm_id, ref
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        let poll = Poll::new().unwrap();
        let mut events = Events::with_capacity(1024);

        TcpHandler{
            poll: poll,
            events: events,
            next_token_index: 0,
            token_to_fsm_ref: HashMap::new(),
        }
    }

    pub fn register<T: FSM>(&mut self, addr: &SocketAddr, fsm_builder: fn() -> T) {
        let listener = TcpListener::bind(addr).unwrap(); // TODO return err


    }

    pub fn run(&mut self) {

    }
}

type Ref = u64;

pub enum Event {
    Read(Ref, Bytes),
    Terminate(Ref),
    TerminateWithBuf(Ref, Bytes),
}

pub enum Return {
    Read(Ref),
    ReadExact(Ref, usize),
    ReadAndWrite(Ref, Bytes),
    ReadExactAndWrite(Ref, usize, Bytes),
    Terminate(Ref),
    TerminateAndWrite(Ref, Bytes),
    Register(Ref, TcpStream),
}

pub trait FSM {
    fn new() -> Self;
    fn handle_event(&self, ev: Event) -> Return;
}
