extern crate mio;
extern crate bytes;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::collections::HashMap;
use self::mio::{Poll, Events, Token, PollOpt, Ready}; // TODO self, ugh?
use self::mio::tcp::{TcpListener, TcpStream};
use self::bytes::{BytesMut, Bytes};

struct FsmState {

}

struct Acceptor {
    listener: TcpListener,
    spawn: fn() -> Box<FSM>,
}

pub struct TcpHandler {
    poll: Poll,
    //events: Events,
    next_token_index: RefCell<usize>, // for mio poll

    acceptors: HashMap<Token, RefCell<Acceptor>>,

    fsms: HashMap<Token, Box<FSM>>,

    // token -> fsm_id, ref
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),

            //token_to_fsm_ref: HashMap::new(),
            fsms: HashMap::new(),
        }
    }

    pub fn register(&mut self, addr: &SocketAddr, spawn: fn() -> Box<FSM>) {
        let listener = TcpListener::bind(addr).unwrap();

        let token = Token(*self.next_token_index.borrow());
        *self.next_token_index.borrow_mut() += 1;

        self.poll.register(&listener, token, Ready::readable(), PollOpt::edge()).unwrap();
        self.acceptors.insert(token, RefCell::new(Acceptor{listener, spawn}));
    }

    pub fn run(&mut self) -> Result<(), ()> {
        loop {
            let mut events = Events::with_capacity(1024);
            self.poll.poll(&mut events, None).unwrap();

            for event in &events {
                if let Some(acceptor) = self.acceptors.get(&event.token()) {
                    self.accept_connection(&acceptor.borrow());
                } else if let Some(fsm) = self.fsms.get(&event.token()) {
                    // TODO which Ref?

                }
            }
        }
    }

    fn accept_connection(&self, acceptor: &Acceptor) {
        let (socket, _) = acceptor.listener.accept().unwrap();

        let token = Token(*self.next_token_index.borrow());
        *self.next_token_index.borrow_mut() += 1;

        self.poll.register(&socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();

        let mut fsm = (acceptor.spawn)();

        //self.fsms.insert(token, fsm);

        let ret = fsm.init();
        self.handle_ret(ret).unwrap();
    }

    fn handle_ret(&self, ret: Return) -> Result<(), ()> {
        Ok(())
    }
}

type Ref = u64;

#[derive(Debug)]
pub enum Event {
    Read(Ref, Bytes),
    Terminate(Ref),
    TerminateWithBuf(Ref, Bytes),
}

#[derive(Debug)]
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
    fn init(&mut self) -> Return;
    fn handle_event(&mut self, ev: Event) -> Return;
}
