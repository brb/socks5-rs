extern crate mio;
extern crate bytes;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use self::mio::{Poll, Events, Token, PollOpt, Ready}; // TODO self, ugh?
use self::mio::tcp::{TcpListener, TcpStream};
use self::bytes::{BytesMut, Bytes};

struct FsmConn {
    conn_ref: Ref,
    read_buf: BytesMut,
    write_buf: BytesMut,
    fsm: Rc<RefCell<Box<FSM>>>,
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

    refs: RefCell<HashMap<Token, Ref>>,
    fsms: RefCell<HashMap<Token, FsmConn>>,
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),

            refs: RefCell::new(HashMap::new()),
            fsms: RefCell::new(HashMap::new()),
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
                } else { //if let Some(fsm) = self.fsms.get(&event.token()) {
                    panic!("nyi");
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

        let fsm = Rc::new(RefCell::new((acceptor.spawn)()));

        //self.refs.borrow_mut().insert(token, 0);
        let fsm_conn = FsmConn{
            conn_ref: 0,
            read_buf: BytesMut::with_capacity(64 * 1024),
            write_buf: BytesMut::with_capacity(64 * 1024),
            fsm: fsm.clone(),
        };
        self.fsms.borrow_mut().insert(token, fsm_conn);

        let fsm = fsm.clone();
        let mut fsm = fsm.borrow_mut();
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
    //Read(Ref),
    ReadExact(Ref, usize),
    //ReadAndWrite(Ref, Bytes),
    //ReadExactAndWrite(Ref, usize, Bytes),
    //Terminate(Ref),
    //TerminateAndWrite(Ref, Bytes),
    //Register(Ref, TcpStream),
}

pub trait FSM {
    fn init(&mut self) -> Return;
    fn handle_event(&mut self, ev: Event) -> Return;
}
