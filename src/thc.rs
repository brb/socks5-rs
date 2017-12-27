// TODO .clone -> Rc::clone
extern crate mio;
extern crate bytes;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::HashMap;
use std::io::{Read, Write, Error, ErrorKind};
use self::mio::{Poll, Events, Token, PollOpt, Ready}; // TODO self, ugh?
use self::mio::tcp::{TcpListener, TcpStream};
use self::bytes::{BytesMut, Bytes, BufMut};

struct FsmConn {
    conn_ref: ConnRef,
    socket: Rc<TcpStream>,
    read_buf: BytesMut,
    req_read_count: usize,
    read: bool,
    write_buf: BytesMut,
    write: bool,
    fsm: Rc<RefCell<Box<FSM>>>,
    // ref -> FsmConn
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

    fsm_conns: RefCell<HashMap<Token, FsmConn>>,
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),
            fsm_conns: RefCell::new(HashMap::new()),
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
                } else if let Some(fsm_conn) = self.fsm_conns.borrow_mut().get_mut(&event.token()) {
                    self.handle_poll_events(event.readiness(), event.token(), fsm_conn);
                }
            }
        }
    }

    fn accept_connection(&self, acceptor: &Acceptor) {
        let (socket, _) = acceptor.listener.accept().unwrap();

        let token = Token(*self.next_token_index.borrow());
        *self.next_token_index.borrow_mut() += 1;

        let fsm = Rc::new(RefCell::new((acceptor.spawn)()));
        let socket = Rc::new(socket);

        let mut fsm_conn = FsmConn{
            conn_ref: 0, // indicates the main connection
            socket: Rc::clone(&socket),
            read_buf: BytesMut::with_capacity(64 * 1024),
            req_read_count: 0,
            read: false,
            write_buf: BytesMut::with_capacity(64 * 1024),
            write: false,
            fsm: fsm.clone(),
        };

        let fsm = fsm.clone();
        let mut fsm = fsm.borrow_mut();

        // Currently we support only the ReadExact(0, <..>) return
        if let Return::ReadExact(0, count) = fsm.init() {
            fsm_conn.req_read_count = count;
            fsm_conn.read = true;
        } else {
            panic!("NYI");
        }

        self.fsm_conns.borrow_mut().insert(token, fsm_conn);
        // Because we support only ReadExact, we can register it here.
        // Order is important, as we do not want to get evented while FSM conn
        // is not stored in the hashmap ^^.
        self.poll.register(&*socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn handle_poll_events(&self, ready: Ready, token: Token, fsm_conn: &mut FsmConn) {
        println!("handle_poll_events: {:?}", ready);

        // Read from socket
        if ready.is_readable() {
            let socket = Rc::get_mut(&mut fsm_conn.socket).unwrap();
            let (_, terminate) = read_until_would_block(socket, &mut fsm_conn.read_buf).unwrap();
            assert!(!terminate); // TODO NYI
            fsm_conn.read = false;
        }

        // Write to socket
        if ready.is_writable() && fsm_conn.write_buf.len() != 0 {
            let socket = Rc::get_mut(&mut fsm_conn.socket).unwrap();
            write_and_flush(socket, &mut fsm_conn.write_buf);
            fsm_conn.write_buf.clear();
            fsm_conn.write = false;
        }

        while fsm_conn.req_read_count != 0 && fsm_conn.read_buf.len() >= fsm_conn.req_read_count {
                let buf = fsm_conn.read_buf.split_to(fsm_conn.req_read_count);
                let e = Event::Read(fsm_conn.conn_ref, buf.freeze());
                let fsm = fsm_conn.fsm.clone();
                let mut fsm = fsm.borrow_mut();
                fsm_conn.req_read_count = 0;
                let ret = fsm.handle_event(e);
                self.handle_fsm_return(ret, token, fsm_conn);
        }

        self.poll_reregister(fsm_conn.read, fsm_conn.write, &*fsm_conn.socket, token);
    }

    fn handle_fsm_return(&self, ret: Return, token: Token, fsm_conn: &mut FsmConn) {
        match ret {
            Return::ReadExact(conn_ref, count) => {
                assert_eq!(conn_ref, fsm_conn.conn_ref);

                fsm_conn.req_read_count = count;
                fsm_conn.read = true;
            },
            Return::WriteAndReadExact(write_conn_ref, msg, read_conn_ref, count) => {
                assert_eq!(write_conn_ref, fsm_conn.conn_ref);
                assert_eq!(read_conn_ref, fsm_conn.conn_ref);

                fsm_conn.req_read_count = count;
                fsm_conn.read = true;
                fsm_conn.write_buf.put(msg);
                fsm_conn.write = true;
            },
        }
    }

    fn poll_reregister(&self, read: bool, write: bool, socket: &TcpStream, token: Token) {
        let mut ready = Ready::empty();
        if read { ready = ready | Ready::readable(); }
        if write { ready = ready | Ready::writable(); }
        self.poll.reregister(socket, token, ready, PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }
}

type ConnRef = u64;

#[derive(Debug)]
pub enum Event {
    None,
    Read(ConnRef, Bytes),
    Terminate(ConnRef),
    TerminateWithBuf(ConnRef, Bytes),
}

#[derive(Debug)]
// TODO maybe support multiple Returns (and hence, s/Return/Action)
pub enum Return {
    //Read(ConnRef),
    ReadExact(ConnRef, usize),
    //WriteAndRead(ConnRef, Bytes),
    // TODO change from Bytes to b[..]
    WriteAndReadExact(ConnRef, Bytes, ConnRef, usize),
    //Terminate(ConnRef),
    //WriteAndTerminate(ConnRef, Bytes),
    //Register(ConnRef, TcpStream),
}

pub trait FSM {
    fn init(&mut self) -> Return;
    fn handle_event(&mut self, ev: Event) -> Return;
}

// ---

fn read_until_would_block(src: &mut Read, buf: &mut BytesMut) -> Result<(usize, bool), Error> {
    let mut total_size = 0;
    let mut tmp_buf = [0; 64];

    loop {
        match src.read(&mut tmp_buf) {
            Ok(0) => {
                return Ok((total_size, true));
            },
            Ok(size) => {
                buf.put(&tmp_buf[0 .. size]);
                total_size += size;
            },
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    // socket is exhausted
                    break;
                } else {
                    return Err(err);
                }
            }
        }
    }

    return Ok((total_size, false));
}

fn write_and_flush(dst: &mut Write, buf: &[u8]) {
    let size = dst.write(&buf).unwrap();
    assert_eq!(size, buf.len());
    dst.flush().unwrap();
}
