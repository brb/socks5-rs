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

struct FsmState {
    fsm_conns: HashMap<ConnRef, FsmConn>,
    fsm: Rc<RefCell<Box<FSM>>>,
}

struct FsmConn {
    socket: Rc<TcpStream>,
    read_buf: BytesMut,
    req_read_count: usize,
    read: bool, // TODO -1 to indicate ReadNonExact
    write_buf: BytesMut,
    write: bool,
}

struct Acceptor {
    listener: TcpListener,
    spawn: fn() -> Box<FSM>,
}

struct FsmConnId {
    main_token: Token, // acceptor token; used to identify FsmState
    conn_ref: ConnRef,
}

pub struct TcpHandler {
    poll: Poll,
    next_token_index: RefCell<usize>, // for mio poll
    acceptors: HashMap<Token, RefCell<Acceptor>>,

    fsm_conn_ids: RefCell<HashMap<Token, FsmConnId>>,
    fsm_states: RefCell<HashMap<Token, FsmState>>,

    // poll event:
    // 1. fsm_conn_ids[token] -> (main_token, conn_ref) // <-- register (token+1, hashmap)
    // 2. fsms[main_token] -> fsm
    // 3. fsm.conns[conn_ref] -> (socket, write_buf, read_buf)
    //
    // fsm return:
    // 1. fsm[conn_ref] -> socket
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),
            fsm_conn_ids: RefCell::new(HashMap::new()),
            fsm_states: RefCell::new(HashMap::new()),
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
                // Otherwise, we need to get FsmConn.
                } else if let Some(fsm_conn_id) = self.fsm_conn_ids.borrow().get(&event.token()) {
                    if let Some(ref mut fsm_state) = self.fsm_states.borrow_mut().get_mut(&fsm_conn_id.main_token) {
                        self.handle_poll_events(event.readiness(), event.token(), fsm_conn_id, fsm_state);
                        //if let Some(ref fsm_conn) = fsm_state.fsm_conns.get_mut(&fsm_conn_id.conn_ref) {
                        //    // TODO maybe pass fsm_state instead of fsm_conn?
                        //    self.handle_poll_events(event.readiness(), event.token(), fsm_conn);
                        //}
                    }
                }
                //else if let Some(fsm_conn) = self.fsm_conns.borrow_mut().get_mut(&event.token()) {
                //}
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
            socket: Rc::clone(&socket),
            read_buf: BytesMut::with_capacity(64 * 1024),
            req_read_count: 0,
            read: false,
            write_buf: BytesMut::with_capacity(64 * 1024),
            write: false,
        };

        let mut fsm_state = FsmState{
            fsm_conns: HashMap::new(),
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

        fsm_state.fsm_conns.insert(0, fsm_conn);

        self.fsm_states.borrow_mut().insert(token, fsm_state);
        self.fsm_conn_ids.borrow_mut().insert(token, FsmConnId{main_token: token, conn_ref: 0});

        // Because we support only ReadExact, we can register it here.
        // Order is important, as we do not want to get evented while FSM conn
        // is not stored in the hashmap ^^.
        self.poll.register(&*socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn handle_poll_events(&self, ready: Ready, token: Token, fsm_conn_id: &FsmConnId, fsm_state: &mut FsmState) {
        println!("handle_poll_events: {:?}", ready);

        let mut fsm_conn;
        if let Some(f) = fsm_state.fsm_conns.get_mut(&fsm_conn_id.conn_ref) {
            fsm_conn = f;
        } else {
            panic!("not found");
        }

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
                let e = Event::Read(fsm_conn_id.conn_ref, buf.freeze());
                let fsm = fsm_state.fsm.clone();
                let mut fsm = fsm.borrow_mut();
                fsm_conn.req_read_count = 0;
                let ret = fsm.handle_event(e);
                // TODO return vec![conn_ref] to indicate which sockets should be reregistered
                self.handle_fsm_return(ret, token, fsm_conn);
        }

        self.poll_reregister(fsm_conn.read, fsm_conn.write, &*fsm_conn.socket, token);
    }

    fn handle_fsm_return(&self, ret: Vec<Return>, token: Token, fsm_conn: &mut FsmConn) {
        for r in ret {
            match r {
                Return::ReadExact(conn_ref, count) => {
                    //assert_eq!(conn_ref, fsm_conn.conn_ref);
                    fsm_conn.req_read_count = count;
                    fsm_conn.read = true;
                },
                Return::Write(write_conn_ref, reply) => {
                    //assert_eq!(write_conn_ref, fsm_conn.conn_ref);
                    fsm_conn.write_buf.put(reply);
                    fsm_conn.write = true;
                },
                Return::Register(conn_ref, socket) => {
                    // 1. Get token
                    // 2. Insert into self.conn_maps
                    panic!("start here");
                },
            }
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
    TerminateAfterRead(ConnRef, Bytes),
}

#[derive(Debug)]
// TODO maybe support multiple Returns (and hence, s/Return/Action)
// TODO change from Bytes to b[..]
pub enum Return {
    ReadExact(ConnRef, usize),
    Write(ConnRef, Bytes),
    Register(ConnRef, TcpStream),
    //Terminate(ConnRef),
    //Read(ConnRef),
    //WriteAndTerminate(ConnRef, Bytes),
}

pub trait FSM {
    fn init(&mut self) -> Return; // TODO maybe vectorize as well?
    fn handle_event(&mut self, ev: Event) -> Vec<Return>;
}

// --- Helpers

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
