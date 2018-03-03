extern crate mio;
extern crate bytes;
extern crate workers;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write, Error, ErrorKind};
use mio::{Poll, Events, Token, PollOpt, Ready};
use mio::tcp::{TcpListener, TcpStream};
use bytes::{BytesMut, Bytes, BufMut};
use workers::WorkersPool;

#[derive(Debug)]
struct FsmConn {
    socket: TcpStream,
    read_buf: BytesMut,
    req_read_count: Option<usize>,
    read: bool,
    write_buf: BytesMut,
    write: bool,
}

impl FsmConn {
    fn new(socket: TcpStream) -> FsmConn {
        FsmConn{
            socket: socket,
            read_buf: BytesMut::with_capacity(64 * 1024),
            req_read_count: None,
            read: false,
            write_buf: BytesMut::with_capacity(64 * 1024),
            write: false,
        }
    }
}

struct FsmState {
    conns: HashMap<ConnRef, FsmConn>,
    fsm: Box<FSM>,
}

struct Acceptor {
    listener: TcpListener,
    spawn: fn() -> Box<FSM>,
}

#[derive(Clone, Copy)]
struct ConnId {
    main_token: Token, // accepted connection token; used to identify FsmState
    cref: ConnRef,
}

pub struct TcpHandler {
    workers: WorkersPool,
    poll: Poll,
    next_token_index: RefCell<usize>, // for mio poll
    acceptors: HashMap<Token, RefCell<Acceptor>>,
    conn_ids: RefCell<HashMap<Token, ConnId>>,
    tokens: RefCell<HashMap<(Token, ConnRef), Token>>,
    fsm_states: RefCell<HashMap<Token, FsmState>>,
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            workers: WorkersPool::new(workers_pool_size),
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),
            conn_ids: RefCell::new(HashMap::new()),
            fsm_states: RefCell::new(HashMap::new()),
            tokens: RefCell::new(HashMap::new()),
        }
    }

    pub fn register(&mut self, addr: &SocketAddr, spawn: fn() -> Box<FSM>) {
        let listener = TcpListener::bind(addr).unwrap();
        let token = self.get_token();

        // TODO Fix the order of statements below: we can get notified about
        //      a new listener before acceptor has been inserted.
        self.poll.register(&listener, token, Ready::readable(), PollOpt::edge()).unwrap();
        self.acceptors.insert(token, RefCell::new(Acceptor{listener, spawn}));
    }

   pub fn run(&mut self) -> Result<(), ()> {
        loop {
            let mut events = Events::with_capacity(1024);
            self.poll.poll(&mut events, None).unwrap();

            for event in &events {
                self.workers.exec(|| { println!("event") });
                if let Some(acceptor) = self.acceptors.get(&event.token()) {
                    self.accept_connection(&acceptor.borrow());
                } else {
                    let conn_id;
                    {
                        let f = self.conn_ids.borrow();
                        conn_id = *f.get(&event.token()).unwrap();
                    }
                    let poll_reg;
                    {
                        let mut f = self.fsm_states.borrow_mut();
                        let fsm_state = f.get_mut(&conn_id.main_token).unwrap();
                        poll_reg = self.handle_poll_events(event.readiness(), conn_id.cref, fsm_state);
                    }
                    self.poll_reregister(&poll_reg, conn_id.main_token);
                }


            }
        }
    }

    fn accept_connection(&self, acceptor: &Acceptor) {
        let (socket, _) = acceptor.listener.accept().unwrap();
        let token = self.get_token();

        // Create a FSM instance
        let fsm = (acceptor.spawn)();
        let mut conn = FsmConn::new(socket);
        let mut fsm_state = FsmState{
            conns: HashMap::new(),
            fsm: fsm,
        };

        // Currently, only ReadExact(0, <..>) is supported in fsm.init() return
        if let Return::ReadExact(0, count) = fsm_state.fsm.init() {
            conn.req_read_count = Some(count);
            conn.read = true;
        } else {
            panic!("NYI");
        }

        fsm_state.conns.insert(0, conn);

        self.fsm_states.borrow_mut().insert(token, fsm_state);
        self.conn_ids.borrow_mut().insert(token, ConnId{main_token: token, cref: 0});

        // Dirty hack to get a reference to the socket moved above ^^.
        let fsm_state = self.fsm_states.borrow();
        let fsm_state = fsm_state.get(&token).unwrap();
        let conn = fsm_state.conns.get(&0).unwrap();

        // Because of the fsm.init() return limitation, we register the socket as readable.
        self.poll.register(&conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn handle_poll_events(&self, ready: Ready, cref: ConnRef, fsm_state: &mut FsmState) -> PollReg {
        let mut poll_reg = PollReg{new: Vec::new(), old: HashSet::new()};
        let mut returns = Vec::new();
        let mut terminate = false;

        {
            let mut conn = fsm_state.conns.get_mut(&cref).unwrap();

            if ready.is_readable() {
                let socket = &mut conn.socket;
                let buf = &mut conn.read_buf;
                let (_, t) = read_until_would_block(socket, buf).unwrap();
                terminate = t;
                conn.read = false;
                if !terminate {
                    poll_reg.old.insert(cref);
                }
            }

            if ready.is_writable() && conn.write_buf.len() != 0 {
                let socket = &mut conn.socket;
                write_and_flush(socket, &mut conn.write_buf);
                conn.write_buf.clear();
                conn.write = false;
                poll_reg.old.insert(cref);
            }

            if terminate {
                let len = conn.read_buf.len();
                let buf = conn.read_buf.split_to(len);
                conn.req_read_count = None;
                conn.read = false;
                let e = Event::Terminate(cref, buf.freeze());
                let mut ret = fsm_state.fsm.handle_event(e);
                returns.append(&mut ret);
            } else {
                while let Some(c) = conn.req_read_count {
                    let count;
                    // Read(cref)
                    if c == 0 && conn.read_buf.len() > 0 {
                        count = conn.read_buf.len();
                    // ReadExact(cref)
                    } else if c > 0  && conn.read_buf.len() >= c {
                        count = c;
                    } else {
                        break;
                    }

                    let buf = conn.read_buf.split_to(count);

                    // reset
                    conn.req_read_count = None;
                    conn.read = false;

                    let e = Event::Read(cref, buf.freeze());
                    let mut ret = fsm_state.fsm.handle_event(e);

                    // process local read requests
                    self.local_conn_reads(&mut ret, cref, &mut conn);

                    returns.append(&mut ret);
                }
            }
        }

        self.handle_fsm_return(returns, fsm_state, &mut poll_reg);

        return poll_reg;
    }

    fn poll_reregister(&self, poll_reg: &PollReg, main_token: Token) {
        let mut conn_ids = self.conn_ids.borrow_mut();
        let mut tokens = self.tokens.borrow_mut();

        for cref in &poll_reg.new {
            let token = self.get_token();
            conn_ids.insert(token, ConnId{main_token, cref: *cref});
            tokens.insert((main_token, *cref), token);

            let f = self.fsm_states.borrow();
            let conn = f.get(&main_token).unwrap().conns.get(cref).unwrap();
            // TODO s/readable/?/
            self.poll.register(&conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }

        for cref in &poll_reg.old {
            let f = self.fsm_states.borrow();
            let conn = f.get(&main_token).unwrap().conns.get(cref).unwrap();

            let mut ready = Ready::empty();
            if conn.read { ready = ready | Ready::readable(); }
            if conn.write { ready = ready | Ready::writable(); }

            let token;
            if *cref == 0 {
                token = main_token;
            } else {
                token = *tokens.get(&(main_token, *cref)).unwrap();
            }
            self.poll.reregister(&conn.socket, token, ready, PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }
    }



    // -> handle_local_read
    fn local_conn_reads(&self, returns: &mut Vec<Return>, cref: ConnRef, conn: &mut FsmConn) {
        for ret in returns {
            match ret {
                &mut Return::ReadExact(cr, count) if cr == cref && count <= conn.read_buf.len() => {
                    conn.req_read_count = Some(count);
                    *ret = Return::None;
                },
                &mut Return::Read(cr) if cr == cref && conn.read_buf.len() > 0 => {
                    conn.req_read_count = Some(0);
                    *ret = Return::None;
                },
                _ => {},
            }
        }
    }

    fn handle_fsm_return(&self, returns: Vec<Return>, fsm_state: &mut FsmState, poll_reg: &mut PollReg) {
        for ret in returns {
            match ret {
                Return::None => {},
                Return::ReadExact(cref, count) => {
                    let mut conn = fsm_state.conns.get_mut(&cref).unwrap();
                    conn.req_read_count = Some(count);
                    conn.read = true;
                    poll_reg.old.insert(cref);
                },
                Return::Read(cref) => {
                    let mut conn = fsm_state.conns.get_mut(&cref).unwrap();
                    conn.req_read_count = Some(0);
                    conn.read = true;
                    poll_reg.old.insert(cref);
                },
                Return::Write(cref, reply) => {
                    let mut conn = fsm_state.conns.get_mut(&cref).unwrap();
                    conn.write_buf.put(reply);
                    conn.write = true;
                    poll_reg.old.insert(cref);
                },
                Return::Register(cref, socket) => {
                    let fc = FsmConn::new(socket);
                    fsm_state.conns.insert(cref, fc);
                    poll_reg.new.push(cref);
                },
            }
        }
    }

    fn get_token(&self) -> Token {
        let token = Token(*self.next_token_index.borrow());
        *self.next_token_index.borrow_mut() += 1;
        token
    }
}

struct PollReg {
    new: Vec<ConnRef>,
    old: HashSet<ConnRef>,
}

type ConnRef = u64;

#[derive(Debug)]
pub enum Event {
    None,
    Read(ConnRef, Bytes),
    Terminate(ConnRef, Bytes),
}

#[derive(Debug)]
// TODO change from Bytes to b[..]
pub enum Return {
    ReadExact(ConnRef, usize),
    Write(ConnRef, Bytes),
    Register(ConnRef, TcpStream),
    Read(ConnRef),
    None, // For internal usage only
    //Terminate(ConnRef), // TODO
    //WriteAndTerminate(ConnRef, Bytes), // TODO
}

pub trait FSM {
    fn init(&mut self) -> Return;
    fn handle_event(&mut self, ev: Event) -> Vec<Return>;
}

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