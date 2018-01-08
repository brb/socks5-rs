// TODO thc -> tcph
extern crate mio;
extern crate bytes;

use std::net::SocketAddr;
use std::cell::RefCell;
use std::rc::Rc;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write, Error, ErrorKind};
use self::mio::{Poll, Events, Token, PollOpt, Ready}; // TODO self, ugh?
use self::mio::tcp::{TcpListener, TcpStream};
use self::bytes::{BytesMut, Bytes, BufMut};

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
    fsm_conns: HashMap<ConnRef, FsmConn>,
    fsm: Rc<RefCell<Box<FSM>>>,
}

struct Acceptor {
    listener: TcpListener,
    spawn: fn() -> Box<FSM>,
}

#[derive(Clone, Copy)]
struct FsmConnId {
    main_token: Token, // accepted connection token; used to identify FsmState
    conn_ref: ConnRef,
}

pub struct TcpHandler {
    poll: Poll,
    next_token_index: RefCell<usize>, // for mio poll
    acceptors: HashMap<Token, RefCell<Acceptor>>,
    fsm_conn_ids: RefCell<HashMap<Token, FsmConnId>>,
    fsm_states: RefCell<HashMap<Token, FsmState>>,
    tokens: RefCell<HashMap<(Token, ConnRef), Token>>,
}

impl TcpHandler {
    pub fn new(_workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            poll: Poll::new().unwrap(),
            next_token_index: RefCell::new(0),
            acceptors: HashMap::new(),
            fsm_conn_ids: RefCell::new(HashMap::new()),
            fsm_states: RefCell::new(HashMap::new()),
            tokens: RefCell::new(HashMap::new()),
        }
    }

    pub fn register(&mut self, addr: &SocketAddr, spawn: fn() -> Box<FSM>) {
        let listener = TcpListener::bind(addr).unwrap();

        let token = self.get_token();

        self.poll.register(&listener, token, Ready::readable(), PollOpt::edge()).unwrap();
        self.acceptors.insert(token, RefCell::new(Acceptor{listener, spawn}));
    }

    fn get_token(&self) -> Token {
        let token = Token(*self.next_token_index.borrow());
        *self.next_token_index.borrow_mut() += 1;
        token
    }

    pub fn run(&mut self) -> Result<(), ()> {
        loop {
            let mut events = Events::with_capacity(1024);
            self.poll.poll(&mut events, None).unwrap();

            for event in &events {
                if let Some(acceptor) = self.acceptors.get(&event.token()) {
                    self.accept_connection(&acceptor.borrow());
                } else {
                    let fsm_conn_id;
                    {
                        let f = self.fsm_conn_ids.borrow();
                        fsm_conn_id = *f.get(&event.token()).unwrap();
                    }
                    let poll_reg;
                    {
                        let mut f = self.fsm_states.borrow_mut();
                        let fsm_state = f.get_mut(&fsm_conn_id.main_token).unwrap();
                        poll_reg = self.handle_poll_events(event.readiness(), fsm_conn_id.conn_ref, fsm_state);
                    }
                    self.poll_reregister(&poll_reg, fsm_conn_id.main_token);
                }
            }
        }
    }

    fn poll_reregister(&self, poll_reg: &PollReg, main_token: Token) {
        let mut fsm_conn_ids = self.fsm_conn_ids.borrow_mut();
        let mut tokens = self.tokens.borrow_mut();

        for cref in &poll_reg.new {
            let token = self.get_token();
            fsm_conn_ids.insert(token, FsmConnId{main_token, conn_ref: *cref});
            tokens.insert((main_token, *cref), token);

            let f = self.fsm_states.borrow();
            let fsm_conn = f.get(&main_token).unwrap().fsm_conns.get(cref).unwrap();
            // TODO s/readable/?/
            self.poll.register(&fsm_conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }

        for cref in &poll_reg.reregister {
            let f = self.fsm_states.borrow();
            let fsm_conn = f.get(&main_token).unwrap().fsm_conns.get(cref).unwrap();

            let mut ready = Ready::empty();
            if fsm_conn.read { ready = ready | Ready::readable(); }
            if fsm_conn.write { ready = ready | Ready::writable(); }

            let token;
            if *cref == 0 {
                token = main_token;
            } else {
                token = *tokens.get(&(main_token, *cref)).unwrap();
            }
            self.poll.reregister(&fsm_conn.socket, token, ready, PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }
    }

    fn accept_connection(&self, acceptor: &Acceptor) {
        let token = self.get_token();

        let (socket, _) = acceptor.listener.accept().unwrap();

        let fsm = Rc::new(RefCell::new((acceptor.spawn)()));

        let mut fsm_conn = FsmConn::new(socket);

        let mut fsm_state = FsmState{
            fsm_conns: HashMap::new(),
            fsm: fsm.clone(),
        };

        let fsm = fsm.clone();
        let mut fsm = fsm.borrow_mut();

        // Currently we support only the ReadExact(0, <..>) return
        if let Return::ReadExact(0, count) = fsm.init() {
            fsm_conn.req_read_count = Some(count);
            fsm_conn.read = true;
        } else {
            panic!("NYI");
        }

        fsm_state.fsm_conns.insert(0, fsm_conn);

        self.fsm_states.borrow_mut().insert(token, fsm_state);
        self.fsm_conn_ids.borrow_mut().insert(token, FsmConnId{main_token: token, conn_ref: 0});

        let fsm_state = self.fsm_states.borrow();
        let fsm_state = fsm_state.get(&token).unwrap();
        let fsm_conn = fsm_state.fsm_conns.get(&0).unwrap();

        // Because we support only ReadExact, we can register it here.
        // Order is important, as we do not want to get evented while FSM conn
        // is not stored in the hashmap ^^.
        self.poll.register(&fsm_conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
    }

    fn handle_poll_events(&self, ready: Ready, cref: ConnRef, fsm_state: &mut FsmState) -> PollReg {
        println!("handle_poll_events: {:?}", ready);

        let mut poll_reg = PollReg{new: Vec::new(), reregister: HashSet::new()};
        let mut returns = Vec::new();

        {
            let mut fsm_conn = fsm_state.fsm_conns.get_mut(&cref).unwrap();

            // Read from socket
            if ready.is_readable() {
                let socket = &mut fsm_conn.socket;
                let buf = &mut fsm_conn.read_buf;
                let (_, terminate) = read_until_would_block(socket, buf).unwrap();
                fsm_conn.read = false;
                if !terminate {
                    poll_reg.reregister.insert(cref);
                }
            }

            // Write to socket
            if ready.is_writable() && fsm_conn.write_buf.len() != 0 {
                let socket = &mut fsm_conn.socket;
                write_and_flush(socket, &mut fsm_conn.write_buf);
                fsm_conn.write_buf.clear();
                fsm_conn.write = false;
                poll_reg.reregister.insert(cref);
            }

            while let Some(c) = fsm_conn.req_read_count {
                let count;
                // Read(cref)
                if c == 0 && fsm_conn.read_buf.len() > 0 {
                    count = fsm_conn.read_buf.len();
                // ReadExact(cref)
                } else if c > 0  && fsm_conn.read_buf.len() >= c {
                    count = c;
                } else {
                    break;
                }

                let buf = fsm_conn.read_buf.split_to(count);
                let e = Event::Read(cref, buf.freeze());
                let fsm = fsm_state.fsm.clone();
                let mut fsm = fsm.borrow_mut();

                // reset
                fsm_conn.req_read_count = None;
                fsm_conn.read = false;

                let mut ret = fsm.handle_event(e);

                // process local read requests
                self.local_conn_reads(&mut ret, cref, &mut fsm_conn);

                returns.append(&mut ret);

            }
        }

        self.handle_fsm_return(returns, fsm_state, &mut poll_reg);

        return poll_reg;
    }

    // -> handle_local_read
    fn local_conn_reads(&self, returns: &mut Vec<Return>, cref: ConnRef, fsm_conn: &mut FsmConn) {
        for ret in returns {
            match ret {
                &mut Return::ReadExact(cr, count) if cr == cref && count <= fsm_conn.read_buf.len() => {
                    fsm_conn.req_read_count = Some(count);
                    *ret = Return::None;
                },
                &mut Return::Read(cr) if cr == cref && fsm_conn.read_buf.len() > 0 => {
                    fsm_conn.req_read_count = Some(0);
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
                    let mut fsm_conn = fsm_state.fsm_conns.get_mut(&cref).unwrap();
                    fsm_conn.req_read_count = Some(count);
                    fsm_conn.read = true;
                    poll_reg.reregister.insert(cref);
                },
                Return::Read(cref) => {
                    let mut fsm_conn = fsm_state.fsm_conns.get_mut(&cref).unwrap();
                    fsm_conn.req_read_count = Some(0);
                    fsm_conn.read = true;
                    poll_reg.reregister.insert(cref);
                },
                Return::Write(cref, reply) => {
                    let mut fsm_conn = fsm_state.fsm_conns.get_mut(&cref).unwrap();
                    fsm_conn.write_buf.put(reply);
                    fsm_conn.write = true;
                    poll_reg.reregister.insert(cref);
                },
                Return::Register(cref, socket) => {
                    let fc = FsmConn::new(socket);
                    fsm_state.fsm_conns.insert(cref, fc);
                    poll_reg.new.push(cref);
                },
            }
        }
    }
}

struct PollReg {
    new: Vec<ConnRef>,
    reregister: HashSet<ConnRef>,
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
// TODO change from Bytes to b[..]
pub enum Return {
    ReadExact(ConnRef, usize),
    Write(ConnRef, Bytes),
    Register(ConnRef, TcpStream),
    Read(ConnRef),
    None,
    //Terminate(ConnRef),
    //WriteAndTerminate(ConnRef, Bytes),
}

pub trait FSM {
    fn init(&mut self) -> Return; // TODO maybe vectorize as well?
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
