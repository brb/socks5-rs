extern crate mio;
extern crate bytes;
extern crate workers;

use std::net::SocketAddr;
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write, Error, ErrorKind};
use std::sync::{mpsc, Arc, Mutex};
use mio::{Poll, Events, Token, PollOpt, Ready};
use mio::tcp::{TcpListener, TcpStream};
use bytes::{BytesMut, Bytes, BufMut};
use workers::WorkersPool;
use std::vec::Vec;

/// A mio token for notifying the dispatcher thread about poll registration requests
/// from worker threads.
const REG_TOKEN: Token = Token(0);

/// Unique (inside FSM) identifier for a TCP connection.
///
/// The initiating (first) connection is always `ConnRef(0)`.
type ConnRef = u64;

/// State of an FSM instance.
struct FsmState {
    fsm: Box<FSM>,
    conns: HashMap<ConnRef, FsmConn>,
}

/// Represents a TCP connection of an FSM instance.
#[derive(Debug)]
struct FsmConn {
    socket: TcpStream,
    /// The FSM requested to read from the socket.
    read: bool,
    /// How many bytes to read from the socket.
    read_count: Option<usize>,
    read_buf: BytesMut,
    /// The FSM requested to write to the socket.
    write: bool,
    write_buf: BytesMut,
}

impl FsmConn {
    fn new(socket: TcpStream) -> FsmConn {
        FsmConn{
            socket: socket,
            read: false,
            read_count: None,
            // TODO Handle overflows, as BytesMut does not grow dynamically.
            read_buf: BytesMut::with_capacity(64 * 1024),
            write: false,
            // TODO Handle overflows, as BytesMut does not grow dynamically.
            write_buf: BytesMut::with_capacity(64 * 1024),
        }
    }
}

/// Represents TCP listener which spawns an FSM upon a new connection.
struct Acceptor {
    listener: TcpListener,
    spawn: fn() -> Box<FSM>,
}

/// Global TCP connection identifier.
#[derive(Clone, Copy)]
struct GlobalConnRef {
    /// Token of an accepted by TCP listener connection.
    ///
    /// Token can be used to identify an instance of FSM.
    main_token: Token,
    /// Local TCP connection identifier.
    cref: ConnRef,
}

/// FSM trait

pub trait FSM: Send + Sync {
    fn init(&mut self) -> Return;
    fn handle_event(&mut self, ev: Event) -> Vec<Return>;
}

#[derive(Debug)]
pub enum Event {
    None,
    Read(ConnRef, Bytes),
    Terminate(ConnRef, Bytes),
}

#[derive(Debug)]
// TODO Bytes -> b[..]
pub enum Return {
    // TODO Do not allow FSM to return None; for internal use only.
    None,
    ReadExact(ConnRef, usize),
    Write(ConnRef, Bytes),
    Register(ConnRef, TcpStream),
    Read(ConnRef),
    //Terminate(ConnRef),
    //WriteAndTerminate(ConnRef, Bytes),
}

// ---------------------------------

struct TcpHandlerInner {
    token_index: usize,
    conn_ids: HashMap<Token, GlobalConnRef>,
    tokens: HashMap<(Token, ConnRef), Token>,
}

pub struct TcpHandler {
    workers: WorkersPool,
    poll: Poll,
    inner: TcpHandlerInner,
    fsm_states: HashMap<Token, Arc<Mutex<FsmState>>>,
    acceptors: HashMap<Token, Arc<Mutex<Acceptor>>>,
}

// ----------------------------------

// TODO main_token -> ?
// TODO new -> register, old -> reregister ?
struct HandleEventsResult {
    main_token: Token,
    new: Vec<ConnRef>,
    old: HashSet<ConnRef>,
}

// TODO A -> ?, H -> ?
enum RegReq {
    New(Vec<FsmState>),
    Existing(HandleEventsResult),
}

// ----------------------------------

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler{
            workers: WorkersPool::new(workers_pool_size),
            poll: Poll::new().unwrap(),
            fsm_states: HashMap::new(),
            acceptors: HashMap::new(),
            inner: TcpHandlerInner::new(),
        }
    }

    pub fn register(&mut self, addr: &SocketAddr, spawn: fn() -> Box<FSM>) {
        let listener = TcpListener::bind(addr).unwrap();
        let token = self.inner.get_token();

        // XXX Possible race window (if `register` is allowed to be called after `run`):
        //     we can get notified about a new listener before the acceptor has
        //     been inserted.
        self.poll.register(&listener, token, Ready::readable(), PollOpt::edge()).unwrap();
        self.acceptors.insert(token, Arc::new(Mutex::new(Acceptor{listener, spawn})));
    }

    pub fn run(&mut self) {
        let mut events = Events::with_capacity(1024);

        let (reg_tx, reg_rx) = mpsc::channel::<RegReq>();
        let (reg, set_ready) = mio::Registration::new2();
        self.poll.register(&reg, REG_TOKEN, Ready::readable(), PollOpt::edge()).unwrap();

        loop {
            self.poll.poll(&mut events, None).unwrap();

            for event in &events {
                if event.token() == REG_TOKEN {
                    while let Ok(req) = reg_rx.try_recv() {
                        match req {
                            RegReq::New(fsm_states) =>
                                self.register_new_fsms(fsm_states),
                            RegReq::Existing(h) => {
                                let fsm_state = Arc::clone(&self.fsm_states.get(&h.main_token).unwrap());
                                let fsm_state = fsm_state.lock().unwrap();
                                self.register_conns(&h.new, &h.old, h.main_token, &fsm_state);
                            }
                        }
                    }
                } else if let Some(acceptor) = self.acceptors.get(&event.token()) {
                    let reg_tx = reg_tx.clone();
                    let set_ready = set_ready.clone();
                    let acceptor = acceptor.clone();

                    self.workers.exec(move || {
                        let acceptor = acceptor.lock().unwrap();
                        let fsm_states = accept_connections(&acceptor);
                        reg_tx.send(RegReq::New(fsm_states)).unwrap();
                        set_ready.set_readiness(Ready::readable()).unwrap();
                    });
                } else {
                    let reg_tx = reg_tx.clone();
                    let set_ready = set_ready.clone();
                    let conn_id = *(self.inner.conn_ids.get(&event.token()).unwrap());
                    let fsm_state = self.fsm_states.get(&conn_id.main_token).unwrap();
                    let fsm_state = fsm_state.clone();

                    self.workers.exec(move || {
                        let fsm_state = &mut *(fsm_state.lock().unwrap());
                        let mut r = handle_events(event.readiness(), conn_id.cref, fsm_state);
                        r.main_token = conn_id.main_token;
                        reg_tx.send(RegReq::Existing(r)).unwrap();
                        set_ready.set_readiness(Ready::readable()).unwrap();
                    });
                }
            }
        }
    }

    fn register_new_fsms(&mut self, fsm_states: Vec<FsmState>) {
        for fsm_state in fsm_states {
            let token = self.inner.get_token();
            let fsm_state = Arc::new(Mutex::new(fsm_state));

            self.fsm_states.insert(token, Arc::clone(&fsm_state));
            self.inner.conn_ids.insert(
                token,
                GlobalConnRef{main_token: token, cref: 0}
            );

            // Dirty hack to get a reference to the socket moved above ^^.
            let fsm_state = fsm_state.lock().unwrap();
            let conn = fsm_state.conns.get(&0).unwrap();
            self.poll.register(&conn.socket, token,
                Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }
    }

    fn register_conns(&mut self, new: &Vec<ConnRef>, old: &HashSet<ConnRef>, main_token: Token, fsm_state: &FsmState) {
        for cref in new {
            let token = self.inner.get_token();
            self.inner.conn_ids.insert(token, GlobalConnRef{main_token, cref: *cref});
            self.inner.tokens.insert((main_token, *cref), token);

            let conn = (*fsm_state).conns.get(cref).unwrap();
            self.poll.register(&conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }

        for cref in old {
            let conn = (*fsm_state).conns.get(cref).unwrap();

            let mut ready = Ready::empty();
            if conn.read { ready = ready | Ready::readable(); }
            if conn.write { ready = ready | Ready::writable(); }

            let token;
            if *cref == 0 {
                token = main_token;
            } else {
                token = *self.inner.tokens.get(&(main_token, *cref)).unwrap();
            }

            self.poll.reregister(&conn.socket, token, ready, PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }
    }
}

impl TcpHandlerInner {
    fn new() -> TcpHandlerInner {
        TcpHandlerInner{
            token_index: 0,
            conn_ids: HashMap::new(),
            tokens: HashMap::new(),
        }
    }

    fn get_token(&mut self) -> Token {
        self.token_index += 1;
        Token(self.token_index)
    }
}

fn accept_connections(acceptor: &Acceptor) -> Vec<FsmState> {
    let mut states = Vec::new();

    loop {
        match acceptor.listener.accept() {
            Err(err) => {
                if err.kind() == ErrorKind::WouldBlock {
                    break;
                } else {
                    panic!("accept_connections: {:?}", err);
                }
            }
            Ok((socket, _)) => {
                // Create an FSM instance
                let fsm = (acceptor.spawn)();
                let mut conn = FsmConn::new(socket);
                let mut fsm_state = FsmState{
                    conns: HashMap::new(),
                    fsm: fsm,
                };

                // Only `ReadExact` can be returned by `fsm.init()` atm
                if let Return::ReadExact(0, count) = fsm_state.fsm.init() {
                    conn.read_count = Some(count);
                    conn.read = true;
                } else {
                    panic!("NYI");
                }

                fsm_state.conns.insert(0, conn);
                states.push(fsm_state);
            }
        }
    }

    states
}

//////////////////// START REF /////////////////////////////

fn handle_events(ready: Ready, cref: ConnRef, fsm_state: &mut FsmState) -> HandleEventsResult {
        let mut poll_reg = HandleEventsResult{new: Vec::new(), old: HashSet::new(), main_token: Token(0)};
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
                conn.read_count = None;
                conn.read = false;
                let e = Event::Terminate(cref, buf.freeze());
                println!("terminate: {:?}", dbg_token);
                let mut ret = fsm_state.fsm.handle_event(e);
                returns.append(&mut ret);
            } else {
                while let Some(c) = conn.read_count {
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
                    conn.read_count = None;
                    conn.read = false;

                    let e = Event::Read(cref, buf.freeze());
                    println!("fsm.handle_event: {:?}", dbg_token);
                    let mut ret = fsm_state.fsm.handle_event(e);

                    // process local read requests
                    local_conn_reads(&mut ret, cref, &mut conn);

                    returns.append(&mut ret);
                }
            }
        }

        handle_fsm_return(returns, fsm_state, &mut poll_reg);

        println!("handle_events: exit");

        return poll_reg;
    }

// -> handle_local_read
fn local_conn_reads(returns: &mut Vec<Return>, cref: ConnRef, conn: &mut FsmConn) {
    for ret in returns {
        match ret {
            &mut Return::ReadExact(cr, count) if cr == cref && count <= conn.read_buf.len() => {
                conn.read_count = Some(count);
                *ret = Return::None;
            },
            &mut Return::Read(cr) if cr == cref && conn.read_buf.len() > 0 => {
                conn.read_count = Some(0);
                *ret = Return::None;
            },
            _ => {},
        }
    }
}

fn handle_fsm_return(returns: Vec<Return>, fsm_state: &mut FsmState, poll_reg: &mut HandleEventsResult) {
    for ret in returns {
        match ret {
            Return::None => {},
            Return::ReadExact(cref, count) => {
                let mut conn = fsm_state.conns.get_mut(&cref).unwrap();
                conn.read_count = Some(count);
                conn.read = true;
                poll_reg.old.insert(cref);
            },
            Return::Read(cref) => {
                let mut conn = fsm_state.conns.get_mut(&cref).unwrap();
                conn.read_count = Some(0);
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

//////////////////// END REF /////////////////////////////

/// Helpers

fn read_until_would_block(src: &mut Read, buf: &mut BytesMut) ->
                                            Result<(usize, bool), Error> {
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
