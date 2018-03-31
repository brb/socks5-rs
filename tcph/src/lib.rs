extern crate bytes;
extern crate mio;
extern crate workers;

use std::net::SocketAddr;
use std::collections::{HashMap, HashSet};
use std::io::{Error, ErrorKind, Read, Write};
use std::sync::{mpsc, Arc, Mutex};
use mio::{Events, Poll, PollOpt, Ready, Token};
use mio::tcp::{TcpListener, TcpStream};
use bytes::{BufMut, Bytes, BytesMut};
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
    /// How many bytes to read from the socket.
    read_count: Option<usize>,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl FsmConn {
    fn new(socket: TcpStream) -> FsmConn {
        FsmConn {
            socket: socket,
            read_count: None,
            // TODO Handle overflows, as BytesMut does not grow dynamically.
            read_buf: BytesMut::with_capacity(64 * 1024),
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
    fsm_token: Token,
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
    // TODO Replace None with Optional<T>
    None,
    ReadExact(ConnRef, usize),
    Write(ConnRef, Bytes),
    Register(ConnRef, TcpStream),
    Read(ConnRef),
    //Terminate(ConnRef),
    //WriteAndTerminate(ConnRef, Bytes),
}

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

struct PollReg {
    fsm_token: Token,
    new: Vec<ConnRef>,
    existing: HashSet<ConnRef>,
    drop: HashSet<ConnRef>,
}

enum RegReq {
    New(Vec<FsmState>),
    Update(PollReg),
}

impl TcpHandler {
    pub fn new(workers_pool_size: usize) -> TcpHandler {
        TcpHandler {
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
        self.poll
            .register(&listener, token, Ready::readable(), PollOpt::edge())
            .unwrap();
        self.acceptors
            .insert(token, Arc::new(Mutex::new(Acceptor { listener, spawn })));
    }

    pub fn run(&mut self) {
        let mut events = Events::with_capacity(1024);

        // The following are used for notifying the dispatcher thread about
        // poll (re-)registration requests.
        let (reg_tx, reg_rx) = mpsc::channel::<RegReq>();
        let (reg, set_ready) = mio::Registration::new2();
        self.poll
            .register(&reg, REG_TOKEN, Ready::readable(), PollOpt::edge())
            .unwrap();

        loop {
            self.poll.poll(&mut events, None).unwrap();

            for event in &events {
                if event.token() == REG_TOKEN {
                    while let Ok(req) = reg_rx.try_recv() {
                        match req {
                            RegReq::New(fsm_states) => self.register_new_fsms(fsm_states),
                            RegReq::Update(r) => {
                                let fsm_state =
                                    Arc::clone(&self.fsm_states.get(&r.fsm_token).unwrap());
                                let mut fsm_state = fsm_state.lock().unwrap();
                                self.update_conns(
                                    &r.new,
                                    &r.existing,
                                    &r.drop,
                                    r.fsm_token,
                                    &mut fsm_state,
                                );
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
                    let fsm_state = self.fsm_states.get(&conn_id.fsm_token).unwrap();
                    let fsm_state = fsm_state.clone();

                    self.workers.exec(move || {
                        let fsm_state = &mut *(fsm_state.lock().unwrap());
                        let mut r = handle_poll_events(event.readiness(), conn_id.cref, fsm_state);
                        r.fsm_token = conn_id.fsm_token;
                        reg_tx.send(RegReq::Update(r)).unwrap();
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
                GlobalConnRef {
                    fsm_token: token,
                    cref: 0,
                },
            );

            // Dirty hack to get a reference to the socket moved above ^^.
            let fsm_state = fsm_state.lock().unwrap();
            let conn = fsm_state.conns.get(&0).unwrap();
            self.poll
                .register(
                    &conn.socket,
                    token,
                    Ready::readable(),
                    PollOpt::edge() | PollOpt::oneshot(),
                )
                .unwrap();
        }
    }

    fn update_conns(
        &mut self,
        new: &Vec<ConnRef>,
        existing: &HashSet<ConnRef>,
        drop: &HashSet<ConnRef>,
        fsm_token: Token,
        fsm_state: &mut FsmState,
    ) {
        for cref in new {
            let token = self.inner.get_token();
            self.inner.conn_ids.insert(
                token,
                GlobalConnRef {
                    fsm_token,
                    cref: *cref,
                },
            );
            self.inner.tokens.insert((fsm_token, *cref), token);

            let conn = (*fsm_state).conns.get(cref).unwrap();
            self.poll
                .register(
                    &conn.socket,
                    token,
                    Ready::readable(),
                    PollOpt::edge() | PollOpt::oneshot(),
                )
                .unwrap();
        }

        for cref in existing {
            if let Some(conn) = (*fsm_state).conns.get(cref) {
                let mut ready = Ready::empty();
                //if conn.read {
                if let Some(_) = conn.read_count {
                    ready = ready | Ready::readable();
                }
                // if conn.write {
                if conn.write_buf.len() > 0 {
                    ready = ready | Ready::writable();
                }

                let token;
                if *cref == 0 {
                    token = fsm_token;
                } else {
                    token = *self.inner.tokens.get(&(fsm_token, *cref)).unwrap();
                }

                self.poll
                    .reregister(
                        &conn.socket,
                        token,
                        ready,
                        PollOpt::edge() | PollOpt::oneshot(),
                    )
                    .unwrap();
            }
        }

        for cref in drop {
            (*fsm_state).conns.remove(&cref);
        }
    }
}

impl TcpHandlerInner {
    fn new() -> TcpHandlerInner {
        TcpHandlerInner {
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
                let mut fsm_state = FsmState {
                    conns: HashMap::new(),
                    fsm: fsm,
                };

                // Only `ReadExact` can be returned by `fsm.init()` atm
                if let Return::ReadExact(0, count) = fsm_state.fsm.init() {
                    conn.read_count = Some(count);
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

fn handle_poll_events(ready: Ready, cref: ConnRef, fsm_state: &mut FsmState) -> PollReg {
    let mut poll_reg = PollReg::new();
    let mut rets = Vec::new();

    if let Some(mut conn) = fsm_state.conns.get_mut(&cref) {
        let mut terminate = false;
        poll_reg.existing.insert(cref);

        if ready.is_readable() {
            let (_, t) = read_until_would_block(&mut conn.socket, &mut conn.read_buf).unwrap();
            terminate = t;
        }

        if ready.is_writable() && conn.write_buf.len() != 0 {
            write_and_flush(&mut conn.socket, &mut conn.write_buf);
            conn.write_buf.clear();
        }

        if terminate {
            let len = conn.read_buf.len();
            let buf = conn.read_buf.split_to(len);
            let ev = Event::Terminate(cref, buf.freeze());
            let mut ret = fsm_state.fsm.handle_event(ev);

            rets.append(&mut ret);
            conn.read_count = None;
            poll_reg.drop.insert(cref);
        } else {
            while let Some(c) = conn.read_count {
                let count;
                // Read(cref)
                if c == 0 && conn.read_buf.len() > 0 {
                    count = conn.read_buf.len();
                // ReadExact(cref)
                } else if c > 0 && conn.read_buf.len() >= c {
                    count = c;
                // Not enough data
                } else {
                    break;
                }

                let buf = conn.read_buf.split_to(count);
                let ev = Event::Read(cref, buf.freeze());
                let mut ret = fsm_state.fsm.handle_event(ev);

                conn.read_count = None;
                update_read_counts(&mut ret, cref, &mut conn);
                rets.append(&mut ret);
            }
        }
    }

    poll_reg.handle_fsm_returns(rets, fsm_state);

    return poll_reg;
}

fn update_read_counts(rets: &mut Vec<Return>, cref: ConnRef, conn: &mut FsmConn) {
    for r in rets {
        match r {
            &mut Return::ReadExact(cr, count) if cr == cref && count <= conn.read_buf.len() => {
                conn.read_count = Some(count);
                *r = Return::None;
            }
            &mut Return::Read(cr) if cr == cref && conn.read_buf.len() > 0 => {
                conn.read_count = Some(0);
                *r = Return::None;
            }
            _ => {}
        }
    }
}

impl PollReg {
    fn new() -> Self {
        PollReg {
            fsm_token: Token(0),
            new: Vec::new(),
            existing: HashSet::new(),
            drop: HashSet::new(),
        }
    }

    fn handle_fsm_returns(&mut self, rets: Vec<Return>, fsm_state: &mut FsmState) {
        for r in rets {
            match r {
                Return::None => {}
                Return::ReadExact(cref, count) => {
                    if let Some(mut conn) = fsm_state.conns.get_mut(&cref) {
                        conn.read_count = Some(count);
                        self.existing.insert(cref);
                    }
                }
                Return::Read(cref) => {
                    if let Some(mut conn) = fsm_state.conns.get_mut(&cref) {
                        conn.read_count = Some(0);
                        self.existing.insert(cref);
                    }
                }
                Return::Write(cref, reply) => {
                    if let Some(mut conn) = fsm_state.conns.get_mut(&cref) {
                        conn.write_buf.put(reply);
                        self.existing.insert(cref);
                    }
                }
                Return::Register(cref, socket) => {
                    let fc = FsmConn::new(socket);
                    fsm_state.conns.insert(cref, fc);
                    self.new.push(cref);
                } // TODO Return::Terminate
            }
        }
    }
}

/// Helpers

fn read_until_would_block(src: &mut Read, buf: &mut BytesMut) -> Result<(usize, bool), Error> {
    let mut total_size = 0;
    let mut tmp_buf = [0; 64];

    loop {
        match src.read(&mut tmp_buf) {
            Ok(0) => {
                return Ok((total_size, true));
            }
            Ok(size) => {
                buf.put(&tmp_buf[0..size]);
                total_size += size;
            }
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
