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

/// A mio token for notifying the dispatcher thread about poll registration requests
/// in the channel.
const REG_TOKEN: Token = Token(0);

/// Unique (inside FSM) identifier for a TCP connection.
///
/// The initiating (first) connection is always `ConnRef(0)`.
type ConnRef = u64;

/// Represents a state of an FSM instance.
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

struct AcceptConnResult {
    token: Token,
    fsm_state: FsmState,
}

struct HandleEventsResult {
    main_token: Token,
    new: Vec<ConnRef>,
    old: HashSet<ConnRef>,
}

enum PollRegReq {
    A(AcceptConnResult),
    H(HandleEventsResult),
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

        // XXX Possible race (if `register` is allowed to be called after `run`):
        //     we can get notified about a new listener before the acceptor has
        //     been inserted.
        self.poll.register(&listener, token, Ready::readable(), PollOpt::edge()).unwrap();
        self.acceptors.insert(token, Arc::new(Mutex::new(Acceptor{listener, spawn})));
    }

   pub fn run(&mut self) {
        let mut events = Events::with_capacity(1024);

        let (tx, rx) = mpsc::channel::<PollRegReq>();
        let (registration, set_readiness) = mio::Registration::new2();
        self.poll.register(&registration, REG_TOKEN, Ready::readable(), PollOpt::edge()).unwrap();

        loop {
            println!("loop: start");
            self.poll.poll(&mut events, None).unwrap();
            for event in &events {
                println!("got event: {:?}", event);

                if event.token() == REG_TOKEN {
                    println!("REG_TOKEN: start");
                    while let Ok(req) = rx.try_recv() {
                        match req {
                            PollRegReq::A(a) => {
                                let fsm_state = Arc::new(Mutex::new(a.fsm_state));
                                self.fsm_states.insert(a.token, Arc::clone(&fsm_state));
                                self.inner.conn_ids.insert(
                                    a.token,
                                    GlobalConnRef{main_token: a.token, cref: 0}
                                );
                                // Dirty hack to get a reference to the socket moved above ^^.
                                let fsm_state = fsm_state.lock().unwrap();
                                let conn = fsm_state.conns.get(&0).unwrap();
                                self.poll.register(&conn.socket, a.token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
                           },
                           PollRegReq::H(poll_reg) => {
                               self.poll_reregister(&poll_reg, poll_reg.main_token);
                           }
                        }
                    }
                    println!("REG_TOKEN: end");
                } else if let Some(acceptor) = self.acceptors.get(&event.token()) {
                        let token = self.inner.get_token();
                        let tx = tx.clone();
                        let set_ready = set_readiness.clone();
                        let acceptor = acceptor.clone();

                        self.workers.exec(move || {
                            let a = accept_connection(&acceptor.lock().unwrap(), token);
                            tx.send(PollRegReq::A(a)).unwrap();
                            set_ready.set_readiness(Ready::readable()).unwrap();
                        });


                } else {
                    let conn_id = *(self.inner.conn_ids.get(&event.token()).unwrap());
                    let mut f = &self.fsm_states;
                    let fsm_state = f.get(&conn_id.main_token).unwrap();
                    let set_ready = set_readiness.clone();
                    let tx = tx.clone();
                    let fsm_state = fsm_state.clone();

                    self.workers.exec(move || {
                        let fsm_state = &mut *(fsm_state.lock().unwrap());
                        let mut poll_reg = handle_events(event.readiness(), conn_id.cref, fsm_state);
                        poll_reg.main_token = conn_id.main_token;
                        tx.send(PollRegReq::H(poll_reg)).unwrap();
                        set_ready.set_readiness(Ready::readable()).unwrap();
                    });
                }
            }
        }
    }

    // TODO Pass locked fsm_state instead of main_token
    fn poll_reregister(&mut self, poll_reg: &HandleEventsResult, main_token: Token) {
        for cref in &poll_reg.new {
            let token = self.inner.get_token();
            self.inner.conn_ids.insert(token, GlobalConnRef{main_token, cref: *cref});
            self.inner.tokens.insert((main_token, *cref), token);

            let f = &self.fsm_states;
            let fsm_state = f.get(&main_token).unwrap();
            let fsm_state = fsm_state.lock().unwrap();
            let conn = (*fsm_state).conns.get(cref).unwrap();
            // TODO s/readable/?/
            println!("register: {:?} Readable", token);
            self.poll.register(&conn.socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();
        }

        for cref in &poll_reg.old {
            let f = &self.fsm_states;

            let fsm_state = f.get(&main_token).unwrap();
            let fsm_state = fsm_state.lock().unwrap();
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
            println!("reregister: {:?} {:?}", token, ready);
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

fn accept_connection(acceptor: &Acceptor, token: Token) -> AcceptConnResult {
    let (socket, _) = acceptor.listener.accept().unwrap();

    // Create an FSM instance
    let fsm = (acceptor.spawn)();
    let mut conn = FsmConn::new(socket);
    let mut fsm_state = FsmState{
        conns: HashMap::new(),
        fsm: fsm,
    };

    // Currently, only ReadExact(0, <..>) is supported in fsm.init() return
    if let Return::ReadExact(0, count) = fsm_state.fsm.init() {
        conn.read_count = Some(count);
        conn.read = true;
    } else {
        panic!("NYI");
    }

    fsm_state.conns.insert(0, conn);

    AcceptConnResult{
        token: token,
        fsm_state: fsm_state,
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
