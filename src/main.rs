// multithreaded macros
extern crate mio;
extern crate bytes;

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, Shutdown};
use std::io::{Read, Write};
use std::io::ErrorKind;
use std::net::IpAddr::V4;
use bytes::{BytesMut, BufMut, ByteOrder, BigEndian};
use mio::*;
use mio::tcp::{TcpListener, TcpStream};

const LADDR: &'static str = "127.0.0.1:1080";
const SERVER_TOKEN: Token = Token(0);

#[derive(PartialEq, Debug)]
enum State {
    GetAuthMethods,
    SelectAuthMethod,
    GetTargetAddr,
    ReplyTargetAddr,
    DoProxy,
    TerminateClient,
    TerminateTarget,
}

#[derive(Debug)]
struct StateData {
    client_socket: TcpStream,
    target_socket: Option<TcpStream>,

    client_buf: BytesMut,
    target_buf: BytesMut,

    has_client_write: bool, // TODO rename
    has_target_write: bool,

    state_name: State,
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Action {
    ClientRead,
    ClientWrite,
    TargetRead,
    TargetWrite,
    RemoteRegister,
    FIN,
}

fn main() {
    let mut next_token_index = 0;
    let mut states = HashMap::new();

    let addr = LADDR.parse().unwrap();
    let server = TcpListener::bind(&addr).unwrap();

    let poll = Poll::new().unwrap();
    poll.register(&server, SERVER_TOKEN, Ready::readable(), PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);

    loop {
        poll.poll(&mut events, None).unwrap();

        for event in events.iter() {
            match event.token() {
                SERVER_TOKEN => {
                    if event.readiness().is_readable() {
                        next_token_index += 2;
                        accept_connection(&server, next_token_index, &poll, &mut states);
                    }
                },
                token => {
                    let real_token = Token(token.0 + (token.0 % 2)); // get original token
                    let mut st = states.get_mut(&real_token).unwrap();

                    match st.handle_event(token, event.readiness()) {
                        Err(e) => {
                            panic!("handle_event returned error: {:?}", e);
                        },
                        Ok(actions) => {
                            for a in actions {
                                handle_action(a, real_token, st, &poll);
                            }
                        },
                    }
                }
            }
        }
    }
}

fn handle_action(action: Action, token: Token, st: &mut StateData, poll: &mio::Poll) {
    let real_token = Token(token.0 + (token.0 % 2)); // get original token

    match action {
        Action::ClientRead => {
            poll.reregister(
                    &st.client_socket,
                    real_token,
                    Ready::readable(),
                    PollOpt::edge() | PollOpt::oneshot()
                    ).unwrap();
        },
        Action::ClientWrite => {
            poll.reregister(
                    &st.client_socket,
                    real_token,
                    Ready::writable(),
                    PollOpt::edge() | PollOpt::oneshot()
                    ).unwrap();
        },
        Action::TargetRead => {
            if let Some(ref client_socket) = st.target_socket {
                poll.reregister(
                        client_socket,
                        Token(real_token.0 - 1),
                        Ready::readable(),
                        PollOpt::edge() | PollOpt::oneshot()
                        ).unwrap();
            }
        },
        Action::TargetWrite => {
            if let Some(ref client_socket) = st.target_socket {
                poll.reregister(
                        client_socket,
                        Token(real_token.0 - 1),
                        Ready::writable(),
                        PollOpt::edge() | PollOpt::oneshot()
                        ).unwrap();
            }
        },
        Action::RemoteRegister => {
            if let Some(ref client_socket) = st.target_socket {
                poll.register(
                        client_socket,
                        Token(real_token.0 - 1),
                        Ready::readable(),
                        PollOpt::edge() | PollOpt::oneshot()
                        ).unwrap();
            }
        },
        Action::FIN => {
            // TODO remove from the hashmap
        }
    }
}

struct Event {
    token: Token,
    readiness: Ready,
}

impl StateData {
    fn handle_event(&mut self, token: Token, readiness: Ready) -> Result<Vec<Action>, ()> {
        let ev = Event{token, readiness};
        let actions = match self.state_name {
            State::GetAuthMethods =>  st_get_auth_methods(ev, self),
            State::SelectAuthMethod =>   st_select_auth_method(ev, self),
            State::GetTargetAddr => st_get_target_addr(ev, self),
            State::ReplyTargetAddr =>  st_reply_target_addr(ev, self),
            State::DoProxy =>          st_do_proxy(ev, self),
            State::TerminateClient =>         st_terminate_client(ev, self),
            State::TerminateTarget =>   st_terminate_target(ev, self),
        };
        return Ok(actions);
    }
}

fn accept_connection(server: &TcpListener, token_index: usize, poll: &Poll, states: &mut HashMap<Token, StateData>) {
    let client_socket = server.accept().unwrap().0;
    let token = Token(token_index);
    poll.register(&client_socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();

    let st = StateData{
        client_socket: client_socket,
        target_socket: None,
        state_name: State::GetAuthMethods,
        client_buf: BytesMut::with_capacity(64 * 1024),
        target_buf: BytesMut::with_capacity(64 * 1024),
        has_client_write: false,
        has_target_write: false,
    };
    states.insert(token, st);
}

// States

fn st_get_auth_methods(_: Event, st: &mut StateData) -> Vec<Action> {
    let client_socket = &mut st.client_socket;

    read_until_would_block(client_socket, &mut st.client_buf).unwrap();
    let len = st.client_buf.len();
    if len < 2 || len < st.client_buf[1] as usize + 2 {
        return vec![Action::ClientRead];
    }

    // TODO do sanity checks: SOCKS version, valid methods

    st.state_name = State::SelectAuthMethod;
    st.client_buf.clear();

    return vec![Action::ClientWrite];
}

fn st_select_auth_method(_: Event, st: &mut StateData) -> Vec<Action> {
    let client_socket = &mut st.client_socket;
    let reply = [5, 0]; // "0" = no auth

    write_and_flush(client_socket, &reply);
    st.state_name = State::GetTargetAddr;

    return vec![Action::ClientRead];
}

fn st_get_target_addr(_: Event, st: &mut StateData) -> Vec<Action> {
    let client_socket = &mut st.client_socket;
    read_until_would_block(client_socket, &mut st.client_buf).unwrap();

    if st.client_buf.len() < 4 {
        return vec![Action::ClientRead];
    }

    let cmd = st.client_buf[1];
    assert_eq!(cmd, 0x1); // CMD CONNECT

    let atyp = st.client_buf[3];
    assert_eq!(atyp, 0x1); // ATYP IPv4

    if st.client_buf.len() < 10 {
        return vec![Action::ClientRead];
    }

    let target_addr = IpAddr::V4(Ipv4Addr::new(
        st.client_buf[4], st.client_buf[5],
        st.client_buf[6], st.client_buf[7]
    ));
    let target_port: u16 = BigEndian::read_u16(
        &[st.client_buf[8], st.client_buf[9]][..]
    );
    let target_socket = TcpStream::connect(&SocketAddr::new(target_addr, target_port)).unwrap();
    st.target_socket = Some(target_socket);

    st.state_name = State::ReplyTargetAddr;
    st.client_buf.clear();

    return vec![Action::ClientWrite];
}

fn st_reply_target_addr(_: Event, st: &mut StateData) -> Vec<Action> {
    let client_socket = &mut st.client_socket;

    if let Some(ref target_socket) = st.target_socket {
        let addr = target_socket.local_addr().unwrap();
        if let V4(ip) = addr.ip() {
            let port = addr.port();
            let tmp = ip.octets();
            let reply = [
                5, 0, 0, 1,
                tmp[0], tmp[1], tmp[2], tmp[3], // IP
                (port >> 8) as u8, port as u8 // Port
            ];
            write_and_flush(client_socket, &reply);

            st.state_name = State::DoProxy;

            return vec![Action::ClientRead, Action::RemoteRegister, Action::TargetRead];
        }
    }

    panic!("should not happen");
}

fn st_do_proxy(ev: Event, st: &mut StateData) -> Vec<Action> {
    let client_st = ev.token.0 % 2 == 0;

    let mut actions = vec![];

    match (client_st, ev.readiness.is_readable()) {
        (true, true) => {
            let client_socket = &mut st.client_socket;

            let (_, eof) = read_until_would_block(client_socket, &mut st.client_buf).unwrap();
            if eof {
                st.state_name = State::TerminateClient;
            } else {
                actions.push(Action::ClientRead);
            }

            if !st.has_target_write {
                    actions.push(Action::TargetWrite);
                    st.has_target_write = true;
            }
        },
        (false, true) => {
            if let Some(ref mut client_socket) = st.target_socket {


                let (_, eof) = read_until_would_block(client_socket, &mut st.client_buf).unwrap();
                if eof {
                    st.state_name = State::TerminateTarget;
                } else {
                    actions.push(Action::TargetRead);
                }


            if !st.has_client_write {
                actions.push(Action::ClientWrite);
                st.has_client_write = true;
            }

            }
        },

        (true, false) => {
            let client_socket = &mut st.client_socket;
            // TODO(mp) handle size = 0
            write_and_flush(client_socket, &st.target_buf);
            st.target_buf.clear();
            st.has_client_write = false;
            actions.push(Action::ClientRead);
        },
        (false, false) => {
            if let Some(ref mut client_socket) = st.target_socket {
                write_and_flush(client_socket, &st.client_buf);
                st.client_buf.clear();
            }
            st.has_target_write = false;
            actions.push(Action::TargetRead);
        },
    }

    return actions;
}

fn st_terminate_client(ev: Event, st: &mut StateData) -> Vec<Action> {
    assert!(ev.readiness.is_writable());

    st.client_socket.shutdown(Shutdown::Both).unwrap();

    if let Some(ref mut sock) = st.target_socket {
        write_and_flush(sock, &st.target_buf);
        st.target_buf.clear();
        sock.shutdown(Shutdown::Both).unwrap();
    }

    return vec![Action::FIN];
}

fn st_terminate_target(ev: Event, st: &mut StateData) -> Vec<Action> {
    assert!(ev.readiness.is_writable());

    if let Some(ref mut sock) = st.target_socket {
        sock.shutdown(Shutdown::Both).unwrap();
    }

    write_and_flush(&mut st.client_socket, &st.client_buf);
    st.client_buf.clear();
    st.client_socket.shutdown(Shutdown::Both).unwrap();

    return vec![Action::FIN];
}

// ---

fn write_and_flush(dst: &mut Write, buf: &[u8]) {
    let size = dst.write(&buf).unwrap();
    assert_eq!(size, buf.len());
    dst.flush().unwrap();
}

fn read_until_would_block(src: &mut Read, buf: &mut BytesMut) -> Result<(usize, bool), std::io::Error> {
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
                    // client_socket is exhausted
                    break;
                } else {
                    return Err(err);
                }
            }
        }
    }

    return Ok((total_size, false));
}
