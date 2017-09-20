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
    ReadFirstRequest,
    WriteFirstReply,
    ReadSecondRequest,
    WriteSecondReply,
    Transfer,
    Terminate,
    TerminateRemote,
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
    Read,
    Write,
    RemoteRad,
    RemoteWite,
    RemoteRegister,
    EOL,
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
                    let mut conn = states.get_mut(&real_token).unwrap();

                    match conn.handle_event(token, event.readiness()) {
                        Err(e) => {
                            panic!("handle_event returned error: {:?}", e);
                        },
                        Ok(events) => {
                            for ev in events {
                                match ev {
                                    Action::Read => {
                                        poll.reregister(
                                            &conn.client_socket,
                                            real_token,
                                            Ready::readable(),
                                            PollOpt::edge() | PollOpt::oneshot()
                                        ).unwrap();
                                    },
                                    Action::Write => {
                                        poll.reregister(
                                            &conn.client_socket,
                                            real_token,
                                            Ready::writable(),
                                            PollOpt::edge() | PollOpt::oneshot()
                                        ).unwrap();
                                    },
                                    Action::RemoteRad => {
                                        if let Some(ref client_socket) = conn.target_socket {
                                            poll.reregister(
                                                client_socket,
                                                Token(real_token.0 - 1),
                                                Ready::readable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Action::RemoteWite => {
                                        if let Some(ref client_socket) = conn.target_socket {
                                            poll.reregister(
                                                client_socket,
                                                Token(real_token.0 - 1),
                                                Ready::writable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Action::RemoteRegister => {
                                        if let Some(ref client_socket) = conn.target_socket {
                                            poll.register(
                                                client_socket,
                                                Token(real_token.0 - 1),
                                                Ready::readable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Action::EOL => {
                                        // TODO remove from the hashmap
                                    }
                                }
                            }
                        },
                    }
                }
            }
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
            State::ReadFirstRequest =>  st_read_first_request(ev, self),
            State::WriteFirstReply =>   st_write_first_reply(ev, self),
            State::ReadSecondRequest => st_read_second_request(ev, self),
            State::WriteSecondReply =>  st_write_second_reply(ev, self),
            State::Transfer =>          st_proxy(ev, self),
            State::Terminate =>         st_terminate(ev, self),
            State::TerminateRemote =>   st_terminate_remote(ev, self),
        };

        return Ok(actions);
    }
}

fn accept_connection(server: &TcpListener, token_index: usize, poll: &Poll, states: &mut HashMap<Token, StateData>) {
    let client_socket = server.accept().unwrap().0;
    let token = Token(token_index);
    poll.register(&client_socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();

    let conn = StateData{
        client_socket: client_socket,
        target_socket: None,
        state_name: State::ReadFirstRequest,
        client_buf: BytesMut::with_capacity(64 * 1024),
        target_buf: BytesMut::with_capacity(64 * 1024),
        has_client_write: false,
        has_target_write: false,
    };
    states.insert(token, conn);
}

// States

// TODO trait for states

fn st_read_first_request(_: Event, conn: &mut StateData) -> Vec<Action> {
    let client_socket = &mut conn.client_socket;
    read_until_would_block(client_socket, &mut conn.client_buf).unwrap();

    let len = conn.client_buf.len();
    if len < 2 || len < conn.client_buf[1] as usize + 2 {
        return vec![Action::Read];
    }

    conn.state_name = State::WriteFirstReply;
    conn.client_buf.clear();

    return vec![Action::Write];
}

fn st_write_first_reply(_: Event, conn: &mut StateData) -> Vec<Action> {
    let client_socket = &mut conn.client_socket;
    let reply = [5, 0];

    write_and_flush(client_socket, &reply);
    conn.state_name = State::ReadSecondRequest;

    return vec![Action::Read];
}

fn st_read_second_request(_: Event, conn: &mut StateData) -> Vec<Action> {
    let client_socket = &mut conn.client_socket;
    read_until_would_block(client_socket, &mut conn.client_buf).unwrap();

    if conn.client_buf.len() < 4 {
        return vec![Action::Read];
    }

    // handle hdr
    let cmd = conn.client_buf[1];
    assert_eq!(cmd, 0x1); // CONNECT

    //let cmd = buf[1];
    let atyp = conn.client_buf[3];
    assert_eq!(atyp, 0x1);

    if conn.client_buf.len() < 10 {
        return vec![Action::Read];
    }

    let target_addr = IpAddr::V4(Ipv4Addr::new(
        conn.client_buf[4], conn.client_buf[5],
        conn.client_buf[6], conn.client_buf[7]
    ));
    let target_port: u16 = BigEndian::read_u16(
        &[conn.client_buf[8], conn.client_buf[9]][..]
    );

    let s = TcpStream::connect(&SocketAddr::new(target_addr, target_port)).unwrap();
    conn.target_socket = Some(s);

    conn.state_name = State::WriteSecondReply;
    conn.client_buf.clear();

    return vec![Action::Write];
}

fn st_write_second_reply(_: Event, conn: &mut StateData) -> Vec<Action> {
    let client_socket = &mut conn.client_socket;

    if let Some(ref target_socket) = conn.target_socket {
        let remote_addr = target_socket.local_addr().unwrap();
        if let V4(addr) = remote_addr.ip() {
            let port = remote_addr.port();
            let tmp = addr.octets();
            let reply = [
                5, 0, 0, 1,
                tmp[0], tmp[1], tmp[2], tmp[3], // IP
                (port >> 8) as u8, port as u8 // Port
            ];

            write_and_flush(client_socket, &reply);
            conn.state_name = State::Transfer;

            return vec![Action::Read, Action::RemoteRegister, Action::RemoteRad];
        } else {
            panic!("why");
        }
    } else {
        panic!("wtf");
    }
}

fn st_proxy(ev: Event, conn: &mut StateData) -> Vec<Action> {

    let client_conn = ev.token.0 % 2 == 0;
    let is_read = ev.readiness.is_readable();
    let is_write = ev.readiness.is_writable();
            //st_proxy(st, is_client_st, readiness.is_readable(), readiness.is_writable())
    println!("st_proxy: {} {} {}", client_conn, is_read, is_write);
    match (client_conn, is_read) {
        (true, true) => {
            let client_socket = &mut conn.client_socket;
            let mut ret = vec![];

            let (_, eof) = read_until_would_block(client_socket, &mut conn.client_buf).unwrap();
            if eof {
                conn.state_name = State::Terminate;
            } else {
                ret.push(Action::Read);
            }

            if !conn.has_target_write {
                    ret.push(Action::RemoteWite);
                    conn.has_target_write = true;
            }

            ret
        },
        (false, true) => {
            let mut ret = vec![];

            if let Some(ref mut client_socket) = conn.target_socket {


                let (_, eof) = read_until_would_block(client_socket, &mut conn.client_buf).unwrap();
                if eof {
                    conn.state_name = State::TerminateRemote;
                } else {
                    ret.push(Action::RemoteRad);
                }


            if !conn.has_client_write {
                ret.push(Action::Write);
                conn.has_client_write = true;
            }

            }

            ret
        },

        (true, false) => {
            let client_socket = &mut conn.client_socket;
            // TODO(mp) handle size = 0
            write_and_flush(client_socket, &conn.target_buf);
            conn.target_buf.clear();
            conn.has_client_write = false;
            vec![Action::Read]
        },
        (false, false) => {
            if let Some(ref mut client_socket) = conn.target_socket {
                write_and_flush(client_socket, &conn.client_buf);
                conn.client_buf.clear();
            }
            conn.has_target_write = false;
            vec![Action::RemoteRad]
        },
    }
}

// assert_eq!(event, remote_write)
fn st_terminate(_: Event, conn: &mut StateData) -> Vec<Action> {
    conn.client_socket.shutdown(Shutdown::Both).unwrap();

    if let Some(ref mut sock) = conn.target_socket {
        write_and_flush(sock, &conn.target_buf);
        conn.target_buf.clear();
        sock.shutdown(Shutdown::Both).unwrap();
    }

    return vec![Action::EOL];
}

// assert_eq!(event, remote_write)
fn st_terminate_remote(_: Event, conn: &mut StateData) -> Vec<Action> {
    if let Some(ref mut sock) = conn.target_socket {
        sock.shutdown(Shutdown::Both).unwrap();
    }

    write_and_flush(&mut conn.client_socket, &conn.client_buf);
    conn.client_buf.clear();
    conn.client_socket.shutdown(Shutdown::Both).unwrap();

    return vec![Action::EOL];
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
