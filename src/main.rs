// FSM
// init -> ... -> handle_proxy
//
// TODO handle close connection
// 1. Make it work
// 2. Cleanup
// 3. Multi threaded
// TODO handle spurious events?
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

#[derive(Debug)]
struct Conn {
    socket: TcpStream, // client <-> proxy conn
    remote_socket: Option<TcpStream>, // proxy <-> server
    state: State,
    read_buf: BytesMut,
    remote_buf: BytesMut,
    already_write: bool,
    already_remote_write: bool,
}

#[derive(PartialEq, Clone, Copy, Debug)]
enum Event {
    Read,
    Write,
    RemoteRead,
    RemoteWrite,
    RemoteRegister,
    EOL,
}

#[derive(PartialEq, Debug)]
enum State {
    ReadFirstRequest,
    WriteFirstReply,
    ReadSecondRequest,
    WriteSecondReply,
    Proxy,
    Terminate,
    TerminateRemote,
}

const SERVER: Token = Token(0);
const LADDR: &'static str = "127.0.0.1:1080";

fn main() {
    let mut next_token_index = 0;
    let mut conns = HashMap::new();

    let addr = LADDR.parse().unwrap();
    let server = TcpListener::bind(&addr).unwrap();

    let poll = Poll::new().unwrap();
    poll.register(&server, SERVER, Ready::readable(), PollOpt::edge()).unwrap();

    let mut events = Events::with_capacity(1024);

    loop {
        poll.poll(&mut events, None).unwrap();

        for event in events.iter() {
            println!("event: {:?}", event);
            match event.token() {
                SERVER => {
                    if event.readiness().is_readable() {
                        next_token_index += 2;
                        accept_connection(&server, next_token_index, &poll, &mut conns);
                    }
                },
                token => {
                    let real_token = Token(token.0 + (token.0 % 2)); // get original token
                    let mut conn = conns.get_mut(&real_token).unwrap();

                    match handle_event(conn, token, event.readiness()) {
                        Err(e) => {
                            panic!("handle_event: {}", e);
                        },
                        Ok(events) => {
                            println!("returned: {:?}", events);
                            for ev in events {
                                match ev {
                                    Event::Read => {
                                        poll.reregister(
                                            &conn.socket,
                                            real_token,
                                            Ready::readable(),
                                            PollOpt::edge() | PollOpt::oneshot()
                                        ).unwrap();
                                    },
                                    Event::Write => {
                                        poll.reregister(
                                            &conn.socket,
                                            real_token,
                                            Ready::writable(),
                                            PollOpt::edge() | PollOpt::oneshot()
                                        ).unwrap();
                                    },
                                    Event::RemoteRead => {
                                        if let Some(ref socket) = conn.remote_socket {
                                            poll.reregister(
                                                socket,
                                                Token(real_token.0 - 1),
                                                Ready::readable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Event::RemoteWrite => {
                                        if let Some(ref socket) = conn.remote_socket {
                                            poll.reregister(
                                                socket,
                                                Token(real_token.0 - 1),
                                                Ready::writable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Event::RemoteRegister => {
                                        if let Some(ref socket) = conn.remote_socket {
                                            poll.register(
                                                socket,
                                                Token(real_token.0 - 1),
                                                Ready::readable(),
                                                PollOpt::edge() | PollOpt::oneshot()
                                            ).unwrap();
                                        }
                                    },
                                    Event::EOL => {
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

fn handle_event(conn: &mut Conn, token: Token, readiness: Ready) -> Result<Vec<Event>, &'static str> {
        let events = match conn.state {
            State::ReadFirstRequest => handle_read_first_request(conn),
            State::WriteFirstReply => handle_write_first_reply(conn),
            State::ReadSecondRequest => handle_read_second_request(conn),
            State::WriteSecondReply => handle_write_second_reply(conn),
            State::Proxy => {
                let is_client_conn = token.0 % 2 == 0;
                handle_proxy(conn, is_client_conn, readiness.is_readable(), readiness.is_writable())
            },
            State::Terminate => handle_terminate(conn),
            State::TerminateRemote => handle_terminate_remote(conn),
        };

        return Ok(events);
    }

fn accept_connection(server: &TcpListener, token_index: usize, poll: &Poll, conns: &mut HashMap<Token, Conn>) {
    let socket = server.accept().unwrap().0;
    let token = Token(token_index);
    poll.register(&socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();

    let conn = Conn{
        socket: socket,
        remote_socket: None,
        state: State::ReadFirstRequest,
        read_buf: BytesMut::with_capacity(64 * 1024),
        remote_buf: BytesMut::with_capacity(64 * 1024),
        already_write: false,
        already_remote_write: false,
    };
    conns.insert(token, conn);
}

// TODO trait for states

fn handle_read_first_request(conn: &mut Conn) -> Vec<Event> {
    let socket = &mut conn.socket;
    read_until_would_block(socket, &mut conn.read_buf).unwrap();

    let len = conn.read_buf.len();
    if len < 2 || len < conn.read_buf[1] as usize + 2 {
        return vec![Event::Read];
    }

    conn.state = State::WriteFirstReply;
    conn.read_buf.clear();

    return vec![Event::Write];
}

fn handle_write_first_reply(conn: &mut Conn) -> Vec<Event> {
    let socket = &mut conn.socket;
    let reply = [5, 0];
    let size = socket.write(&reply).unwrap();
    assert_eq!(size, reply.len());

    conn.state = State::ReadSecondRequest;

    return vec![Event::Read];
}

fn handle_read_second_request(conn: &mut Conn) -> Vec<Event> {
    let socket = &mut conn.socket;
    read_until_would_block(socket, &mut conn.read_buf).unwrap();

    if conn.read_buf.len() < 4 {
        return vec![Event::Read];
    }

    // handle hdr
    let cmd = conn.read_buf[1];
    assert_eq!(cmd, 0x1); // CONNECT

    //let cmd = buf[1];
    let atyp = conn.read_buf[3];
    assert_eq!(atyp, 0x1);

    if conn.read_buf.len() < 10 {
        return vec![Event::Read];
    }

    let dst_addr = IpAddr::V4(Ipv4Addr::new(
        conn.read_buf[4], conn.read_buf[5],
        conn.read_buf[6], conn.read_buf[7]
    ));
    let dst_port: u16 = BigEndian::read_u16(
        &[conn.read_buf[8], conn.read_buf[9]][..]
    );

    let s = TcpStream::connect(&SocketAddr::new(dst_addr, dst_port)).unwrap();
    conn.remote_socket = Some(s);

    conn.state = State::WriteSecondReply;
    conn.read_buf.clear();

    return vec![Event::Write];
}

fn handle_write_second_reply(conn: &mut Conn) -> Vec<Event> {
    let socket = &mut conn.socket;

    if let Some(ref remote_socket) = conn.remote_socket {
        let remote_addr = remote_socket.local_addr().unwrap();
        if let V4(addr) = remote_addr.ip() {
            let port = remote_addr.port();
            let tmp = addr.octets();
            let reply = [
                5, 0, 0, 1,
                tmp[0], tmp[1], tmp[2], tmp[3], // IP
                (port >> 8) as u8, port as u8 // Port
            ];
            let size = socket.write(&reply).unwrap();
            assert_eq!(size, reply.len());
            conn.state = State::Proxy;

            return vec![Event::Read, Event::RemoteRegister, Event::RemoteRead];
        } else {
            panic!("why");
        }
    } else {
        panic!("wtf");
    }
}

fn handle_proxy(conn: &mut Conn, client_conn: bool, is_read: bool, is_write: bool) -> Vec<Event> {
    println!("handle_proxy: {} {} {}", client_conn, is_read, is_write);
    match (client_conn, is_read) {
        (true, true) => {
            let socket = &mut conn.socket;
            let mut ret = vec![];

            let (_, eof) = read_until_would_block(socket, &mut conn.read_buf).unwrap();
            if eof {
                conn.state = State::Terminate;
            } else {
                ret.push(Event::Read);
            }

            if !conn.already_remote_write {
                    ret.push(Event::RemoteWrite);
                    conn.already_remote_write = true;
            }

            ret
        },
        (false, true) => {
            let mut ret = vec![];

            if let Some(ref mut socket) = conn.remote_socket {


                let (size, eof) = read_until_would_block(socket, &mut conn.read_buf).unwrap();
                if eof {
                    conn.state = State::TerminateRemote;
                } else {
                    ret.push(Event::RemoteRead);
                }


            if !conn.already_write {
                ret.push(Event::Write);
                conn.already_write = true;
            }

            }

            ret
        },

        (true, false) => {
            let socket = &mut conn.socket;
            // TODO(mp) handle size = 0
            let size = socket.write(&conn.remote_buf).unwrap();
            assert_eq!(size, conn.remote_buf.len());
            conn.remote_buf.clear();
            conn.already_write = false;
            vec![Event::Read]
        },
        (false, false) => {
            if let Some(ref mut socket) = conn.remote_socket {
                let size = socket.write(&conn.read_buf).unwrap();
                assert_eq!(size, conn.read_buf.len());
                conn.read_buf.clear();
            }
            conn.already_remote_write = false;
            vec![Event::RemoteRead]
        },
    }
}

// assert_eq!(event, remote_write)
fn handle_terminate(conn: &mut Conn) -> Vec<Event> {
    conn.socket.shutdown(Shutdown::Both).unwrap();

    if let Some(ref mut sock) = conn.remote_socket {
    let size = sock.write(&conn.remote_buf).unwrap();
    assert_eq!(size, conn.remote_buf.len());
    conn.remote_buf.clear();

    sock.shutdown(Shutdown::Both).unwrap();
    }

    return vec![Event::EOL];
}

// assert_eq!(event, remote_write)
fn handle_terminate_remote(conn: &mut Conn) -> Vec<Event> {
    if let Some(ref mut sock) = conn.remote_socket {
        sock.shutdown(Shutdown::Both).unwrap();
    }

    let size = conn.socket.write(&conn.read_buf).unwrap();
    assert_eq!(size, conn.read_buf.len());
    conn.read_buf.clear();

    conn.socket.shutdown(Shutdown::Both).unwrap();

    return vec![Event::EOL];
}

// ---

fn read_until_would_block(source: &mut Read, buf: &mut BytesMut) -> Result<(usize, bool), std::io::Error> {
    let mut total_size = 0;
    let mut tmp_buf = [0; 64];

    loop {
        match source.read(&mut tmp_buf) {
            Ok(0) => {
                return Ok((total_size, true));
            },
            Ok(size) => {
                buf.put(&tmp_buf[0 .. size]);
                total_size += size;
            },
            // TODO if size == 0 EOF!
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
