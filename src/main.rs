// TODO open a connection and monitor it
// TODO proper state machine
// TODO thread pool

extern crate mio;
extern crate bytes;

use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::io::{Read, Write};
use bytes::{BytesMut, BufMut, ByteOrder, BigEndian};
use std::io::ErrorKind;
use std::net::IpAddr::V4;

struct Conn {
    socket: TcpStream,
    remote_socket: Option<TcpStream>,
    state: State,
    read_buf: BytesMut,
    write_buf: BytesMut,
    next_event: Event,
}

#[derive(PartialEq, Clone, Copy)]
enum Event {
    Read,
    Write,
    Nil,
}

#[derive(PartialEq)]
enum State {
    ReadFirstRequest,
    WriteFirstReply,
    ReadSecondRequest,
    WriteSecondReply,
    WaitForTerminate,
    Proxy,
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
            match event.token() {
                SERVER => {
                    if event.readiness().is_readable() {
                        next_token_index += 1;
                        accept_connection(&server, next_token_index, &poll, &mut conns);
                    }
                },
                token => {
                    let mut conn = &mut conns.get_mut(&token).unwrap();

                    // TODO handle close connection

                    // Trait
                    match conn.handle_event(event.readiness()) {
                        Ok(Event::Read) => {
                            poll.reregister(
                                &conn.socket,
                                token,
                                Ready::readable(),
                                PollOpt::edge() | PollOpt::oneshot()
                            ).unwrap();
                        },
                        Ok(Event::Write) => {
                            poll.reregister(
                                &conn.socket,
                                token,
                                Ready::writable(),
                                PollOpt::edge() | PollOpt::oneshot()
                            ).unwrap();
                        },
                        Ok(Event::Nil) => {
                            panic!("event::nil");
                        },
                        Err(_) => {
                            panic!("err");
                        },
                    }
                }
            }
        }
    }
}

impl Conn {
    fn handle_event(&mut self, readiness: Ready) -> Result<Event, &'static str> {
        // sanity checks
        if !(readiness.is_writable() && self.next_event == Event::Write) &&
            !(readiness.is_readable() && self.next_event == Event::Read) {
                return Err("wtf");
        }

        match self.state {
            State::ReadFirstRequest =>
                handle_read_first_request(self),
            State::WriteFirstReply =>
                handle_write_first_reply(self),
            State::ReadSecondRequest =>
                handle_read_second_request(self),
            State::WriteSecondReply =>
                handle_write_second_reply(self),
            State::Proxy =>
                panic!("Proxy"),
            State::WaitForTerminate =>
                panic!("NYI"),
        }

        return Ok(self.next_event);
    }
}

fn accept_connection(server: &TcpListener, token_index: usize, poll: &Poll, conns: &mut HashMap<Token, Conn>) {
    let socket = server.accept().unwrap().0;
    let token = Token(token_index);
    poll.register(&socket, token, Ready::readable(), PollOpt::edge() | PollOpt::oneshot()).unwrap();

    let conn = Conn{
        socket: socket,
        remote_socket: None,
        state: State::ReadFirstRequest,
        read_buf: BytesMut::with_capacity(64),
        write_buf: BytesMut::with_capacity(64),
        next_event: Event::Read,
    };
    conns.insert(token, conn);
}

// TODO trait for states

fn handle_read_first_request(conn: &mut Conn) {
    let socket = &mut conn.socket;
    read_until_would_block(socket, &mut conn.read_buf).unwrap();

    let len = conn.read_buf.len();
    if len < 2 || len < conn.read_buf[1] as usize + 2 {
        return;
    }

    conn.state = State::WriteFirstReply;
    conn.next_event = Event::Write;
    conn.read_buf.clear();
}

fn handle_write_first_reply(conn: &mut Conn) {
    let socket = &mut conn.socket;
    let reply = [5, 0];
    let size = socket.write(&reply).unwrap();
    assert_eq!(size, reply.len());

    conn.state = State::ReadSecondRequest;
    conn.next_event = Event::Read;
}

fn handle_read_second_request(conn: &mut Conn) {
    let socket = &mut conn.socket;
    println!("socket: {:?}", socket);
    read_until_would_block(socket, &mut conn.read_buf).unwrap();

    if conn.read_buf.len() < 4 {
        return;
    }

    // handle hdr
    let cmd = conn.read_buf[1];
    assert_eq!(cmd, 0x1); // CONNECT

    //let cmd = buf[1];
    let atyp = conn.read_buf[3];
    assert_eq!(atyp, 0x1);

    if conn.read_buf.len() < 10 {
        return;
    }

    let dst_addr = IpAddr::V4(Ipv4Addr::new(
        conn.read_buf[4], conn.read_buf[5],
        conn.read_buf[6], conn.read_buf[7]
    ));
    let dst_port: u16 = BigEndian::read_u16(
        &[conn.read_buf[8], conn.read_buf[9]][..]
    );

    println!("second: {}:{}", dst_addr, dst_port);

    let s = TcpStream::connect(&SocketAddr::new(dst_addr, dst_port)).unwrap();
    conn.remote_socket = Some(s);


    conn.state = State::WriteSecondReply;
    conn.next_event = Event::Write;
    conn.read_buf.clear();

    // TODO handle errors
}

fn handle_write_second_reply(conn: &mut Conn) {
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
            conn.next_event = Event::Read;
        } else {
            panic!("why");
        }
    } else {
        panic!("wtf");
    }
}

// ---

fn read_until_would_block(source: &mut Read, buf: &mut BytesMut) -> Result<usize, std::io::Error> {
    let mut total_size = 0;
    let mut tmp_buf = [0; 64];

    loop {
        match source.read(&mut tmp_buf) {
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

    return Ok(total_size);
}
