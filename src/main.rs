extern crate mio;
extern crate bytes;

use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::io::{Read, Write};
use bytes::{BytesMut, BufMut, ByteOrder, BigEndian};

struct Conn {
    socket: TcpStream,
    state: State,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

#[derive(PartialEq)]
enum State {
    ReadFirstRequest,
    WriteFirstReply,
    ReadSecondRequest,
    WriteSecondReply,
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
                token_index => {
                    if event.readiness().is_readable() {
                        let mut conn = &mut conns.get_mut(&token_index).unwrap();
                        match conn.state {
                            State::ReadFirstRequest =>
                                handle_read_first_request(&mut conn),
                            State::WriteFirstReply =>
                                panic!("NYI"),
                            State::ReadSecondRequest =>
                                handle_read_second_request(&mut conn),
                            State::WriteSecondReply =>
                                panic!("NYI"),
                        }
                    }
                }
            }
        }
    }
}

fn accept_connection(server: &TcpListener, token_index: usize, poll: &Poll, conns: &mut HashMap<Token, Conn>) {
    let socket = server.accept().unwrap().0;
    let token = Token(token_index);
    poll.register(&socket, token, Ready::readable(), PollOpt::edge()).unwrap();

    let conn = Conn{
        socket: socket,
        state: State::ReadFirstRequest,
        read_buf: BytesMut::with_capacity(64),
        write_buf: BytesMut::with_capacity(64),
    };
    conns.insert(token, conn);
}

// TODO trait for states

fn handle_read_first_request(conn: &mut Conn) {
    let mut buf = [0; 64];
    let socket = &mut conn.socket;

    let size = socket.read(&mut buf).unwrap();
    conn.read_buf.put(&buf[0 .. size]);

    let len = conn.read_buf.len();
    if len < 2 || len < conn.read_buf[1] as usize + 2 {
        return;
    }

    let reply = [5, 0];
    let size = socket.write(&reply).unwrap();
    if size != reply.len() { panic!("oops"); }

    conn.state = State::ReadSecondRequest;
    conn.read_buf.clear();
}

fn handle_read_second_request(conn: &mut Conn) {
    let mut buf = [0; 64];
    let socket = &mut conn.socket;

    let size = socket.read(&mut buf).unwrap();
    conn.read_buf.put(&buf[0 .. size]);

    if conn.read_buf.len() < 4 {
        return;
    }

    // handle hdr

    //let cmd = buf[1];
    let atyp = conn.read_buf[3];
    assert_eq!(atyp, 0x1);

    if conn.read_buf.len() < 10 {
        return;
    }

    let dst_addr = Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]);
    let dst_port: u16 = BigEndian::read_u16(&[buf[8], buf[9]][..]);

    println!("second: {}:{}", dst_addr, dst_port);

    // TODO reply
}
