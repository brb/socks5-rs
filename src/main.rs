extern crate mio;
extern crate bytes;

use mio::*;
use mio::tcp::{TcpListener, TcpStream};
use std::collections::HashMap;
use std::io;
use std::net::Ipv4Addr;
use std::io::{Read, Write};
use bytes::{BytesMut, BufMut, ByteOrder, BigEndian};

struct Conn {
    socket: TcpStream,
    state: State,
    read_buf: BytesMut,
    read_left: isize,
    write_buf: BytesMut,
}

#[derive(PartialEq)]
enum State {
    ReadFirstRequestHdr,
    ReadSecondRequest,
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
                            State::ReadFirstRequestHdr =>
                                handle_read_first_request_hdr(&mut conn),
                            State::ReadSecondRequest =>
                                handle_read_second_request(&mut conn),
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
        state: State::ReadFirstRequestHdr,
        read_buf: BytesMut::with_capacity(64),
        read_left: 2, // SOCKS_VSN + AUTH_METHODS_COUNT
        write_buf: BytesMut::with_capacity(64),
    };
    conns.insert(token, conn);
}

// TODO trait for states

fn handle_read_first_request_hdr(conn: &mut Conn) {
    let mut buf = [0; 64];
    let socket = &mut conn.socket;
    let mut size = socket.read(&mut buf).unwrap();

    conn.read_buf.put(&buf[0 .. size]);
    conn.read_left -= size as isize;

    if conn.read_left > 0 {
        return;
    }

    // FIXME
    conn.read_left += conn.read_buf[1] as isize; // auth meth count

    if conn.read_left == 0 {

        let reply = [5, 0];
        size = socket.write(&reply).unwrap();
        if size != reply.len() {
            panic!("oops");
        }

        conn.state = State::ReadSecondRequest;
        conn.read_left = 4;

    }
}

fn handle_read_second_request(conn: &mut Conn) {
    let mut buf = [0; 64];
    let socket = &mut conn.socket;

    let mut size = socket.read(&mut buf).unwrap();
    conn.read_left -= size as isize;

    if conn.read_left > 0 {
        return;
    }

    //let cmd = buf[1];
    let atyp = buf[3];
    assert_eq!(atyp, 0x1);

    // FIXME bug!
    conn.read_left += 7;
    //if conn.read_left == 0;

    let dst_addr = Ipv4Addr::new(buf[4], buf[5], buf[6], buf[7]);
    let dst_port: u16 = BigEndian::read_u16(&[buf[8], buf[9]][..]);

    println!("second: {}:{}", dst_addr, dst_port);
}
