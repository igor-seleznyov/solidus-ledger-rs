use std::io;
use std::sync::Arc;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use crate::ring_buffer::RingBuffer;

const SERVER: Token = Token(0);

pub struct Acceptor {
    poll: Poll,
    listener: TcpListener,
    workers: Vec<Arc<RingBuffer<TcpStream>>>,
    next_worker: usize
}

impl Acceptor {
    pub fn new(addr: &str, queues: Vec<Arc<RingBuffer<TcpStream>>>) -> io::Result<Self> {
        let poll = Poll::new()?;
        let addr = addr.parse().unwrap();
        let mut listener = TcpListener::bind(addr)?;
        poll.registry().register(&mut listener, SERVER, Interest::READABLE)?;

        Ok(
            Self {
                poll,
                listener,
                workers: queues,
                next_worker: 0,
            }
        )
    }

    pub fn run(&mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(128);

        loop {
            self.poll.poll(&mut events, None)?;

            for event in events.iter() {
                if event.token() == SERVER {
                    loop {
                        match self.listener.accept() {
                            Ok((stream, addr)) => {
                                println!("Accepted connection from {}", addr);
                                let idx = self.next_worker % self.workers.len();
                                self.next_worker += 1;
                                if self.workers[idx].push(stream).is_err() {
                                    println!("Acceptor worker {} is full!", idx);
                                }
                            }
                            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                            Err(e) => return Err(e)
                        }
                    }
                }
            }
        }
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }
}