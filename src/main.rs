#![cfg_attr(feature="clippy", feature(plugin))]
#![cfg_attr(feature="clippy", plugin(clippy))]

use std::time::{Instant};
use std::net::TcpStream;
use std::io::{BufReader,BufWriter,BufRead,Write};

enum Request {
    Ping,
    Get {
        key: String,
    },
    Set {
        key: String,
        value: String,
    },
    Incr {
        key: String,
    },
    Lpush {
        key: String,
        value: String
    },
    Lrange {
        key: String,
        start: i64,
        end: i64
    }
}

#[derive(Debug)]
enum Response {
    SimpleString { value: String },
    Error { value: String },
    Integer { value: i64 },
    BulkString {
        length: i64,
        value: String,
    },
    Array {
        length: i64,
        value: Vec<Box<Response>>,
    },

    Unknown {
        resp_type: Option<char>,
        value: Option<String>,
    },
}

struct RedisClient {
    writer: BufWriter<TcpStream>,
    reader: BufReader<TcpStream>,
}


impl RedisClient {

    fn filter_eol(x: &char) -> bool {
        !(*x == '\r' || *x == '\n')
    }

    fn get_result(iter: std::str::Chars) -> String {
        iter
            .filter(RedisClient::filter_eol)
            .collect()
    }

    fn get_length(iter: std::str::Chars) -> i64 {
        iter
            .filter(RedisClient::filter_eol)
            .collect::<String>()
            .parse()
            .expect("Should be a number")
    }

    pub fn new(url: &str) -> Result<RedisClient, std::io::Error> {
        let stream = TcpStream::connect(url)?;
        Ok(RedisClient {
            writer: BufWriter::new(stream.try_clone()?),
            reader: BufReader::new(stream.try_clone()?),
        })
    }

    pub fn send_command(&mut self, command: Request) -> Result<Response, std::io::Error> {
        let req = RedisClient::get_command(command);
        self.writer.write_all(req.as_bytes())?;
        self.writer.write_all(b"\r\n")?;
        self.writer.flush()?;
        Ok(RedisClient::parse_response(&mut self.reader))
    }

    fn escape(input: &str) -> String {
        input.replace("\"", "\\\"")
    }

    fn get_command(command: Request) -> String {
        match command {
            Request::Ping => String::from("PING"),
            Request::Get { key } => format!(
                "GET \"{}\"",
                RedisClient::escape(&key)
            ),
            Request::Set { key, value } => format!(
                "SET \"{}\" \"{}\"",
                RedisClient::escape(&key),
                RedisClient::escape(&value),
            ),
            Request::Incr { key } => format!(
                "INCR \"{}\"",
                RedisClient::escape(&key),
            ),
            Request::Lpush { key, value } => format!(
                "LPUSH \"{}\" \"{}\"",
                RedisClient::escape(&key),
                RedisClient::escape(&value),
            ),
            Request::Lrange { key, start, end } => format!(
                "LRANGE \"{}\" \"{}\" \"{}\"",
                key, start, end,
            ),
        }
    }


    fn parse_response(reader: &mut BufReader<TcpStream>) -> Response {
        let mut line = String::new();
        reader.read_line(&mut line).expect("Can't read line from redis");

        let mut chars = line.chars();
        match chars.next() {
            Some('+') => Response::SimpleString {
                value: RedisClient::get_result(chars)
            },
            Some('-') => Response::Error {
                value: RedisClient::get_result(chars)
            },
            Some(':') => Response::Integer {
                value: RedisClient::get_length(chars)
            },
            Some('$') => {
                let length = RedisClient::get_length(chars);
                let mut buf = String::new();
                reader.read_line(&mut buf).expect("Can't read content for BulkString");
                Response::BulkString {
                    length,
                    value: RedisClient::get_result(buf.chars()),
                }
            },
            Some('*') => {
                let length = RedisClient::get_length(chars);
                
                let mut vector : Vec<Box<Response>> = Vec::with_capacity(length as usize);

                for _ in 0..length {
                    vector.push(Box::new(
                        RedisClient::parse_response(reader)
                    ));
                }

                Response::Array {
                    length,
                    value: vector
                }
            }
            Some(unknown_type) => Response::Unknown {
                resp_type: Some(unknown_type),
                value: Some(chars.collect()),
            },
            None => Response::Unknown { resp_type: None, value: None },
        }
    }
}


fn main() {
    let url = "127.0.0.1:6379";
    let mut client = RedisClient::new(url).unwrap();

    println!("simple get/set");
    println!("{:?}", client.send_command(Request::Ping).unwrap());
    println!("{:?}", client.send_command(
        Request::Set {
             key: String::from("keeey"),
             value: String::from("hellloooo world"),
        }
    ).unwrap());
    println!("{:?}", client.send_command(
        Request::Get {
            key: String::from("keeey")
        }
    ).unwrap());
    println!();

    println!("incr/get");
    println!("{:?}", client.send_command(
        Request::Incr {
             key: String::from("myincr"),
        }
    ).unwrap());
    println!("{:?}", client.send_command(
        Request::Get {
            key: String::from("myincr")
        }
    ).unwrap());
    println!();

    println!("error");
    println!("{:?}", client.send_command(
        Request::Incr {
            key: String::from("keeey")
        }
    ).unwrap());
    println!();

    println!("simple list");
    println!("{:?}", client.send_command(
        Request::Lpush {
             key: String::from("prettylist"),
             value: String::from("hellloooo world"),
        }
    ).unwrap());
    println!("{:?}", client.send_command(
        Request::Lpush {
             key: String::from("prettylist"),
             value: String::from("world hello"),
        }
    ).unwrap());
    println!("{:?}", client.send_command(
        Request::Lrange {
            key: String::from("prettylist"),
            start: 0,
            end: -1
        }
    ).unwrap());
    println!();

    println!("bench");
    let bench_key = String::from("digit");

    let now = Instant::now(); 
    for _ in 0..100_000 { 
        client.send_command(
            Request::Set {
                key: bench_key.clone(),
                value: "10".to_string()
            }
        ).unwrap();
        client.send_command(
            Request::Get {
                key: bench_key.clone()
            }
        ).unwrap();
    };
    println!("{:?}", now.elapsed());
}
