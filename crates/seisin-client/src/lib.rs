//! A minimal client that follows `Response::Redirect` automatically, so
//! callers (tests, future tooling) don't reimplement the redirect-follow
//! loop themselves.

use std::net::TcpStream;

use anyhow::Result;

use seisin_protocol::{
  decode_response, encode_request, read_frame, write_frame, Request, Response,
};

/// Follows at most this many redirects before giving up — guards against
/// a misconfigured or buggy cluster causing an infinite redirect loop.
pub const MAX_REDIRECTS: u32 = 8;

/// Sends `request` to `initial_address`, following any `Redirect`
/// responses (up to `MAX_REDIRECTS` hops) until a non-redirect response
/// is received.
pub fn call(initial_address: &str, request: Request) -> Result<Response> {
  let mut address = initial_address.to_string();
  for _ in 0..MAX_REDIRECTS {
    let mut stream = TcpStream::connect(&address)?;
    write_frame(&mut stream, &encode_request(&request))?;
    let payload = read_frame(&mut stream)?;
    match decode_response(&payload)? {
      Response::Redirect { address: next } => address = next,
      other => return Ok(other),
    }
  }
  anyhow::bail!("gave up after {MAX_REDIRECTS} redirects, still pointed at {address}");
}

#[cfg(test)]
mod tests {
  use super::*;
  use std::net::TcpListener;
  use std::thread;

  use seisin_core::datum::DatumId;
  use seisin_protocol::encode_response;

  /// A tiny fake server: replies with a `Redirect` to whatever's in
  /// `redirect_targets`, in order, then replies with `final_response`.
  fn start_fake_server(redirect_targets: Vec<String>, final_response: Response) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    thread::spawn(move || {
      let (mut stream, _) = listener.accept().unwrap();
      let _payload = read_frame(&mut stream).unwrap();
      let response = match redirect_targets.into_iter().next() {
        Some(address) => Response::Redirect { address },
        None => final_response,
      };
      write_frame(&mut stream, &encode_response(&response)).unwrap();
    });
    addr
  }

  #[test]
  fn returns_the_response_directly_when_there_is_no_redirect() {
    let addr = start_fake_server(vec![], Response::Ok);
    let response = call(&addr, Request::Get { id: DatumId::new() }).unwrap();
    assert_eq!(response, Response::Ok);
  }

  #[test]
  fn follows_a_single_redirect() {
    let final_addr = start_fake_server(vec![], Response::Ok);
    let first_addr = start_fake_server(vec![final_addr], Response::Ok);
    let response = call(&first_addr, Request::Get { id: DatumId::new() }).unwrap();
    assert_eq!(response, Response::Ok);
  }

  #[test]
  fn gives_up_after_max_redirects() {
    // A server that always redirects to itself never resolves.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let addr_for_thread = addr.clone();
    thread::spawn(move || {
      for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        let _ = read_frame(&mut stream);
        let response = Response::Redirect {
          address: addr_for_thread.clone(),
        };
        let _ = write_frame(&mut stream, &encode_response(&response));
      }
    });
    let result = call(&addr, Request::Get { id: DatumId::new() });
    assert!(result.is_err());
  }
}
