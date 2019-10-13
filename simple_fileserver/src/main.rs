use futures;
use futures::prelude::*;
use hyper;
use hyper::service::service_fn;
use hyper::{Body, Request, Response, Server, StatusCode};
use std::path::PathBuf;
use tokio;
use tokio::codec::{BytesCodec, FramedWrite};
use tokio::prelude::*;

fn main() {
    let ip = [0, 0, 0, 0];
    let port = 80;
    let addr = (ip, port).into();
    let server = Server::bind(&addr)
        .serve(|| service_fn(store_file))
        .map_err(|e| eprintln!("ERROR: {:?}", e));
    hyper::rt::run(server);
}

fn store_file(req: Request<Body>) -> impl Future<Item = Response<Body>, Error = hyper::Error> {
    let path_suffix = req.uri().path().to_owned();
    let parsed = PathBuf::from(path_suffix);
    let bad_url_res = if parsed.starts_with("/data1/ilan_glidetester_runs")
        || parsed.starts_with("/data2/ilan_glidetester_runs")
    {
        Ok(())
    } else {
        Err(FileUploadError::BadUrl(format!("{}", parsed.display())))
    };
    let bd = req.into_body();
    let url_parse_future = future::result(bad_url_res);

    let file_name = parsed.clone();
    let parent_name = file_name.parent().unwrap().to_owned();
    let create_dir_fute = url_parse_future.and_then(move |_| {
        tokio::fs::create_dir_all(parent_name).map_err(FileUploadError::FileOpenError)
    });
    let file_future = create_dir_fute.and_then(|_| {
        tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(file_name)
            .map_err(FileUploadError::FileOpenError)
    });
    let len_future = file_future
        .map(|f| FramedWrite::new(f, BytesCodec::new()))
        .and_then(|f| {
            bd.map(|chunk| chunk.into())
                .map_err(FileUploadError::HyperError)
                .forward(f)
        })
        .and_then(|_| {
            tokio::fs::OpenOptions::new()
                .read(true)
                .open(parsed)
                .map_err(FileUploadError::FileOpenError)
                .and_then(|f| f.metadata().map_err(FileUploadError::FileReadError))
        })
        .map(|(_f, m)| m.len());
    len_future
        .map(|len| {
            let msg = format!("Wrote {} bytes.", len);
            let mut resp = Response::new(Body::from(msg));
            *resp.status_mut() = StatusCode::from_u16(200).unwrap();
            resp
        })
        .or_else(|err| match err {
            FileUploadError::HyperError(e) => future::err(e),
            other => {
                let msg = format!("Got error: {:?}", other);
                let mut resp = Response::new(Body::from(msg));
                *resp.status_mut() = StatusCode::from_u16(500).unwrap();
                future::ok(resp)
            }
        })
}

#[derive(Debug)]
pub enum FileUploadError {
    BadUrl(String),
    FileOpenError(std::io::Error),
    FileReadError(std::io::Error),
    FileWriteError(std::io::Error),
    HyperError(hyper::Error),
}

impl From<std::io::Error> for FileUploadError {
    fn from(write_err: std::io::Error) -> FileUploadError {
        FileUploadError::FileWriteError(write_err)
    }
}
