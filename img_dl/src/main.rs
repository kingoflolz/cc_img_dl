use std::str;
use std::fs::OpenOptions;
use std::io::{BufReader, Write};
use flate2::read::GzDecoder;
#[macro_use]
extern crate lazy_static;
use std::env;

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use flate2::Compression;
use std::io::BufRead;
use anyhow::{Result, anyhow, Error};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use access_queue::AccessQueue;
use futures::future::*;

use tokio::time::timeout;
use std::time::Duration;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ImageRecord {
    url: String,
    hash: String,
}

fn get_image_records(fname: &str) -> Vec<ImageRecord> {
    let file = OpenOptions::new().read(true).open(fname).unwrap();
    let mut file = GzDecoder::new(file);
    let file = BufReader::new(file);

    let mut ret = Vec::new();

    for l in file.lines() {
        let i: ImageRecord = serde_json::from_str(&(l.unwrap())).unwrap();
        ret.push(i)
    }

    return ret;
}

async fn try_download(url: String, fname: String) -> Result<()> {
    let guard = Q.access().await;
    println!("download url: {}", url);

    let mut response = reqwest::get(&url).await?;
    println!("response");

    if response.status() != 200 {
        return Err(anyhow!("Not ok"))
    }

    let mut buffered_file = if let Some(size) = response.content_length() {
        if size >= 10_000_000 {
            return Err(anyhow!("File too large (content length)"))
        }

        Vec::with_capacity(size as usize)
    } else {
        Vec::new()
    };


    while let Some(chunk) = response.chunk().await? {
        buffered_file.extend(chunk);

        if buffered_file.len() >= 10_000_000 {
            return Err(anyhow!("File too large (response length)"))
        }
    }

    drop(guard);

    let mut file = File::create(fname).await?;
    file.write_all(&buffered_file).await?;
    file.shutdown().await?;
    Ok(())
}

async fn retry_download(url: String, fname: String) -> Result<()> {
    for i in 0..3 {
        let ret = timeout(Duration::from_secs(20), try_download(url.clone(), fname.clone())).await;

        match ret {
            Ok(Ok(_)) => {
                break;
            }
            _ => {
                continue;
            }
        }
    }

    Ok(())
}

lazy_static! {
    static ref Q: AccessQueue<()> = AccessQueue::new((), 4096);
}

#[tokio::main]
async fn main() {
    // let args: Vec<String> = env::args().collect();
    // assert_eq!(args.len(), 3, "please call img_dl <input_file> <output_dir>");

    let records = get_image_records("deduped.jsonl.gz");
    println!("records {}", records.len());

    // let futures: Vec<_> = records.iter().map(|record| {
    //     try_download(record.url.clone(), format!("dbg/{}", &record.hash)).then(|result| {
    //         if let Err(e) = &result {
    //             println!("error: {}", e)
    //         }
    //         ok::<(), Error>(())
    //     })
    // }).collect();
//
    // let all_fut = join_all(futures);
    // all_fut.await;
}
