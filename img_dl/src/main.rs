use std::str;
use std::fs::OpenOptions;
use std::io::{BufReader, Write};
use std::io::BufRead;
use std::time::Duration;
use std::env;

use futures::future::*;
use futures::{StreamExt, stream::iter};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;

use serde::{Deserialize, Serialize};

use anyhow::{Result, anyhow, Error};


#[derive(Serialize, Deserialize, Debug, Clone)]
struct ImageRecord {
    url: String,
    hash: String,
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if="Option::is_none")]
    error: Option<String>,
}

fn get_image_records(fname: &str) -> Vec<ImageRecord> {
    let file = OpenOptions::new().read(true).open(fname).unwrap();
    let file = GzDecoder::new(file);
    let file = BufReader::new(file);

    let mut ret = Vec::new();

    for l in file.lines() {
        let i: ImageRecord = serde_json::from_str(&(l.unwrap())).unwrap();
        ret.push(i)
    }

    return ret;
}

async fn try_download(url: String, fname: String) -> Result<()> {
    // println!("download url: {}", url);
    let mut response = reqwest::get(&url).await?;
    // println!("response");

    if response.status() != 200 {
        return Err(anyhow!("Return code not ok ({:?})", response.status()))
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

    let mut file = File::create(fname).await?;
    file.write_all(&buffered_file).await?;
    file.shutdown().await?;
    Ok(())
}

async fn retry_download(url: String, fname: String) -> Result<()> {
    let mut tries: usize = 0;
    loop {
        let ret = timeout(Duration::from_secs(20), try_download(url.clone(), fname.clone())).await;
        tries += 1;

        match ret {
            Ok(Ok(_)) => {
                return Ok(())
            }
            Ok(Err(e)) => {
                if tries == 3 {
                    return Err(e)
                }
            }
            Err(_) => {
                if tries == 3 {
                    return Err(anyhow!("timeout error"))
                }
            }
        };
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = env::args().collect();
    assert_eq!(args.len(), 4, "please call img_dl <input_file> <output_dir> <output_file>");

    let records = get_image_records(&args[1]);

    // records.truncate(100);
    // println!("records {}", records.len());

    let outfile = OpenOptions::new().create(true).write(true).truncate(true).open(&args[3])?;
    let mut outfile_writer = GzEncoder::new(outfile, Compression::new(3));

    let futures: Vec<_> = records.iter().map(|record| {
        retry_download(record.url.clone(), format!("{}/{}", &args[2], &record.hash)).then(move |result| {
            if let Err(e) = &result {
                let mut record = record.clone();
                record.error = Some(e.to_string());
                ok::<Option<ImageRecord>, Error>(Some(record))
            } else {
                ok::<Option<ImageRecord>, Error>(None)
            }
        })
    }).collect();

    let mut buffered = iter(futures).buffer_unordered(128);

    while let Some(i) = buffered.next().await {
        match i {
                Ok(Some(r)) => {
                    let mut jsonstr = serde_json::to_string(&r)?;
                    jsonstr.push('\n');

                    outfile_writer.write_all(jsonstr.as_bytes())?;
                }
                _ => {}
            }
    }

    Ok(())
}
