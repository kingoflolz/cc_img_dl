use warc::WarcReader;
use std::str;
use std::fs::OpenOptions;
use std::io::{BufReader, Write};
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
#[macro_use]
extern crate lazy_static;
use std::env;

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use flate2::Compression;

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Link {
    #[serde(skip_serializing_if="Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    url: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    text: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    alt: Option<String>
}

impl Link{
    fn accept(&self) -> bool {
        let has_cc = if self.url.is_some() {
            self.url.as_ref().unwrap().find("creativecommons.org").is_some()
        } else {
            false
        };

        let has_alt_img = self.url.is_some() && self.alt.is_some() && self.path.is_some() && self.path.as_ref().unwrap().find("IMG@").is_some();
        has_cc || has_alt_img
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct HeaderMeta {
    #[serde(skip_serializing_if="Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(rename = "http-equiv")]
    http_equiv: Option<String>,
    #[serde(skip_serializing_if="Option::is_none")]
    name: Option<String>,
}

lazy_static! {
    static ref HEADER_NAME_FILTER: HashSet<Option<String>> = {
        let mut m = HashSet::new();
        m.insert(Some(String::from("description")));
        m.insert(Some(String::from("keywords")));
        m
    };

    static ref HEADER_HTTP_FILTER: HashSet<Option<String>> = {
        let mut m = HashSet::new();
        m.insert(Some(String::from("content-language")));
        m.insert(Some(String::from("content-type")));
        m
    };
}

impl HeaderMeta{
    fn accept(&self) -> bool {
        HEADER_HTTP_FILTER.contains(&self.http_equiv) || HEADER_NAME_FILTER.contains(&self.name)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Head {
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(rename = "Title")]
    title: Option<String>,
    #[serde(rename = "Metas")]
    #[serde(skip_serializing_if="Option::is_none")]
    metas: Option<Vec<HeaderMeta>>,
}

impl Head {
    fn filter(&mut self) {
        if self.metas.is_some() {
            self.metas = Some(self.metas.as_ref().unwrap().iter().filter(|x| x.accept()).cloned().collect())
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct HtmlMetadata {
    #[serde(rename = "Links")]
    links: Vec<Link>,
    #[serde(rename = "Head")]
    head: Option<Head>
}

impl HtmlMetadata {
    fn filter(&mut self) {
        if self.head.is_some() {
            self.head.as_mut().unwrap().filter()
        }
        self.links = self.links.iter().filter(|x| x.accept()).cloned().collect()
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct WarcHeaderMetadata {
    #[serde(rename = "WARC-Target-URI")]
    url: String,
    #[serde(rename = "WARC-Date")]
    data: String
}

#[derive(Serialize, Deserialize, Debug)]
struct ResponseMetadata {
    #[serde(rename = "HTML-Metadata")]
    response: HtmlMetadata
}

#[derive(Serialize, Deserialize, Debug)]
struct PayloadMetadata {
    #[serde(rename = "HTTP-Response-Metadata")]
    response: ResponseMetadata
}

#[derive(Serialize, Deserialize, Debug)]
struct Envelope {
    #[serde(rename = "Payload-Metadata")]
    payload: PayloadMetadata,
    #[serde(rename = "WARC-Header-Metadata")]
    warc: WarcHeaderMetadata,
}

#[derive(Serialize, Deserialize, Debug)]
struct Wrapper {
    #[serde(rename = "Envelope")]
    envelope: Envelope
}

fn main() -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();

    assert_eq!(args.len(), 3, "please call commoncrawl_filter <input url> <output_path>");

    // let mut file = OpenOptions::new().read(true).open("CC-MAIN-20210115134101-20210115164101-00000.warc.wat.gz")?;
    // "http://commoncrawl.s3.amazonaws.com/crawl-data/CC-MAIN-2021-04/segments/1610703495901.0/wat/CC-MAIN-20210115134101-20210115164101-00000.warc.wat.gz"
    let response = ureq::get(&args[1]).call().unwrap();
    let file = response.into_reader();

    let gz = MultiGzDecoder::new(file);
    let text = BufReader::with_capacity(1024*1024*16, gz);
    let file = WarcReader::new(text);

    // CC-MAIN-20210115134101-20210115164101-00000.warc.wat.jsonl.gz
    let outfile = OpenOptions::new().create(true).write(true).truncate(true).open(&args[2])?;
    let mut outfile_writer = GzEncoder::new(outfile, Compression::new(3));

    let mut count = 0;
    let mut has_both = 0;
    for record in file {
        count += 1;
        match record {
            Err(err) => println!("ERROR: {}\r\n", err),
            Ok(record) => {
                let has_img = twoway::find_bytes(&record.body, b"IMG@/").is_some();
                let has_alt = twoway::find_bytes(&record.body, b"\"alt\":").is_some();
                let has_cc = twoway::find_bytes(&record.body, b"creativecommons.org").is_some();

                let process_more = has_alt && has_img && has_cc;

                if process_more {
                    has_both += 1;

                    let mut w: Wrapper = serde_json::from_slice(&record.body).unwrap();
                    w.envelope.payload.response.response.filter();

                    let output = serde_json::to_string(&w).unwrap();
                    outfile_writer.write(output.as_bytes()).unwrap();
                    outfile_writer.write(b"\n").unwrap();
                }
            }
        }
    }

    outfile_writer.finish().unwrap();

    // println!("Total records: {}", count);
    // println!("Has cc img: {}", has_both);

    Ok(())
}
