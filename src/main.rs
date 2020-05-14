extern crate futures;
extern crate rusoto_core;
extern crate rusoto_sqs;
extern crate rusoto_sts;
extern crate tokio_core;
extern crate dipstick;
extern crate rand;

#[macro_use]
extern crate quicli;

use dipstick::*;
use uuid::Uuid;
use std::thread;
use std::result::Result;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::mpsc::SendError;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use rusoto_sqs::{ SendMessageBatchRequest, SendMessageBatchRequestEntry, SendMessageBatchResult, SendMessageBatchError };
use rusoto_core::credential::ProfileProvider;
use futures::future::Future;
use rusoto_core::{ Region, RusotoError};
use rusoto_sqs::{SqsClient, Sqs};
use tokio_core::reactor::Core;
use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::time::Duration;
use structopt::StructOpt;
use quicli::prelude::*;

#[derive(Debug, StructOpt)]
struct Cli {
    #[structopt(long="region", short="r")]
    region: String,
    #[structopt(long="custom_aws_endpoint_url", short="c")]
    custom_aws_endpoint_url: Option<String>,
    #[structopt(long="q_url", short="q")]
    q_url: String,
    #[structopt(long="filename", short="f")]
    filename: String,
    #[structopt(long="batch_size", short="b", default_value="10")]
    batch_size: usize,
    #[structopt(long="profile", short="p")]
    profile: String
}

metrics! { METRICS = "sqs_file_spewer" => {
        pub SQS_MESSAGES_SENT: Counter = "messages_sent";
    }
}

fn get_credentials(profile: &str) -> ProfileProvider {
    let mut credentials = ProfileProvider::new().unwrap();
    if !profile.is_empty(){
        credentials.set_profile(profile);
    }

    credentials
}

fn main() -> CliResult {
    let args = Cli::from_args();
    let reg = match args.custom_aws_endpoint_url {
        Some(ce) =>  Region::Custom {
            name: args.region,
            endpoint: ce,
        },
        None => args.region.parse().unwrap()
    };
    let client = SqsClient::new(reg);
    batch_submit(&client, args.filename, args.q_url);
    Ok(())
}

fn msg_thru_sender(msg: Vec<String>, s: Sender<Vec<String>>, counter: Counter, count: usize) -> Result<(), SendError<Vec<String>>> {
    counter.count(count);
    s.send(msg) 
}


fn batch_submit(client: &SqsClient, filename: String, q_url: String) {
    let message_cnt = Arc::new(AtomicUsize::new(0));
    let recieved_cnt = Arc::new(AtomicUsize::new(0));
    let metrics = Graphite::send_to("localhost:2003")
                .expect("Connected")
                        .metrics();
    let msg_metrics = metrics.named("sqs_file_spewer");
    let send_cnt = msg_metrics.counter("send_cnt");
    let rec_cnt = msg_metrics.counter("rec_cnt");
    let file = match File::open(&filename) {
        Err(e) => panic!("Could not open {}: {}", &filename, e),
        Ok(f) => f,
    };
    msg_metrics.flush_every(Duration::new(5, 0));
    let lines: Vec<String> = io::BufReader::new(file).lines()
        .map(|l| l.unwrap().to_string()).collect(); 
    
    let (s, r) = mpsc::channel();
    thread::spawn(move || {
        lines.as_slice().chunks(10).for_each(|l| match msg_thru_sender(l.to_owned(), s.to_owned(), send_cnt.to_owned(), message_cnt.fetch_add(1, Ordering::SeqCst)) {
            Ok(_) => (),
            Err(_) =>  println!("Receiver has stopped listening")
        })
    });

    let mut core = Core::new().unwrap();

    loop {
        match r.recv() {
            Ok(lns) => match core.run(client.send_message_batch(sqs_send_message_batch_req(lns, &q_url, rec_cnt.to_owned(), recieved_cnt.fetch_add(1, Ordering::SeqCst)))) {
                Ok(r) => println!("Success! {:?}",r),
                Err(e) => panic!("huh...{:?}", e)
            },
            Err(_) => panic!("Error occurred receiving lines")
        }
    }

   
}

fn batch_sqs_send(req: SendMessageBatchRequest, client: &SqsClient)

-> impl Future<Item = SendMessageBatchResult, Error = RusotoError<SendMessageBatchError>>
{
    client.send_message_batch(req)
}


fn message_body_to_smbre(body: String) -> SendMessageBatchRequestEntry {
    let mut se = SendMessageBatchRequestEntry::default();
    se.id = format!("{}", Uuid::new_v4());
    se.message_body = body;
    se
}

fn sqs_send_message_batch_req(msg_batch: Vec<String>, q_url: &String, cnt: Counter, msg_count: usize) -> SendMessageBatchRequest {
   cnt.count(msg_count);
   let mut req = SendMessageBatchRequest::default();
   req.queue_url = q_url.to_owned();
   let entries: Vec<SendMessageBatchRequestEntry> = msg_batch.into_iter().map(|m| message_body_to_smbre(m)).collect();
   req.entries = entries;
   req
}

#[test]
fn batch_messages_test() {
    let test_message_file_name = &"tests/resources/test_messages.txt";
    let file = match File::open(test_message_file_name) {
        Err(e) => panic!("Could not open {}: {}", test_message_file_name, e),
        Ok(f) => f,
    };
    let q_url = "http://localhost:4567/queue/da-q".to_owned();
    let mut core = Core::new().unwrap();
    let client = SqsClient::new(Region::Custom {
        name: "us-east-1".to_owned(),
        endpoint: "http://localhost:4576".to_owned(),
    });
    let metrics = Graphite::send_to("localhost:2003")
                .expect("Connected").metrics();
    let msg_metrics = metrics.named("sqs_file_spewer");
    let rec_cnt = msg_metrics.counter("a_counter");
    let lines: Vec<String> = io::BufReader::new(file).lines().map(|l| l.unwrap().to_string()).collect();
    lines.as_slice().chunks(10).for_each(|l| match core.run(batch_sqs_send(sqs_send_message_batch_req(l.to_owned(), &q_url, rec_cnt.to_owned(), 100), &client)) {
        Ok(s) => println!("Found this {:?}",s),
        Err(e) => panic!("Error completing futures: {}", e),
    });
}

